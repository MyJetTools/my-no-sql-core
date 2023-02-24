use crate::db::DbRow;

use my_json::json_reader::array_parser::ArrayToJsonObjectsSplitter;

use std::{collections::BTreeMap, sync::Arc};

use super::DbEntityParseFail;
use super::JsonKeyValuePosition;
use super::JsonTimeStamp;
use my_json::json_reader::JsonFirstLineReader;
use rust_extensions::date_time::DateTimeAsMicroseconds;

pub struct DbJsonEntity<'s> {
    pub partition_key: &'s str,
    pub row_key: &'s str,
    pub expires: Option<DateTimeAsMicroseconds>,
    pub time_stamp: Option<&'s str>,
    timestamp_value_position: Option<JsonKeyValuePosition>,
    pub expires_value_position: Option<JsonKeyValuePosition>,
    pub raw: &'s [u8],
}

impl<'s> DbJsonEntity<'s> {
    pub fn parse(raw: &'s [u8]) -> Result<Self, DbEntityParseFail> {
        let mut partition_key = None;
        let mut row_key = None;
        let mut expires = None;
        let mut time_stamp = None;
        let mut timestamp_value_position = None;
        let mut expires_value_position = None;

        for line in JsonFirstLineReader::new(raw) {
            let line = line?;

            let name = line.get_name()?;

            if name == super::consts::PARTITION_KEY {
                partition_key = Some(line.get_value()?);
            }

            if name == super::consts::ROW_KEY {
                row_key = Some(line.get_value()?);
            }

            if name == super::consts::EXPIRES {
                expires = line.get_value()?.as_date_time();
                expires_value_position = Some(JsonKeyValuePosition {
                    key_start: line.name_start,
                    key_end: line.name_end,
                    value_start: line.value_start,
                    value_end: line.value_end,
                });
            }

            if name == super::consts::TIME_STAMP
                || name.to_lowercase() == super::consts::TIME_STAMP_LOWER_CASE
            {
                timestamp_value_position = Some(JsonKeyValuePosition {
                    key_start: line.name_start,
                    key_end: line.name_end,
                    value_start: line.value_start,
                    value_end: line.value_end,
                });

                time_stamp = line.get_value()?.as_str();
            }
        }

        if partition_key.is_none() {
            return Err(DbEntityParseFail::FieldPartitionKeyIsRequired);
        }

        let partition_key = partition_key.unwrap().as_str();

        if partition_key.is_none() {
            return Err(DbEntityParseFail::FieldPartitionKeyCanNotBeNull);
        }

        let partition_key = partition_key.unwrap();

        if partition_key.len() > 255 {
            return Err(DbEntityParseFail::PartitionKeyIsTooLong);
        }

        if row_key.is_none() {
            return Err(DbEntityParseFail::FieldRowKeyIsRequired);
        }

        let row_key = row_key.unwrap().as_str();

        if row_key.is_none() {
            return Err(DbEntityParseFail::FieldRowKeyCanNotBeNull);
        }

        let result = Self {
            raw,
            partition_key,
            row_key: row_key.unwrap(),
            expires,
            time_stamp,
            timestamp_value_position,
            expires_value_position,
        };

        return Ok(result);
    }

    pub fn new_db_row(&self, inject_time_stamp: &JsonTimeStamp) -> DbRow {
        let data =
            compile_row_content(self.raw, &self.timestamp_value_position, &inject_time_stamp);

        return DbRow::new(
            self,
            data,
            #[cfg(feature = "master-node")]
            inject_time_stamp,
        );
    }

    pub fn restore_db_row(&self) -> DbRow {
        #[cfg(feature = "master-node")]
        let time_stamp = if let Some(time_stamp) = self.time_stamp {
            JsonTimeStamp::parse_or_now(time_stamp)
        } else {
            JsonTimeStamp::now()
        };

        let data = self.raw.to_vec();

        return DbRow::new(
            self,
            data,
            #[cfg(feature = "master-node")]
            &time_stamp,
        );
    }

    pub fn parse_as_vec(
        src: &'s [u8],
        inject_time_stamp: &JsonTimeStamp,
    ) -> Result<Vec<Arc<DbRow>>, DbEntityParseFail> {
        let mut result = Vec::new();

        for json in src.split_array_json_to_objects() {
            let db_entity = DbJsonEntity::parse(json?)?;
            let db_row = db_entity.new_db_row(inject_time_stamp);

            result.push(Arc::new(db_row));
        }
        return Ok(result);
    }

    pub fn restore_as_vec(src: &'s [u8]) -> Result<Vec<Arc<DbRow>>, DbEntityParseFail> {
        let mut result = Vec::new();

        for json in src.split_array_json_to_objects() {
            let db_entity = DbJsonEntity::parse(json?)?;
            let db_row = db_entity.restore_db_row();

            result.push(Arc::new(db_row));
        }
        return Ok(result);
    }

    pub fn parse_as_btreemap(
        src: &'s [u8],
        inject_time_stamp: &JsonTimeStamp,
    ) -> Result<BTreeMap<String, Vec<Arc<DbRow>>>, DbEntityParseFail> {
        let mut result = BTreeMap::new();

        for json in src.split_array_json_to_objects() {
            let db_entity = DbJsonEntity::parse(json?)?;
            let db_row = db_entity.new_db_row(inject_time_stamp);

            if !result.contains_key(db_entity.partition_key) {
                result.insert(db_entity.partition_key.to_string(), Vec::new());
            }

            result
                .get_mut(db_entity.partition_key)
                .unwrap()
                .push(Arc::new(db_row));
        }

        return Ok(result);
    }

    pub fn restore_as_btreemap(
        src: &'s [u8],
    ) -> Result<BTreeMap<String, Vec<Arc<DbRow>>>, DbEntityParseFail> {
        let mut result = BTreeMap::new();

        for json in src.split_array_json_to_objects() {
            let db_entity = DbJsonEntity::parse(json?)?;
            let db_row = db_entity.restore_db_row();

            if !result.contains_key(db_entity.partition_key) {
                result.insert(db_entity.partition_key.to_string(), Vec::new());
            }

            result
                .get_mut(db_entity.partition_key)
                .unwrap()
                .push(Arc::new(db_row));
        }

        return Ok(result);
    }
}

fn compile_row_content(
    raw: &[u8],
    time_stamp_value_position: &Option<JsonKeyValuePosition>,
    time_stamp: &JsonTimeStamp,
) -> Vec<u8> {
    if let Some(time_stamp_value_position) = time_stamp_value_position {
        return super::date_time_injector::replace_timestamp_value(
            raw,
            time_stamp_value_position,
            time_stamp,
        );
    } else {
        return super::date_time_injector::inject(raw, time_stamp);
    }
}

#[cfg(test)]
mod tests {

    use crate::db_json_entity::{DbEntityParseFail, JsonTimeStamp};

    use super::DbJsonEntity;

    #[test]
    pub fn parse_expires_with_z() {
        let src_json = r#"{"TwoFaMethods": {},
            "PartitionKey": "ff95cdae9f7e4f1a847f6b83ad68b495",
            "RowKey": "6c09c7f0e44d4ef79cfdd4252ebd54ab",
            "TimeStamp": "2022-03-17T09:28:27.5923",
            "Expires": "2022-03-17T13:28:29.6537478Z"
          }"#;

        let entity = DbJsonEntity::parse(src_json.as_bytes()).unwrap();

        let expires = entity.expires.as_ref().unwrap();

        assert_eq!("2022-03-17T13:28:29.653747", &expires.to_rfc3339()[..26]);

        let expires_value_position = entity.expires_value_position.unwrap();

        let expires_key =
            &src_json.as_bytes()[expires_value_position.key_start..expires_value_position.key_end];

        assert_eq!("\"Expires\"", std::str::from_utf8(expires_key).unwrap());

        let expires_value = &src_json.as_bytes()
            [expires_value_position.value_start..expires_value_position.value_end];

        assert_eq!(
            "\"2022-03-17T13:28:29.6537478Z\"",
            std::str::from_utf8(expires_value).unwrap()
        );
    }

    #[test]
    pub fn parse_with_partition_key_is_null() {
        let src_json = r#"{"TwoFaMethods": {},
            "PartitionKey": null,
            "RowKey": "test",
            "TimeStamp": "2022-03-17T09:28:27.5923",
            "Expires": "2022-03-17T13:28:29.6537478Z"
          }"#;

        let result = DbJsonEntity::parse(src_json.as_bytes());

        if let Err(DbEntityParseFail::FieldPartitionKeyCanNotBeNull) = result {
        } else {
            panic!("Should not be here")
        }
    }
    #[test]
    pub fn parse_some_case_from_real_life() {
        let src_json = r#"{"value":{"is_enabled":true,"fee_percent":5.0,"min_balance_usd":100.0,"fee_period_days":30,"inactivity_period_days":90},"PartitionKey":"*","RowKey":"*"}"#;

        let result = DbJsonEntity::parse(src_json.as_bytes()).unwrap();

        let time_stamp = JsonTimeStamp::now();
        let db_row = result.new_db_row(&time_stamp);

        println!("{:?}", std::str::from_utf8(db_row.data.as_slice()).unwrap());
    }
}
