use crate::db::DbRow;

use super::JsonKeyValuePosition;

pub fn compile_data_with_new_expires(
    db_row: &DbRow,
    value: &str,
) -> (Vec<u8>, JsonKeyValuePosition) {
    if let Some(expires_position) = &db_row.expires_json_position {
        replace_expires(db_row, expires_position, value)
    } else {
        inject_expires_at_the_end_of_content(db_row, value)
    }
}

pub fn remove_expiration_time(db_row: &DbRow) -> Vec<u8> {
    if let Some(expires_position) = &db_row.expires_json_position {
        let mut result = Vec::new();

        result.extend_from_slice(&db_row.data[..expires_position.key_start - 1]);

        let mut comma_pos = None;
        for i in expires_position.value_end..db_row.data.len() {
            if db_row.data[i] == b',' {
                comma_pos = Some(i);
                break;
            }
        }

        if let Some(comma_pos) = comma_pos {
            result.extend_from_slice(&db_row.data[comma_pos + 1..]);
        } else {
            result.extend_from_slice(&db_row.data[expires_position.value_end + 1..]);
        }

        result
    } else {
        db_row.data.clone()
    }
}

fn replace_expires(
    db_row: &DbRow,
    expires_position: &JsonKeyValuePosition,
    value: &str,
) -> (Vec<u8>, JsonKeyValuePosition) {
    let mut result = Vec::new();

    let mut json_key_value_position = expires_position.clone();

    result.extend_from_slice(&db_row.data[..expires_position.key_start + 1]);
    result.extend_from_slice("Expires\":\"".as_bytes());

    json_key_value_position.value_start = result.len() - 1;
    result.extend_from_slice(value.as_bytes());
    json_key_value_position.value_end = result.len() + 1;
    result.extend_from_slice(&db_row.data[expires_position.value_end - 1..]);
    (result, json_key_value_position)
}

fn inject_expires_at_the_end_of_content(
    db_row: &DbRow,
    value: &str,
) -> (Vec<u8>, JsonKeyValuePosition) {
    let mut json_key_value_position = JsonKeyValuePosition {
        key_start: 0,
        key_end: 0,
        value_start: 0,
        value_end: 0,
    };

    let mut i = 0;
    for b in db_row.data.as_slice() {
        if *b == b'{' {
            break;
        }

        i += 1;
    }

    i += 1;

    let mut result = Vec::new();

    result.extend_from_slice(&db_row.data[..i]);
    json_key_value_position.key_start = result.len();
    result.extend_from_slice("\"Expires\"".as_bytes());
    json_key_value_position.key_end = result.len();
    result.extend_from_slice(":\"".as_bytes());

    json_key_value_position.value_start = result.len() - 1;

    result.extend_from_slice(value.as_bytes());

    json_key_value_position.value_end = result.len() + 1;
    result.extend_from_slice("\",".as_bytes());

    result.extend_from_slice(&db_row.data[i..]);
    (result, json_key_value_position)
}

#[cfg(test)]
mod test {

    use crate::db_json_entity::{DbJsonEntity, JsonTimeStamp};

    #[test]
    fn test_replace_existing_expires() {
        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "Expires": "2019-01-01T00:00:00",
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let inject_time_stamp = JsonTimeStamp::now();
        let db_row = db_json_entity.new_db_row(&inject_time_stamp);

        let (new_value, new_json_value) =
            super::compile_data_with_new_expires(&db_row, "2020-01-01T00:00:00");

        let db_json_entity = DbJsonEntity::parse(new_value.as_slice()).unwrap();

        let expires_position = db_json_entity.expires_value_position.as_ref().unwrap();

        let value = &new_value[expires_position.value_start..expires_position.value_end];

        assert_eq!(
            "\"2020-01-01T00:00:00\"",
            std::str::from_utf8(value).unwrap()
        );

        assert_eq!(new_json_value.key_start, expires_position.key_start);
        assert_eq!(new_json_value.key_end, expires_position.key_end);

        assert_eq!(new_json_value.value_start, expires_position.value_start);
        assert_eq!(new_json_value.value_end, expires_position.value_end);
    }

    #[test]
    fn test_injecting_expires() {
        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test"
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let inject_time_stamp = JsonTimeStamp::now();
        let db_row = db_json_entity.new_db_row(&inject_time_stamp);

        let (new_value, new_json_value) =
            super::compile_data_with_new_expires(&db_row, "2020-01-01T00:00:00");

        let db_json_entity = DbJsonEntity::parse(new_value.as_slice()).unwrap();

        let expires_position = db_json_entity.expires_value_position.as_ref().unwrap();

        let value = &new_value[expires_position.value_start..expires_position.value_end];

        assert_eq!(
            "\"2020-01-01T00:00:00\"",
            std::str::from_utf8(value).unwrap()
        );

        assert_eq!(new_json_value.key_start, expires_position.key_start);
        assert_eq!(new_json_value.key_end, expires_position.key_end);

        assert_eq!(new_json_value.value_start, expires_position.value_start);
        assert_eq!(new_json_value.value_end, expires_position.value_end);
    }

    #[test]
    fn test_remove_expiration_time() {
        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "Expires": "2019-01-01T00:00:00"
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let inject_time_stamp = JsonTimeStamp::now();
        let db_row = db_json_entity.new_db_row(&inject_time_stamp);

        let new_value = super::remove_expiration_time(&db_row);

        println!("{:#}", std::str::from_utf8(&new_value).unwrap());

        let db_json_entity = DbJsonEntity::parse(new_value.as_slice()).unwrap();

        assert!(db_json_entity.expires_value_position.is_none());
    }

    #[test]
    fn test_remove_expiration_time_at_begin() {
        let test_json = r#"{
            "Expires": "2019-01-01T00:00:00"   ,
            "PartitionKey": "test",
            "RowKey": "test",
  
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let inject_time_stamp = JsonTimeStamp::now();
        let db_row = db_json_entity.new_db_row(&inject_time_stamp);

        let new_value = super::remove_expiration_time(&db_row);

        println!("{:#}", std::str::from_utf8(&new_value).unwrap());

        let db_json_entity = DbJsonEntity::parse(new_value.as_slice()).unwrap();

        assert!(db_json_entity.expires_value_position.is_none());
    }

    #[test]
    fn test_remove_expiration_time_at_the_middle() {
        let test_json = r#"{
            "PartitionKey": "test",
            "Expires": "2019-01-01T00:00:00",
            "RowKey": "test",
  
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let inject_time_stamp = JsonTimeStamp::now();
        let db_row = db_json_entity.new_db_row(&inject_time_stamp);

        let new_value = super::remove_expiration_time(&db_row);

        println!("{:#}", std::str::from_utf8(&new_value).unwrap());

        let db_json_entity = DbJsonEntity::parse(new_value.as_slice()).unwrap();

        assert!(db_json_entity.expires_value_position.is_none());
    }
}
