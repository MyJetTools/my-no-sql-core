use std::{
    collections::{btree_map::Values, BTreeMap},
    sync::Arc,
};

#[cfg(feature = "master_node")]
use rust_extensions::date_time::DateTimeAsMicroseconds;
use rust_extensions::lazy::LazyVec;
#[cfg(feature = "master_node")]
use std::collections::HashMap;

use crate::db::DbRow;

pub struct DbRowsContainer {
    data: BTreeMap<String, Arc<DbRow>>,

    #[cfg(feature = "master_node")]
    rows_with_expiration_index: BTreeMap<i64, HashMap<String, Arc<DbRow>>>,
}

impl DbRowsContainer {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            #[cfg(feature = "master_node")]
            rows_with_expiration_index: BTreeMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[cfg(feature = "master_node")]
    pub fn rows_with_expiration_index_len(&self) -> usize {
        self.rows_with_expiration_index.len()
    }

    #[cfg(feature = "master_node")]
    pub fn get_rows_to_expire(&self, now: DateTimeAsMicroseconds) -> Option<Vec<String>> {
        let mut keys = LazyVec::new();
        for expiration_time in self.rows_with_expiration_index.keys() {
            if *expiration_time > now.unix_microseconds {
                break;
            }

            keys.add(*expiration_time);
        }

        let keys = keys.get_result()?;

        let mut result = Vec::new();
        for key in &keys {
            if let Some(removed) = self.rows_with_expiration_index.get(key) {
                for (_, db_row) in removed {
                    result.push(db_row.row_key.to_string());
                }
            }
        }

        Some(result)
    }

    #[cfg(feature = "master_node")]
    fn insert_expiration_index(&mut self, db_row: &Arc<DbRow>) {
        if db_row.expires.is_none() {
            return;
        }

        let expires = db_row.expires.unwrap();

        if !self
            .rows_with_expiration_index
            .contains_key(&expires.unix_microseconds)
        {
            self.rows_with_expiration_index
                .insert(expires.unix_microseconds, HashMap::new());
        }

        if let Some(index_data) = self
            .rows_with_expiration_index
            .get_mut(&expires.unix_microseconds)
        {
            index_data.insert(db_row.row_key.to_string(), db_row.clone());
        }
    }

    #[cfg(feature = "master_node")]
    fn remove_expiration_index(&mut self, db_row: &Arc<DbRow>) {
        if db_row.expires.is_none() {
            return;
        }

        let expires = db_row.expires.unwrap();

        let remove_root_index = if let Some(index_data) = self
            .rows_with_expiration_index
            .get_mut(&expires.unix_microseconds)
        {
            index_data.remove(&db_row.row_key);
            index_data.len() == 0
        } else {
            false
        };

        if remove_root_index {
            self.rows_with_expiration_index
                .remove(&expires.unix_microseconds);
        }
    }

    #[cfg(feature = "master_node")]
    fn insert_indices(&mut self, db_row: &Arc<DbRow>) {
        self.insert_expiration_index(&db_row);
    }

    #[cfg(feature = "master_node")]
    fn remove_indices(&mut self, db_row: &Arc<DbRow>) {
        self.remove_expiration_index(&db_row);
    }

    pub fn insert(&mut self, db_row: Arc<DbRow>) -> Option<Arc<DbRow>> {
        #[cfg(feature = "master_node")]
        self.insert_indices(&db_row);

        let result = self.data.insert(db_row.row_key.to_string(), db_row);

        #[cfg(feature = "master_node")]
        if let Some(removed_db_row) = &result {
            self.remove_indices(&removed_db_row);
        }

        result
    }

    pub fn remove(&mut self, row_key: &str) -> Option<Arc<DbRow>> {
        let result = self.data.remove(row_key);

        #[cfg(feature = "master_node")]
        if let Some(removed_db_row) = &result {
            self.remove_indices(&removed_db_row);
        }

        result
    }

    pub fn get(&self, row_key: &str) -> Option<&Arc<DbRow>> {
        self.data.get(row_key)
    }

    pub fn has_db_row(&self, row_key: &str) -> bool {
        return self.data.contains_key(row_key);
    }

    pub fn get_all<'s>(&'s self) -> Values<'s, String, Arc<DbRow>> {
        self.data.values()
    }

    pub fn get_highest_row_and_below(
        &self,
        row_key: &String,
        limit: Option<usize>,
    ) -> Option<Vec<&Arc<DbRow>>> {
        let mut result = LazyVec::new();

        for (db_row_key, db_row) in self.data.range(..row_key.to_string()) {
            if db_row_key <= row_key {
                result.add(db_row);

                if let Some(limit) = limit {
                    if result.len() >= limit {
                        break;
                    }
                }
            }
        }

        result.get_result()
    }

    #[cfg(feature = "master_node")]
    pub fn update_expiration_time(
        &mut self,
        row_key: &str,
        expiration_time: Option<DateTimeAsMicroseconds>,
    ) -> Option<Arc<DbRow>> {
        if let Some(db_row) = self.get(row_key) {
            if db_row.expires.is_none() && expiration_time.is_none() {
                return None;
            }

            if let Some(db_row_expires) = db_row.expires {
                if let Some(new_expires) = expiration_time {
                    if db_row_expires.unix_microseconds == new_expires.unix_microseconds {
                        return None;
                    }
                }
            }
        }

        let removed_db_row = self.data.remove(row_key)?;
        self.remove_expiration_index(&removed_db_row);

        let new_db_row = removed_db_row.create_with_new_expiration_time(expiration_time);

        let new_db_row = Arc::new(new_db_row);

        self.insert_expiration_index(&new_db_row);

        self.data.insert(row_key.to_string(), new_db_row);

        Some(removed_db_row)
    }
}

#[cfg(feature = "master_node")]
#[cfg(test)]
mod tests {

    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use crate::db_json_entity::{DbJsonEntity, JsonTimeStamp};

    use super::*;

    #[test]
    fn test_that_index_appears() {
        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "Expires": "2019-01-01T00:00:00",
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let mut db_rows = DbRowsContainer::new();
        let time_stamp = JsonTimeStamp::now();
        db_rows.insert(Arc::new(db_json_entity.new_db_row(&time_stamp)));

        assert_eq!(1, db_rows.rows_with_expiration_index.len())
    }

    #[test]
    fn test_that_index_does_not_appear_since_we_do_not_have_expiration() {
        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let mut db_rows = DbRowsContainer::new();
        let time_stamp = JsonTimeStamp::now();
        db_rows.insert(Arc::new(db_json_entity.new_db_row(&time_stamp)));

        assert_eq!(0, db_rows.rows_with_expiration_index.len())
    }

    #[test]
    fn test_that_index_dissapears() {
        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "Expires": "2019-01-01T00:00:00"
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let mut db_rows = DbRowsContainer::new();
        let time_stamp = JsonTimeStamp::now();
        db_rows.insert(Arc::new(db_json_entity.new_db_row(&time_stamp)));

        db_rows.remove("test");

        assert_eq!(0, db_rows.rows_with_expiration_index.len())
    }

    #[test]
    fn test_update_expiration_time_from_no_to() {
        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test"
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let mut db_rows = DbRowsContainer::new();
        let time_stamp = JsonTimeStamp::now();
        db_rows.insert(Arc::new(db_json_entity.new_db_row(&time_stamp)));

        assert_eq!(0, db_rows.rows_with_expiration_index.len());

        let new_expiration_time = DateTimeAsMicroseconds::new(2);

        db_rows.update_expiration_time("test", Some(new_expiration_time));

        assert_eq!(true, db_rows.rows_with_expiration_index.contains_key(&2));
        assert_eq!(1, db_rows.rows_with_expiration_index.len());
    }

    #[test]
    fn test_update_expiration_time_to_new_expiration_time() {
        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "Expires": "2019-01-01T00:00:00",
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let mut db_rows = DbRowsContainer::new();

        let time_stamp = JsonTimeStamp::now();

        let db_row = Arc::new(db_json_entity.new_db_row(&time_stamp));
        db_rows.insert(db_row.clone());

        let current_expiration = DateTimeAsMicroseconds::from_str("2019-01-01T00:00:00").unwrap();

        assert_eq!(
            true,
            db_rows
                .rows_with_expiration_index
                .contains_key(&current_expiration.unix_microseconds)
        );
        assert_eq!(1, db_rows.rows_with_expiration_index.len());

        db_rows.update_expiration_time("test", Some(DateTimeAsMicroseconds::new(2)));

        assert_eq!(true, db_rows.rows_with_expiration_index.contains_key(&2));
        assert_eq!(1, db_rows.rows_with_expiration_index.len());
    }

    #[test]
    fn test_update_expiration_time_from_some_to_no() {
        let mut db_rows = DbRowsContainer::new();

        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "Expires": "2019-01-01T00:00:00",
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let now = JsonTimeStamp::now();
        let db_row = Arc::new(db_json_entity.new_db_row(&now));
        db_rows.insert(db_row.clone());

        let current_expiration = DateTimeAsMicroseconds::from_str("2019-01-01T00:00:00").unwrap();

        assert_eq!(
            true,
            db_rows
                .rows_with_expiration_index
                .contains_key(&current_expiration.unix_microseconds)
        );
        assert_eq!(1, db_rows.rows_with_expiration_index.len());

        db_rows.update_expiration_time("test", None);
        assert_eq!(0, db_rows.rows_with_expiration_index.len());
    }

    #[test]
    fn test_we_do_not_have_db_rows_to_expire() {
        let mut db_rows = DbRowsContainer::new();

        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "Expires": "2019-01-01T00:00:00",
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let now = JsonTimeStamp::now();

        let db_row = Arc::new(db_json_entity.new_db_row(&now));
        db_rows.insert(db_row.clone());

        let mut now = DateTimeAsMicroseconds::from_str("2019-01-01T00:00:00").unwrap();
        now.unix_microseconds -= 1;

        let rows_to_expire = db_rows.get_rows_to_expire(now);

        assert_eq!(true, rows_to_expire.is_none());
    }

    #[test]
    fn test_we_do_have_db_rows_to_expire() {
        let mut db_rows = DbRowsContainer::new();

        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "Expires": "2019-01-01T00:00:00",
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let now = JsonTimeStamp::now();

        let db_row = Arc::new(db_json_entity.new_db_row(&now));
        db_rows.insert(db_row.clone());

        let now = DateTimeAsMicroseconds::from_str("2019-01-01T00:00:00").unwrap();

        let rows_to_expire = db_rows.get_rows_to_expire(now);

        assert_eq!(true, rows_to_expire.is_some());
    }
}
