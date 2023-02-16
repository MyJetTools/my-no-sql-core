use std::{
    collections::{btree_map::Values, BTreeMap},
    sync::Arc,
};

#[cfg(feature = "master_node")]
use crate::db::update_expiration_time_model::UpdateExpirationDateTime;
#[cfg(feature = "master_node")]
use crate::db::UpdateExpirationTimeModel;
#[cfg(feature = "master_node")]
use rust_extensions::date_time::DateTimeAsMicroseconds;
#[cfg(feature = "master_node")]
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
        let expires = db_row.get_expires();
        if expires.is_none() {
            return;
        }

        let expires = expires.unwrap();

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
        let expires = db_row.get_expires();
        if expires.is_none() {
            return;
        }

        let expires = expires.unwrap();

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

    #[cfg(feature = "master_node")]
    pub fn get_and_update_expiration_time(
        &mut self,
        row_key: &str,
        update_expiration_time: &UpdateExpirationTimeModel,
    ) -> Option<Arc<DbRow>> {
        let result = self.data.get(row_key)?.clone();

        if let UpdateExpirationDateTime::Yes(expiration_time) =
            update_expiration_time.update_db_rows_expiration_time
        {
            self.update_expiration_time(&result, expiration_time);
        }

        Some(result)
    }

    pub fn has_db_row(&self, row_key: &str) -> bool {
        return self.data.contains_key(row_key);
    }

    pub fn get_all<'s>(&'s self) -> Values<'s, String, Arc<DbRow>> {
        self.data.values()
    }

    #[cfg(feature = "master_node")]
    pub fn get_all_and_update_expiration_time<'s>(
        &'s mut self,
        update_expiration_time: &UpdateExpirationTimeModel,
    ) -> Vec<Arc<DbRow>> {
        let mut result = Vec::new();
        for db_row in self.data.values() {
            result.push(db_row.clone());
        }

        if let UpdateExpirationDateTime::Yes(expiration_time) =
            update_expiration_time.update_db_rows_expiration_time
        {
            for item in &result {
                self.update_expiration_time(item, expiration_time);
            }
        }

        result
    }
    #[cfg(feature = "master_node")]
    pub fn get_highest_row_and_below(
        &self,
        row_key: &String,
        limit: Option<usize>,
    ) -> Vec<&Arc<DbRow>> {
        let mut result = Vec::new();
        for (db_row_key, db_row) in self.data.range(..row_key.to_string()) {
            if db_row_key <= row_key {
                result.insert(0, db_row);

                if let Some(limit) = limit {
                    if result.len() >= limit {
                        break;
                    }
                }
            }
        }

        result
    }

    #[cfg(feature = "master_node")]
    pub fn get_highest_row_and_below_and_update_expiration_time(
        &mut self,
        row_key: &String,
        limit: Option<usize>,
        update_expiration: &UpdateExpirationTimeModel,
    ) -> Vec<Arc<DbRow>> {
        let mut result = Vec::new();
        for (db_row_key, db_row) in self.data.range(..row_key.to_string()) {
            if db_row_key <= row_key {
                let db_row = db_row.clone();

                result.insert(0, db_row);

                if let Some(limit) = limit {
                    if result.len() >= limit {
                        break;
                    }
                }
            }
        }

        #[cfg(feature = "master_node")]
        if let UpdateExpirationDateTime::Yes(expiration_time) =
            update_expiration.update_db_rows_expiration_time
        {
            for db_row in &result {
                self.update_expiration_time(&db_row, expiration_time);
            }
        }

        result
    }

    #[cfg(feature = "master_node")]
    fn update_expiration_time(
        &mut self,
        db_row: &Arc<DbRow>,
        expiration_time: Option<DateTimeAsMicroseconds>,
    ) {
        self.remove_expiration_index(db_row);

        db_row.update_expires(expiration_time);

        if expiration_time.is_some() {
            self.insert_expiration_index(db_row);
        }
    }
}

#[cfg(feature = "master_node")]
#[cfg(test)]
mod tests {

    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use crate::db_json_entity::JsonTimeStamp;

    use super::*;

    #[test]
    fn test_that_index_appears() {
        let mut db_rows = DbRowsContainer::new();

        let db_row = DbRow::new(
            "testPartitionKey".to_string(),
            "testRowKey".to_string(),
            vec![],
            Some(DateTimeAsMicroseconds::new(1)),
            &JsonTimeStamp::now(),
        );

        db_rows.insert(Arc::new(db_row));

        assert_eq!(1, db_rows.rows_with_expiration_index.len())
    }

    #[test]
    fn test_that_index_does_not_appear_since_we_do_not_have_expiration() {
        let mut db_rows = DbRowsContainer::new();

        let db_row = DbRow::new(
            "testPartitionKey".to_string(),
            "testRowKey".to_string(),
            vec![],
            None,
            &JsonTimeStamp::now(),
        );

        db_rows.insert(Arc::new(db_row));

        assert_eq!(0, db_rows.rows_with_expiration_index.len())
    }

    #[test]
    fn test_that_index_dissapears() {
        let mut db_rows = DbRowsContainer::new();

        let db_row = DbRow::new(
            "testPartitionKey".to_string(),
            "testRowKey".to_string(),
            vec![],
            Some(DateTimeAsMicroseconds::new(1)),
            &JsonTimeStamp::now(),
        );

        let db_row = Arc::new(db_row);
        db_rows.insert(db_row.clone());

        db_rows.remove(db_row.row_key.as_str());

        assert_eq!(0, db_rows.rows_with_expiration_index.len())
    }

    #[test]
    fn test_update_expiration_time_from_no_to() {
        const ROW_KEY: &str = "testRowKey";
        let mut db_rows = DbRowsContainer::new();

        let db_row = DbRow::new(
            "testPartitionKey".to_string(),
            ROW_KEY.to_string(),
            vec![],
            None,
            &JsonTimeStamp::now(),
        );

        let db_row = Arc::new(db_row);
        db_rows.insert(db_row.clone());

        assert_eq!(0, db_rows.rows_with_expiration_index.len());

        db_rows.update_expiration_time(&db_row, Some(DateTimeAsMicroseconds::new(2)));

        assert_eq!(true, db_rows.rows_with_expiration_index.contains_key(&2));
        assert_eq!(1, db_rows.rows_with_expiration_index.len());
    }

    #[test]
    fn test_update_expiration_time_to_new_expiration_time() {
        const ROW_KEY: &str = "testRowKey";
        let mut db_rows = DbRowsContainer::new();

        let db_row = DbRow::new(
            "testPartitionKey".to_string(),
            ROW_KEY.to_string(),
            vec![],
            Some(DateTimeAsMicroseconds::new(1)),
            &JsonTimeStamp::now(),
        );

        let db_row = Arc::new(db_row);
        db_rows.insert(db_row.clone());

        assert_eq!(true, db_rows.rows_with_expiration_index.contains_key(&1));
        assert_eq!(1, db_rows.rows_with_expiration_index.len());

        db_rows.update_expiration_time(&db_row, Some(DateTimeAsMicroseconds::new(2)));

        assert_eq!(true, db_rows.rows_with_expiration_index.contains_key(&2));
        assert_eq!(1, db_rows.rows_with_expiration_index.len());
    }

    #[test]
    fn test_update_expiration_time_from_some_to_no() {
        const ROW_KEY: &str = "testRowKey";
        let mut db_rows = DbRowsContainer::new();

        let db_row = DbRow::new(
            "testPartitionKey".to_string(),
            ROW_KEY.to_string(),
            vec![],
            Some(DateTimeAsMicroseconds::new(1)),
            &JsonTimeStamp::now(),
        );

        let db_row = Arc::new(db_row);
        db_rows.insert(db_row.clone());

        assert_eq!(true, db_rows.rows_with_expiration_index.contains_key(&1));
        assert_eq!(1, db_rows.rows_with_expiration_index.len());

        db_rows.update_expiration_time(&db_row, None);

        assert_eq!(0, db_rows.rows_with_expiration_index.len());
    }

    #[test]
    fn test_we_do_not_have_db_rows_to_expire() {
        const ROW_KEY: &str = "testRowKey";
        let mut db_rows = DbRowsContainer::new();

        let db_row = DbRow::new(
            "testPartitionKey".to_string(),
            ROW_KEY.to_string(),
            vec![],
            Some(DateTimeAsMicroseconds::new(10)),
            &JsonTimeStamp::now(),
        );

        let db_row = Arc::new(db_row);
        db_rows.insert(db_row.clone());

        let rows_to_expire = db_rows.get_rows_to_expire(DateTimeAsMicroseconds::new(9));

        assert_eq!(true, rows_to_expire.is_none());
    }

    #[test]
    fn test_we_do_have_db_rows_to_expire() {
        const ROW_KEY: &str = "testRowKey";
        let mut db_rows = DbRowsContainer::new();

        let db_row = DbRow::new(
            "testPartitionKey".to_string(),
            ROW_KEY.to_string(),
            vec![],
            Some(DateTimeAsMicroseconds::new(10)),
            &JsonTimeStamp::now(),
        );

        let db_row = Arc::new(db_row);
        db_rows.insert(db_row.clone());

        let rows_to_expire = db_rows.get_rows_to_expire(DateTimeAsMicroseconds::new(10));

        assert_eq!(true, rows_to_expire.is_some());
    }
}
