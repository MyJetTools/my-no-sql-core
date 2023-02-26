use my_json::json_writer::JsonArrayWriter;
use rust_extensions::lazy::LazyVec;

#[cfg(feature = "master-node")]
use rust_extensions::date_time::AtomicDateTimeAsMicroseconds;

use crate::db::DbRow;

use std::{collections::btree_map::Values, sync::Arc};

use super::DbRowsContainer;

pub struct DbPartition {
    #[cfg(feature = "master-node")]
    pub expires: Option<rust_extensions::date_time::DateTimeAsMicroseconds>,
    pub rows: DbRowsContainer,
    #[cfg(feature = "master-node")]
    pub last_read_moment: AtomicDateTimeAsMicroseconds,
    #[cfg(feature = "master-node")]
    pub last_write_moment: rust_extensions::date_time::DateTimeAsMicroseconds,
    content_size: usize,
}

impl DbPartition {
    pub fn new() -> DbPartition {
        DbPartition {
            rows: DbRowsContainer::new(),
            #[cfg(feature = "master-node")]
            last_read_moment: AtomicDateTimeAsMicroseconds::now(),
            #[cfg(feature = "master-node")]
            last_write_moment: rust_extensions::date_time::DateTimeAsMicroseconds::now(),
            content_size: 0,
            #[cfg(feature = "master-node")]
            expires: None,
        }
    }

    #[cfg(feature = "master-node")]
    pub fn get_rows_to_expire(
        &self,
        now: rust_extensions::date_time::DateTimeAsMicroseconds,
    ) -> Option<Vec<&Arc<DbRow>>> {
        self.rows.get_rows_to_expire(now)
    }

    pub fn get_content_size(&self) -> usize {
        self.content_size
    }

    pub fn rows_count(&self) -> usize {
        return self.rows.len();
    }

    #[inline]
    pub fn insert_row(&mut self, db_row: Arc<DbRow>) -> bool {
        if self.rows.has_db_row(db_row.row_key.as_str()) {
            return false;
        }

        self.insert_or_replace_row(db_row);
        return true;
    }

    #[inline]
    pub fn insert_or_replace_row(&mut self, db_row: Arc<DbRow>) -> Option<Arc<DbRow>> {
        self.content_size += db_row.data.len();

        let result = self.rows.insert(db_row);

        if let Some(removed_item) = result.as_ref() {
            self.content_size -= removed_item.data.len();
        }

        result
    }

    #[inline]
    pub fn insert_or_replace_rows_bulk(
        &mut self,
        db_rows: &[Arc<DbRow>],
    ) -> Option<Vec<Arc<DbRow>>> {
        let mut result = LazyVec::new();

        for db_row in db_rows {
            self.content_size += db_row.data.len();

            if let Some(removed_item) = self.rows.insert(db_row.clone()) {
                self.content_size -= removed_item.data.len();
                result.add(removed_item);
            }
        }

        result.get_result()
    }

    pub fn remove_row(&mut self, row_key: &str) -> Option<Arc<DbRow>> {
        let result = self.rows.remove(row_key);

        if let Some(removed_item) = result.as_ref() {
            self.content_size -= removed_item.data.len();
        }
        result
    }

    pub fn remove_rows_bulk<'s, TRowsIterator: Iterator<Item = &'s String>>(
        &mut self,
        row_keys: TRowsIterator,
    ) -> Option<Vec<Arc<DbRow>>> {
        let mut result = LazyVec::new();

        for row_key in row_keys {
            if let Some(removed_item) = self.rows.remove(row_key) {
                self.content_size -= removed_item.data.len();
                result.add(removed_item);
            }
        }

        result.get_result()
    }

    pub fn get_all_rows<'s>(&'s self) -> Values<'s, String, Arc<DbRow>> {
        self.rows.get_all()
    }

    pub fn get_all_rows_cloned<'s>(&'s self) -> Vec<Arc<DbRow>> {
        self.rows.get_all().map(|itm| itm.clone()).collect()
    }

    pub fn get_rows_amount(&self) -> usize {
        self.rows.len()
    }

    #[cfg(feature = "master-node")]
    pub fn get_expiration_index_rows_amount(&self) -> usize {
        self.rows.rows_with_expiration_index_len()
    }

    pub fn get_row(&self, row_key: &str) -> Option<&Arc<DbRow>> {
        let result = self.rows.get(row_key);
        result
    }

    pub fn get_row_and_clone(&self, row_key: &str) -> Option<Arc<DbRow>> {
        let result = self.rows.get(row_key)?;
        Some(result.clone())
    }

    pub fn fill_with_json_data(&self, json_array_writer: &mut JsonArrayWriter) {
        for db_row in self.rows.get_all() {
            json_array_writer.write_raw_element(db_row.data.as_slice());
        }
    }

    pub fn get_highest_row_and_below(
        &self,
        row_key: &String,
        limit: Option<usize>,
    ) -> Option<Vec<&Arc<DbRow>>> {
        return self.rows.get_highest_row_and_below(row_key, limit);
    }

    pub fn is_empty(&self) -> bool {
        self.rows.len() == 0
    }
}

#[cfg(feature = "master-node")]
impl DbPartition {
    pub fn update_last_read_moment(&self, now: rust_extensions::date_time::DateTimeAsMicroseconds) {
        self.last_read_moment.update(now);
    }

    pub fn get_last_write_moment(&self) -> rust_extensions::date_time::DateTimeAsMicroseconds {
        self.last_write_moment
    }

    pub fn get_last_read_moment(&self) -> rust_extensions::date_time::DateTimeAsMicroseconds {
        self.last_read_moment.as_date_time()
    }
}
