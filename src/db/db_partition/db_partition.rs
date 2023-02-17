use my_json::json_writer::JsonArrayWriter;
use rust_extensions::{date_time::DateTimeAsMicroseconds, lazy::LazyVec};

#[cfg(feature = "master_node")]
use rust_extensions::date_time::AtomicDateTimeAsMicroseconds;

#[cfg(feature = "master_node")]
use crate::db::{
    update_expiration_time_model::UpdateExpirationDateTime, UpdateExpirationTimeModel,
};

use crate::db::DbRow;

use std::{collections::btree_map::Values, sync::Arc};

use super::DbRowsContainer;

pub enum UpdatePartitionReadMoment {
    Update(DateTimeAsMicroseconds),
    UpdateIfElementIsFound(DateTimeAsMicroseconds),
    None,
}

pub struct DbPartition {
    #[cfg(feature = "master_node")]
    pub expires: Option<DateTimeAsMicroseconds>,
    pub rows: DbRowsContainer,
    #[cfg(feature = "master_node")]
    pub last_read_moment: AtomicDateTimeAsMicroseconds,
    #[cfg(feature = "master_node")]
    pub last_write_moment: AtomicDateTimeAsMicroseconds,
    content_size: usize,
}

impl DbPartition {
    pub fn new() -> DbPartition {
        DbPartition {
            rows: DbRowsContainer::new(),
            #[cfg(feature = "master_node")]
            last_read_moment: AtomicDateTimeAsMicroseconds::now(),
            #[cfg(feature = "master_node")]
            last_write_moment: AtomicDateTimeAsMicroseconds::now(),
            content_size: 0,
            #[cfg(feature = "master_node")]
            expires: None,
        }
    }

    #[cfg(feature = "master_node")]
    pub fn get_rows_to_expire(&self, now: DateTimeAsMicroseconds) -> Option<Vec<String>> {
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

    #[cfg(feature = "master_node")]
    pub fn get_rows_and_update_expiration_time(
        &mut self,
        row_keys: &[String],
        update_expiration_time: &UpdateExpirationTimeModel,
    ) -> Option<Vec<Arc<DbRow>>> {
        let mut result = LazyVec::new();

        for row_key in row_keys {
            if let Some(db_row) = self
                .rows
                .get_and_update_expiration_time(row_key, update_expiration_time)
            {
                result.add(db_row);
            }
        }

        result.get_result()
    }

    pub fn get_all_rows<'s>(&'s self) -> Values<'s, String, Arc<DbRow>> {
        self.rows.get_all()
    }

    #[cfg(feature = "master_node")]
    pub fn get_all_rows_and_update_expiration_time<'s>(
        &'s mut self,
        update_expiration_time: &UpdateExpirationTimeModel,
    ) -> Vec<Arc<DbRow>> {
        self.rows
            .get_all_and_update_expiration_time(update_expiration_time)
    }

    pub fn get_all_rows_cloned<'s>(&'s self) -> Vec<Arc<DbRow>> {
        self.rows.get_all().map(|itm| itm.clone()).collect()
    }

    pub fn get_rows_amount(&self) -> usize {
        self.rows.len()
    }

    #[cfg(feature = "master_node")]
    pub fn get_expiration_index_rows_amount(&self) -> usize {
        self.rows.rows_with_expiration_index_len()
    }

    //TODO - Продолжить ревьювить content рассчет Content Size

    pub fn get_row(&self, row_key: &str) -> Option<&Arc<DbRow>> {
        let result = self.rows.get(row_key);
        result
    }
    #[cfg(feature = "master_node")]
    pub fn get_row_and_update_expiration_time(
        &mut self,
        row_key: &str,
        expiration_time: &UpdateExpirationTimeModel,
    ) -> Option<Arc<DbRow>> {
        let result = self
            .rows
            .get_and_update_expiration_time(row_key, expiration_time);
        result
    }

    pub fn get_row_and_clone(&self, row_key: &str) -> Option<Arc<DbRow>> {
        let result = self.rows.get(row_key)?;
        Some(result.clone())
    }

    pub fn get_highest_row_and_below(
        &self,
        row_key: &String,
        limit: Option<usize>,
    ) -> Vec<&Arc<DbRow>> {
        return self.rows.get_highest_row_and_below(row_key, limit);
    }

    #[cfg(feature = "master_node")]
    pub fn get_highest_row_and_below_and_update_expiration_time(
        &mut self,
        row_key: &String,
        limit: Option<usize>,
        update_expiration_time: &UpdateExpirationTimeModel,
    ) -> Vec<Arc<DbRow>> {
        if let UpdateExpirationDateTime::Yes(expiration_moment) =
            update_expiration_time.update_db_partition_expiration_time
        {
            self.expires = expiration_moment;
        }

        return self
            .rows
            .get_highest_row_and_below_and_update_expiration_time(
                row_key,
                limit,
                update_expiration_time,
            );
    }

    pub fn fill_with_json_data(&self, json_array_writer: &mut JsonArrayWriter) {
        for db_row in self.rows.get_all() {
            json_array_writer.write_raw_element(db_row.data.as_slice());
        }
    }

    pub fn is_empty(&self) -> bool {
        self.rows.len() == 0
    }
}

#[cfg(feature = "master_node")]
impl DbPartition {
    pub fn update_last_read_moment(&self, update: UpdatePartitionReadMoment, found_element: bool) {
        match update {
            UpdatePartitionReadMoment::Update(now) => {
                self.last_write_moment.update(now);
            }
            UpdatePartitionReadMoment::UpdateIfElementIsFound(now) => {
                if found_element {
                    self.last_write_moment.update(now);
                }
            }
            UpdatePartitionReadMoment::None => {}
        }
    }

    pub fn get_last_write_moment(&self) -> DateTimeAsMicroseconds {
        self.last_write_moment.as_date_time()
    }

    pub fn gc_rows(&mut self, max_rows_amount: usize) -> Option<Vec<Arc<DbRow>>> {
        use crate::utils::SortedDictionary;

        if self.rows.len() == 0 {
            return None;
        }

        let mut partitions_by_date_time: SortedDictionary<i64, String> = SortedDictionary::new();

        for db_row in &mut self.rows.get_all() {
            let mut last_access = db_row.last_read_access.as_date_time();

            let last_access_before_insert = last_access;

            while partitions_by_date_time.contains_key(&last_access.unix_microseconds) {
                last_access.unix_microseconds += 1;
            }

            partitions_by_date_time
                .insert(last_access.unix_microseconds, db_row.row_key.to_string());

            if last_access_before_insert.unix_microseconds != last_access.unix_microseconds {
                db_row.last_read_access.update(last_access);
            }
        }

        let mut gced = None;

        while self.rows.len() > max_rows_amount {
            let (dt, row_key) = partitions_by_date_time.first().unwrap();

            let removed_result = self.rows.remove(&row_key);

            if let Some(db_row) = removed_result {
                if gced.is_none() {
                    gced = Some(Vec::new())
                }

                gced.as_mut().unwrap().push(db_row);
            }

            partitions_by_date_time.remove(&dt);
        }

        gced
    }

    pub fn get_highest_row_and_below(
        &self,
        row_key: &String,
        limit: Option<usize>,
    ) -> Vec<&Arc<DbRow>> {
        return self.rows.get_highest_row_and_below(row_key, limit);
    }

    pub fn get_last_access(&self) -> rust_extensions::date_time::DateTimeAsMicroseconds {
        let last_read_moment = self.last_read_moment.as_date_time();
        let last_write_access = self.last_write_moment.as_date_time();

        if last_read_moment.unix_microseconds > last_write_access.unix_microseconds {
            return last_read_moment;
        }

        return last_write_access;
    }
}
