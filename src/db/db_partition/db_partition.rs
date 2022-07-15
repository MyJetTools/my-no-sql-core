use my_json::json_writer::JsonArrayWriter;
use rust_extensions::{
    date_time::{AtomicDateTimeAsMicroseconds, DateTimeAsMicroseconds},
    lazy::LazyVec,
};

use crate::{
    db::{
        db_snapshots::{DbPartitionSnapshot, DbRowsSnapshot},
        update_expiration_time_model::UpdateExpirationDateTime,
        DbRow, UpdateExpirationTimeModel,
    },
    utils::SortedDictionary,
};
use std::{collections::btree_map::Values, sync::Arc};

use super::DbRowsContainer;

pub enum UpdatePartitionReadMoment {
    Update(DateTimeAsMicroseconds),
    UpdateIfElementIsFound(DateTimeAsMicroseconds),
    None,
}

pub struct DbPartition {
    expires: Option<DateTimeAsMicroseconds>,
    rows: DbRowsContainer,
    last_read_moment: AtomicDateTimeAsMicroseconds,
    last_write_moment: AtomicDateTimeAsMicroseconds,
    content_size: usize,
}

impl DbPartition {
    pub fn new() -> DbPartition {
        DbPartition {
            rows: DbRowsContainer::new(),
            last_read_moment: AtomicDateTimeAsMicroseconds::now(),
            last_write_moment: AtomicDateTimeAsMicroseconds::now(),
            content_size: 0,
            expires: None,
        }
    }

    fn update_last_read_moment(&self, update: UpdatePartitionReadMoment, found_element: bool) {
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

    pub fn get_rows_to_expire(&self, now: DateTimeAsMicroseconds) -> Option<Vec<String>> {
        self.rows.get_rows_to_expire(now)
    }

    pub fn get_content_size(&self) -> usize {
        self.content_size
    }

    pub fn rows_count(&self) -> usize {
        return self.rows.len();
    }

    pub fn get_last_write_moment(&self) -> DateTimeAsMicroseconds {
        self.last_write_moment.as_date_time()
    }

    #[inline]
    pub fn insert_row(
        &mut self,
        db_row: Arc<DbRow>,
        update_last_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> bool {
        if self.rows.has_db_row(db_row.row_key.as_str()) {
            return false;
        }

        self.content_size += db_row.data.len();
        self.insert_or_replace_row(db_row, update_last_write_moment);
        return true;
    }

    #[inline]
    pub fn insert_or_replace_row(
        &mut self,
        db_row: Arc<DbRow>,
        update_last_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> Option<Arc<DbRow>> {
        if let Some(update_last_write_moment) = update_last_write_moment {
            self.last_write_moment.update(update_last_write_moment);
        }

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
        update_last_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> Option<Vec<Arc<DbRow>>> {
        if let Some(update_last_write_moment) = update_last_write_moment {
            self.last_write_moment.update(update_last_write_moment);
        }

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

    pub fn remove_row(
        &mut self,
        row_key: &str,
        update_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> Option<Arc<DbRow>> {
        let result = self.rows.remove(row_key);

        if let Some(removed_item) = result.as_ref() {
            if let Some(write_moment) = update_write_moment {
                self.last_write_moment.update(write_moment);
            }
            self.content_size -= removed_item.data.len();
        }
        result
    }

    pub fn remove_rows_bulk<'s, TRowsIterator: Iterator<Item = &'s String>>(
        &mut self,
        row_keys: TRowsIterator,
        update_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> Option<Vec<Arc<DbRow>>> {
        let mut result = LazyVec::new();

        for row_key in row_keys {
            if let Some(removed_item) = self.rows.remove(row_key) {
                self.content_size -= removed_item.data.len();
                result.add(removed_item);
            }
        }

        if !result.is_empty() {
            if let Some(write_moment) = update_write_moment {
                self.last_write_moment.update(write_moment);
            }
        }

        result.get_result()
    }

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

    pub fn get_all_rows<'s>(
        &'s self,
        update_last_read_moment: Option<DateTimeAsMicroseconds>,
    ) -> Values<'s, String, Arc<DbRow>> {
        if let Some(update_last_read_moment) = update_last_read_moment {
            self.last_read_moment.update(update_last_read_moment);
        }

        self.rows.get_all()
    }

    pub fn get_all_rows_and_update_expiration_time<'s>(
        &'s mut self,
        update_last_read_moment: Option<DateTimeAsMicroseconds>,
        update_expiration_time: &UpdateExpirationTimeModel,
    ) -> Vec<Arc<DbRow>> {
        if let Some(update_last_read_moment) = update_last_read_moment {
            self.last_read_moment.update(update_last_read_moment);
        }

        self.rows
            .get_all_and_update_expiration_time(update_expiration_time)
    }

    pub fn get_all_rows_cloned<'s>(
        &'s self,
        update_last_read_moment: Option<DateTimeAsMicroseconds>,
    ) -> Vec<Arc<DbRow>> {
        if let Some(update_last_read_moment) = update_last_read_moment {
            self.last_read_moment.update(update_last_read_moment);
        }

        self.rows.get_all().map(|itm| itm.clone()).collect()
    }

    pub fn get_rows_amount(&self) -> usize {
        self.rows.len()
    }

    pub fn get_expiration_index_rows_amount(&self) -> usize {
        self.rows.rows_with_expiration_index_len()
    }

    //TODO - Продолжить ревьювить content рассчет Content Size

    pub fn get_row(
        &self,
        row_key: &str,
        update_last_read_moment: UpdatePartitionReadMoment,
    ) -> Option<&Arc<DbRow>> {
        let result = self.rows.get(row_key);
        self.update_last_read_moment(update_last_read_moment, result.is_some());
        result
    }

    pub fn get_row_and_update_expiration_time(
        &mut self,
        row_key: &str,
        update_last_read_moment: UpdatePartitionReadMoment,
        expiration_time: &UpdateExpirationTimeModel,
    ) -> Option<Arc<DbRow>> {
        let result = self
            .rows
            .get_and_update_expiration_time(row_key, expiration_time);
        self.update_last_read_moment(update_last_read_moment, result.is_some());
        result
    }

    pub fn get_row_and_clone(
        &self,
        row_key: &str,
        update_last_read_moment: Option<DateTimeAsMicroseconds>,
    ) -> Option<Arc<DbRow>> {
        let result = self.rows.get(row_key)?;

        if let Some(update_time) = update_last_read_moment {
            result.last_read_access.update(update_time);
        }

        Some(result.clone())
    }

    pub fn gc_rows(&mut self, max_rows_amount: usize) -> Option<Vec<Arc<DbRow>>> {
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
        update_last_read_moment: Option<DateTimeAsMicroseconds>,
        limit: Option<usize>,
    ) -> Vec<&Arc<DbRow>> {
        if let Some(read_moment) = update_last_read_moment {
            self.last_read_moment.update(read_moment);
        }

        return self.rows.get_highest_row_and_below(row_key, limit);
    }

    pub fn get_highest_row_and_below_and_update_expiration_time(
        &mut self,
        row_key: &String,
        update_last_read_moment: Option<DateTimeAsMicroseconds>,
        limit: Option<usize>,
        update_expiration_time: &UpdateExpirationTimeModel,
    ) -> Vec<Arc<DbRow>> {
        if let Some(read_moment) = update_last_read_moment {
            self.last_read_moment.update(read_moment);
        }

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

    pub fn get_last_access(&self) -> DateTimeAsMicroseconds {
        let last_read_moment = self.last_read_moment.as_date_time();
        let last_write_access = self.last_write_moment.as_date_time();

        if last_read_moment.unix_microseconds > last_write_access.unix_microseconds {
            return last_read_moment;
        }

        return last_write_access;
    }
}

impl Into<DbRowsSnapshot> for &DbPartition {
    fn into(self) -> DbRowsSnapshot {
        DbRowsSnapshot::new_from_snapshot(self.rows.get_all().map(|itm| itm.clone()).collect())
    }
}

impl Into<DbPartitionSnapshot> for &DbPartition {
    fn into(self) -> DbPartitionSnapshot {
        DbPartitionSnapshot {
            last_read_moment: self.last_read_moment.as_date_time(),
            last_write_moment: self.last_write_moment.as_date_time(),
            db_rows_snapshot: self.into(),
        }
    }
}
