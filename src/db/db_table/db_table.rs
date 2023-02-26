use my_json::json_writer::JsonArrayWriter;
use rust_extensions::date_time::{AtomicDateTimeAsMicroseconds, DateTimeAsMicroseconds};

use std::{
    collections::{btree_map::Values, BTreeMap},
    sync::Arc,
};

use crate::db::{DbPartition, DbRow};

use super::DbPartitionsContainer;
#[cfg(feature = "master-node")]
use super::DbTableAttributes;

pub struct DbTable {
    pub name: String,
    pub partitions: DbPartitionsContainer,
    pub last_read_time: AtomicDateTimeAsMicroseconds,
    pub last_update_time: DateTimeAsMicroseconds,
    #[cfg(feature = "master-node")]
    pub attributes: DbTableAttributes,
}

impl DbTable {
    #[cfg(not(feature = "master-node"))]
    pub fn new(name: String) -> Self {
        Self {
            name,
            partitions: DbPartitionsContainer::new(),
            last_read_time: AtomicDateTimeAsMicroseconds::now(),
            last_update_time: DateTimeAsMicroseconds::now(),
        }
    }

    pub fn get_partitions_amount(&self) -> usize {
        self.partitions.len()
    }

    pub fn get_last_update_time(&self) -> DateTimeAsMicroseconds {
        self.last_update_time
    }

    pub fn get_all_rows<'s>(&'s self) -> Vec<&Arc<DbRow>> {
        let mut result = Vec::new();
        for db_partition in self.partitions.get_partitions() {
            result.extend(db_partition.get_all_rows());
        }
        result
    }

    pub fn get_table_as_json_array(&self) -> JsonArrayWriter {
        let mut json_array_writer = JsonArrayWriter::new();

        for db_partition in self.partitions.get_partitions() {
            for db_row in db_partition.get_all_rows() {
                json_array_writer.write_raw_element(db_row.data.as_slice())
            }
        }

        json_array_writer
    }

    pub fn get_rows_amount(&self) -> usize {
        let mut result = 0;
        for db_partition in self.partitions.get_partitions() {
            result += db_partition.get_rows_amount();
        }

        result
    }

    pub fn get_table_size(&self) -> usize {
        let mut result = 0;
        for db_partition in self.partitions.get_partitions() {
            result += db_partition.get_content_size();
        }
        result
    }

    pub fn get_partition_as_json_array(&self, partition_key: &str) -> Option<JsonArrayWriter> {
        let mut json_array_writer = JsonArrayWriter::new();

        if let Some(db_partition) = self.partitions.get(partition_key) {
            for db_row in db_partition.get_all_rows() {
                json_array_writer.write_raw_element(db_row.data.as_slice())
            }
        }

        json_array_writer.into()
    }

    #[inline]
    pub fn get_partition_mut(&mut self, partition_key: &str) -> Option<&mut DbPartition> {
        self.partitions.get_mut(partition_key)
    }

    #[inline]
    pub fn get_partition(&self, partition_key: &str) -> Option<&DbPartition> {
        self.partitions.get(partition_key)
    }
    #[inline]
    pub fn get_partitions(&self) -> Values<String, DbPartition> {
        self.partitions.get_partitions()
    }
}

/// Insert Operations

impl DbTable {
    #[inline]
    pub fn insert_or_replace_row(&mut self, db_row: &Arc<DbRow>) -> Option<Arc<DbRow>> {
        if !self.partitions.has_partition(&db_row.partition_key) {
            let mut db_partition = DbPartition::new();
            db_partition.insert_or_replace_row(db_row.clone());

            self.partitions.insert(&db_row.partition_key, db_partition);

            return None;
        }

        let db_partition = self.partitions.get_mut(&db_row.partition_key).unwrap();
        let removed_db_row = db_partition.insert_or_replace_row(db_row.clone());

        self.last_update_time = DateTimeAsMicroseconds::now();

        removed_db_row
    }

    #[inline]
    pub fn insert_row(&mut self, db_row: &Arc<DbRow>) -> bool {
        if !self.partitions.has_partition(&db_row.partition_key) {
            self.partitions
                .insert(&db_row.partition_key, DbPartition::new());
        }

        let db_partition = self.partitions.get_mut(&db_row.partition_key).unwrap();

        let result = db_partition.insert_row(db_row.clone());

        if result {
            self.last_update_time = DateTimeAsMicroseconds::now();
        }

        result
    }

    #[inline]
    pub fn bulk_insert_or_replace(
        &mut self,
        partition_key: &String,
        db_rows: &[Arc<DbRow>],
    ) -> Option<Vec<Arc<DbRow>>> {
        if !self.partitions.has_partition(partition_key) {
            self.partitions.insert(partition_key, DbPartition::new());
        }

        let db_partition = self.partitions.get_mut(partition_key).unwrap();

        let result = db_partition.insert_or_replace_rows_bulk(db_rows);

        self.last_update_time = DateTimeAsMicroseconds::now();
        result
    }

    #[inline]
    pub fn init_partition(&mut self, partition_key: String, db_partition: DbPartition) {
        self.partitions.insert(&partition_key, db_partition);
        self.last_update_time = DateTimeAsMicroseconds::now();
    }
}

/// Delete Oprations
///
///

impl DbTable {
    pub fn remove_row(
        &mut self,
        partition_key: &String,
        row_key: &str,
        delete_empty_partition: bool,
    ) -> Option<(Arc<DbRow>, bool)> {
        let (removed_row, partition_is_empty) = {
            let db_partition = self.partitions.get_mut(partition_key)?;

            let removed_row = db_partition.remove_row(row_key)?;
            self.last_update_time = DateTimeAsMicroseconds::now();

            (removed_row, db_partition.is_empty())
        };

        if delete_empty_partition && partition_is_empty {
            self.partitions.remove(partition_key);
        }

        return Some((removed_row, partition_is_empty));
    }

    pub fn bulk_remove_rows<'s, TIter: Iterator<Item = &'s String>>(
        &mut self,
        partition_key: &String,
        row_keys: TIter,
        delete_empty_partition: bool,
    ) -> Option<(Vec<Arc<DbRow>>, bool)> {
        let (removed_rows, partition_is_empty) = {
            let db_partition = self.partitions.get_mut(partition_key)?;

            let removed_rows = db_partition.remove_rows_bulk(row_keys);

            if removed_rows.is_some() {
                self.last_update_time = DateTimeAsMicroseconds::now();
            }

            (removed_rows, db_partition.is_empty())
        };

        let removed_rows = removed_rows?;

        if delete_empty_partition && partition_is_empty {
            self.partitions.remove(partition_key);
        }

        return Some((removed_rows, partition_is_empty));
    }

    #[inline]
    pub fn remove_partition(&mut self, partition_key: &String) -> Option<DbPartition> {
        let removed_partition = self.partitions.remove(partition_key);

        if removed_partition.is_some() {
            self.last_update_time = DateTimeAsMicroseconds::now();
        }

        removed_partition
    }

    pub fn clear_table(&mut self) -> Option<BTreeMap<String, DbPartition>> {
        self.partitions.clear()
    }
}
