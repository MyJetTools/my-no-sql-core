use my_json::json_writer::JsonArrayWriter;
#[cfg(feature = "master-node")]
use rust_extensions::date_time::DateTimeAsMicroseconds;
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
    #[cfg(feature = "master-node")]
    pub last_write_moment: DateTimeAsMicroseconds,
    #[cfg(feature = "master-node")]
    pub attributes: DbTableAttributes,
}

impl DbTable {
    #[cfg(not(feature = "master-node"))]
    pub fn new(name: String) -> Self {
        Self {
            name,
            partitions: DbPartitionsContainer::new(),
        }
    }

    pub fn get_partitions_amount(&self) -> usize {
        self.partitions.len()
    }

    #[cfg(feature = "master-node")]
    pub fn get_last_write_moment(&self) -> DateTimeAsMicroseconds {
        self.last_write_moment
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
    pub fn insert_or_replace_row(
        &mut self,
        db_row: &Arc<DbRow>,
        #[cfg(feature = "master-node")] set_last_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> Option<Arc<DbRow>> {
        if !self.partitions.has_partition(&db_row.partition_key) {
            let mut db_partition = DbPartition::new();
            db_partition.insert_or_replace_row(db_row.clone());

            self.partitions.insert(&db_row.partition_key, db_partition);

            #[cfg(feature = "master-node")]
            if let Some(set_last_write_moment) = set_last_write_moment {
                self.last_write_moment = set_last_write_moment;
            }

            return None;
        }

        let db_partition = self.partitions.get_mut(&db_row.partition_key).unwrap();
        let removed_db_row = db_partition.insert_or_replace_row(db_row.clone());

        #[cfg(feature = "master-node")]
        if let Some(set_last_write_moment) = set_last_write_moment {
            self.last_write_moment = set_last_write_moment;
            db_partition.last_write_moment = set_last_write_moment;
        }

        removed_db_row
    }

    #[inline]
    pub fn insert_row(
        &mut self,
        db_row: &Arc<DbRow>,
        #[cfg(feature = "master-node")] set_last_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> bool {
        if !self.partitions.has_partition(&db_row.partition_key) {
            self.partitions
                .insert(&db_row.partition_key, DbPartition::new());
        }

        let db_partition = self.partitions.get_mut(&db_row.partition_key).unwrap();

        let result = db_partition.insert_row(db_row.clone());
        #[cfg(feature = "master-node")]
        if result {
            if let Some(set_last_write_moment) = set_last_write_moment {
                self.last_write_moment = DateTimeAsMicroseconds::now();
                db_partition.last_write_moment = set_last_write_moment;
            }
        }

        result
    }

    #[inline]
    pub fn bulk_insert_or_replace(
        &mut self,
        partition_key: &String,
        db_rows: &[Arc<DbRow>],
        #[cfg(feature = "master-node")] set_last_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> Option<Vec<Arc<DbRow>>> {
        if !self.partitions.has_partition(partition_key) {
            self.partitions.insert(partition_key, DbPartition::new());
        }

        let db_partition = self.partitions.get_mut(partition_key).unwrap();

        let result = db_partition.insert_or_replace_rows_bulk(db_rows);
        #[cfg(feature = "master-node")]
        if let Some(set_last_write_moment) = set_last_write_moment {
            self.last_write_moment = set_last_write_moment;
            db_partition.last_write_moment = set_last_write_moment;
        }

        result
    }

    #[inline]
    pub fn init_partition(&mut self, partition_key: String, db_partition: DbPartition) {
        self.partitions.insert(&partition_key, db_partition);
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
        #[cfg(feature = "master-node")] set_last_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> Option<(Arc<DbRow>, bool)> {
        let (removed_row, partition_is_empty) = {
            let db_partition = self.partitions.get_mut(partition_key)?;

            let removed_row = db_partition.remove_row(row_key)?;
            #[cfg(feature = "master-node")]
            if let Some(set_last_write_moment) = set_last_write_moment {
                self.last_write_moment = DateTimeAsMicroseconds::now();
                db_partition.last_write_moment = set_last_write_moment;
            }

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
        #[cfg(feature = "master-node")] set_last_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> Option<(Vec<Arc<DbRow>>, bool)> {
        let (removed_rows, partition_is_empty) = {
            let db_partition = self.partitions.get_mut(partition_key)?;

            let removed_rows = db_partition.remove_rows_bulk(row_keys)?;

            #[cfg(feature = "master-node")]
            if let Some(set_last_write_moment) = set_last_write_moment {
                self.last_write_moment = DateTimeAsMicroseconds::now();
                db_partition.last_write_moment = set_last_write_moment;
            }

            (removed_rows, db_partition.is_empty())
        };

        if delete_empty_partition && partition_is_empty {
            self.partitions.remove(partition_key);
        }

        return Some((removed_rows, partition_is_empty));
    }

    #[inline]
    pub fn remove_partition(
        &mut self,
        partition_key: &String,
        #[cfg(feature = "master-node")] set_last_write_moment: Option<DateTimeAsMicroseconds>,
    ) -> Option<DbPartition> {
        let removed_partition = self.partitions.remove(partition_key);

        #[cfg(feature = "master-node")]
        if removed_partition.is_some() {
            if let Some(set_last_write_moment) = set_last_write_moment {
                self.last_write_moment = set_last_write_moment;
            }
        }

        removed_partition
    }

    pub fn clear_table(&mut self) -> Option<BTreeMap<String, DbPartition>> {
        self.partitions.clear()
    }
}
