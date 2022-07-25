use my_json::json_writer::JsonArrayWriter;
use rust_extensions::date_time::{AtomicDateTimeAsMicroseconds, DateTimeAsMicroseconds};

use std::{
    collections::{btree_map::Values, BTreeMap, HashMap},
    sync::Arc,
};

use crate::{
    db::{db_snapshots::DbPartitionSnapshot, DbPartition, DbRow, UpdateExpirationTimeModel},
    db_json_entity::JsonTimeStamp,
};

pub type TPartitions = BTreeMap<String, DbPartition>;

pub struct DbTableInner {
    pub name: String,
    pub partitions: TPartitions,
    pub created: DateTimeAsMicroseconds,
    pub last_read_time: AtomicDateTimeAsMicroseconds,
    pub last_update_time: DateTimeAsMicroseconds,
    table_size: usize,
}

impl DbTableInner {
    pub fn new(name: String, created: DateTimeAsMicroseconds) -> Self {
        Self {
            name,
            partitions: BTreeMap::new(),
            created,
            last_read_time: AtomicDateTimeAsMicroseconds::new(created.unix_microseconds),
            last_update_time: DateTimeAsMicroseconds::now(),
            table_size: 0,
        }
    }

    pub fn get_partitions_to_expire(&self, max_amount: usize) -> Option<Vec<String>> {
        if self.partitions.len() <= max_amount {
            return None;
        }

        let mut partitions = BTreeMap::new();

        for (pk, db_partition) in &self.partitions {
            partitions.insert(db_partition.get_last_access().unix_microseconds, pk);
        }

        //TODO - UnitTest
        let mut expire_amount = self.partitions.len() - max_amount;

        let mut result = Vec::new();

        for pk in partitions.values() {
            result.push(pk.to_string());

            expire_amount -= 1;
            if expire_amount == 0 {
                break;
            }
        }

        Some(result)
    }

    pub fn get_partitions_amount(&self) -> usize {
        self.partitions.len()
    }

    pub fn get_expiration_index_rows_amount(&self) -> usize {
        let mut result = 0;

        for db_partition in self.partitions.values() {
            result += db_partition.get_expiration_index_rows_amount();
        }

        result
    }

    pub fn get_last_update_time(&self) -> DateTimeAsMicroseconds {
        self.last_update_time
    }

    pub fn get_all_rows<'s>(&'s self) -> Vec<&Arc<DbRow>> {
        let mut result = Vec::new();
        for db_partition in self.partitions.values() {
            result.extend(db_partition.get_all_rows(None));
        }
        result
    }

    pub fn get_all_rows_and_update_expiration_time<'s>(
        &'s mut self,
        update_expiration_time: &UpdateExpirationTimeModel,
    ) -> Vec<Arc<DbRow>> {
        let mut result = Vec::new();
        for db_partition in self.partitions.values_mut() {
            result.extend(
                db_partition.get_all_rows_and_update_expiration_time(None, update_expiration_time),
            );
        }
        result
    }

    pub fn get_table_as_json_array(&self) -> JsonArrayWriter {
        let mut json_array_writer = JsonArrayWriter::new();

        for db_partition in self.partitions.values() {
            for db_row in db_partition.get_all_rows(None) {
                json_array_writer.write_raw_element(db_row.data.as_slice())
            }
        }

        json_array_writer
    }

    pub fn get_rows_amount(&self) -> usize {
        let mut result = 0;
        for db_partition in self.partitions.values() {
            result += db_partition.get_rows_amount();
        }

        result
    }

    pub fn get_table_size(&self) -> usize {
        self.table_size
    }

    pub fn get_partition_as_json_array(&self, partition_key: &str) -> Option<JsonArrayWriter> {
        let mut json_array_writer = JsonArrayWriter::new();

        if let Some(db_partition) = self.partitions.get(partition_key) {
            for db_row in db_partition.get_all_rows(None) {
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
        self.partitions.values()
    }

    pub fn get_partitions_last_write_moment(&self) -> HashMap<String, DateTimeAsMicroseconds> {
        let mut result = HashMap::new();

        for (partition_key, db_partition) in &self.partitions {
            result.insert(
                partition_key.to_string(),
                db_partition.get_last_write_moment(),
            );
        }

        result
    }
}

/// Insert Operations

impl DbTableInner {
    #[inline]
    pub fn insert_or_replace_row(
        &mut self,
        db_row: &Arc<DbRow>,
        update_write_access: &JsonTimeStamp,
    ) -> Option<Arc<DbRow>> {
        self.table_size += db_row.data.len();
        if !self.partitions.contains_key(&db_row.partition_key) {
            let mut db_partition = DbPartition::new();
            db_partition.insert_or_replace_row(db_row.clone(), Some(update_write_access.date_time));

            self.partitions
                .insert(db_row.partition_key.to_string(), db_partition);

            return None;
        }

        let db_partition = self.partitions.get_mut(&db_row.partition_key).unwrap();
        let removed_db_row =
            db_partition.insert_or_replace_row(db_row.clone(), Some(update_write_access.date_time));

        if let Some(removed_db_row) = &removed_db_row {
            self.table_size -= removed_db_row.data.len();
        }

        self.last_update_time = DateTimeAsMicroseconds::now();

        removed_db_row
    }

    #[inline]
    pub fn insert_row(&mut self, db_row: &Arc<DbRow>, update_write_access: &JsonTimeStamp) -> bool {
        if !self.partitions.contains_key(&db_row.partition_key) {
            self.partitions
                .insert(db_row.partition_key.to_string(), DbPartition::new());
        }

        let db_partition = self.partitions.get_mut(&db_row.partition_key).unwrap();

        let result = db_partition.insert_row(db_row.clone(), Some(update_write_access.date_time));

        if result {
            self.table_size += db_row.data.len();
            self.last_update_time = DateTimeAsMicroseconds::now();
        }

        result
    }

    #[inline]
    pub fn bulk_insert_or_replace(
        &mut self,
        partition_key: &str,
        db_rows: &[Arc<DbRow>],
        update_write_access: &JsonTimeStamp,
    ) -> Option<Vec<Arc<DbRow>>> {
        if !self.partitions.contains_key(partition_key) {
            self.partitions
                .insert(partition_key.to_string(), DbPartition::new());
        }

        let db_partition = self.partitions.get_mut(partition_key).unwrap();

        for itm in db_rows {
            self.table_size += itm.data.len();
        }

        let result =
            db_partition.insert_or_replace_rows_bulk(db_rows, Some(update_write_access.date_time));

        if let Some(result) = &result {
            for removed in result {
                self.table_size -= removed.data.len();
            }
        }

        self.last_update_time = DateTimeAsMicroseconds::now();
        result
    }

    #[inline]
    pub fn init_partition(&mut self, partition_key: String, db_partition: DbPartition) {
        self.table_size += db_partition.get_content_size();

        let removed_partition = self.partitions.insert(partition_key, db_partition);

        if let Some(removed_partition) = removed_partition {
            self.table_size -= removed_partition.get_content_size();
        }

        self.last_update_time = DateTimeAsMicroseconds::now();
    }
}

/// Delete Oprations
///
///

impl DbTableInner {
    pub fn remove_row(
        &mut self,
        partition_key: &str,
        row_key: &str,
        delete_empty_partition: bool,
        now: &JsonTimeStamp,
    ) -> Option<(Arc<DbRow>, bool)> {
        let (removed_row, partition_is_empty) = {
            let db_partition = self.partitions.get_mut(partition_key)?;

            let removed_row = db_partition.remove_row(row_key, Some(now.date_time))?;
            self.last_update_time = DateTimeAsMicroseconds::now();
            self.table_size -= removed_row.data.len();

            (removed_row, db_partition.is_empty())
        };

        if delete_empty_partition && partition_is_empty {
            self.partitions.remove(partition_key);
        }

        return Some((removed_row, partition_is_empty));
    }

    pub fn bulk_remove_rows<'s, TIter: Iterator<Item = &'s String>>(
        &mut self,
        partition_key: &str,
        row_keys: TIter,
        delete_empty_partition: bool,
        now: DateTimeAsMicroseconds,
    ) -> Option<(Vec<Arc<DbRow>>, bool)> {
        let (removed_rows, partition_is_empty) = {
            let db_partition = self.partitions.get_mut(partition_key)?;

            let removed_rows = db_partition.remove_rows_bulk(row_keys, Some(now));

            if let Some(removed_rows) = &removed_rows {
                for removed in removed_rows {
                    self.table_size -= removed.data.len();
                }
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

    fn get_partitions_to_gc(&self, max_partitions_amount: usize) -> Option<BTreeMap<i64, String>> {
        if self.partitions.len() <= max_partitions_amount {
            return None;
        }

        let mut partitions_to_gc = BTreeMap::new();

        for (partition_key, partition) in &self.partitions {
            let last_read_access = partition.get_last_access().unix_microseconds;

            partitions_to_gc.insert(last_read_access, partition_key.to_string());
        }

        Some(partitions_to_gc)
    }

    pub fn gc_and_keep_max_partitions_amount(
        &mut self,
        max_partitions_amount: usize,
    ) -> Option<HashMap<String, DbPartition>> {
        let partitions_to_gc = self.get_partitions_to_gc(max_partitions_amount)?;

        let mut result = HashMap::new();

        for (_, partition_key) in partitions_to_gc {
            if self.partitions.len() <= max_partitions_amount {
                break;
            }

            if let Some(partition) = self.partitions.remove(partition_key.as_str()) {
                self.table_size -= partition.get_content_size();
                result.insert(partition_key, partition);
                self.last_update_time = DateTimeAsMicroseconds::now();
            }
        }

        Some(result)
    }

    #[inline]
    pub fn remove_partition(&mut self, partition_key: &str) -> Option<DbPartition> {
        let removed_partition = self.partitions.remove(partition_key);

        if let Some(removed_partition) = &removed_partition {
            self.table_size -= removed_partition.get_content_size();
            self.last_update_time = DateTimeAsMicroseconds::now();
        }

        removed_partition
    }

    pub fn clean_table(&mut self) -> Option<TPartitions> {
        if self.partitions.len() == 0 {
            return None;
        }

        let mut partitions = BTreeMap::new();

        std::mem::swap(&mut partitions, &mut self.partitions);
        self.table_size = 0;
        self.last_update_time = DateTimeAsMicroseconds::now();
        Some(partitions)
    }
}

impl Into<BTreeMap<String, DbPartitionSnapshot>> for &DbTableInner {
    fn into(self) -> BTreeMap<String, DbPartitionSnapshot> {
        let mut result: BTreeMap<String, DbPartitionSnapshot> = BTreeMap::new();

        for (partition_key, db_partition) in &self.partitions {
            result.insert(partition_key.to_string(), db_partition.into());
        }

        result
    }
}