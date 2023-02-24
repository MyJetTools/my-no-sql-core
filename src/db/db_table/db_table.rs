use my_json::json_writer::JsonArrayWriter;
use rust_extensions::date_time::{AtomicDateTimeAsMicroseconds, DateTimeAsMicroseconds};

use std::{
    collections::{btree_map::Values, BTreeMap},
    sync::Arc,
};

#[cfg(feature = "master-node")]
use std::collections::HashMap;

use crate::db::{DbPartition, DbRow};

#[cfg(feature = "master-node")]
use super::DbTableAttributes;

pub type TPartitions = BTreeMap<String, DbPartition>;

pub struct DbTable {
    pub name: String,
    pub partitions: TPartitions,
    pub last_read_time: AtomicDateTimeAsMicroseconds,
    pub last_update_time: DateTimeAsMicroseconds,
    #[cfg(feature = "master-node")]
    pub attributes: DbTableAttributes,
}

impl DbTable {
    #[cfg(feature = "master-node")]
    pub fn new(name: String, attributes: DbTableAttributes) -> Self {
        Self {
            name,
            partitions: BTreeMap::new(),
            last_read_time: AtomicDateTimeAsMicroseconds::new(attributes.created.unix_microseconds),
            last_update_time: DateTimeAsMicroseconds::now(),
            #[cfg(feature = "master-node")]
            attributes,
        }
    }

    #[cfg(not(feature = "master-node"))]
    pub fn new(name: String) -> Self {
        Self {
            name,
            partitions: BTreeMap::new(),
            last_read_time: AtomicDateTimeAsMicroseconds::now(),
            last_update_time: DateTimeAsMicroseconds::now(),
        }
    }

    #[cfg(feature = "master-node")]
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

    #[cfg(feature = "master-node")]
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
            result.extend(db_partition.get_all_rows());
        }
        result
    }

    pub fn get_table_as_json_array(&self) -> JsonArrayWriter {
        let mut json_array_writer = JsonArrayWriter::new();

        for db_partition in self.partitions.values() {
            for db_row in db_partition.get_all_rows() {
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
        let mut result = 0;
        for db_partition in self.partitions.values() {
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

    #[cfg(feature = "master-node")]
    pub fn get_partitions_last_write_moment(&self) -> HashMap<String, DateTimeAsMicroseconds> {
        let mut result = HashMap::new();

        for (pk, db_partition) in &self.partitions {
            result.insert(pk.to_string(), db_partition.get_last_write_moment());
        }

        result
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
}

/// Insert Operations

impl DbTable {
    #[inline]
    pub fn insert_or_replace_row(&mut self, db_row: &Arc<DbRow>) -> Option<Arc<DbRow>> {
        if !self.partitions.contains_key(&db_row.partition_key) {
            let mut db_partition = DbPartition::new();
            db_partition.insert_or_replace_row(db_row.clone());

            self.partitions
                .insert(db_row.partition_key.to_string(), db_partition);

            return None;
        }

        let db_partition = self.partitions.get_mut(&db_row.partition_key).unwrap();
        let removed_db_row = db_partition.insert_or_replace_row(db_row.clone());

        self.last_update_time = DateTimeAsMicroseconds::now();

        removed_db_row
    }

    #[inline]
    pub fn insert_row(&mut self, db_row: &Arc<DbRow>) -> bool {
        if !self.partitions.contains_key(&db_row.partition_key) {
            self.partitions
                .insert(db_row.partition_key.to_string(), DbPartition::new());
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
        partition_key: &str,
        db_rows: &[Arc<DbRow>],
    ) -> Option<Vec<Arc<DbRow>>> {
        if !self.partitions.contains_key(partition_key) {
            self.partitions
                .insert(partition_key.to_string(), DbPartition::new());
        }

        let db_partition = self.partitions.get_mut(partition_key).unwrap();

        let result = db_partition.insert_or_replace_rows_bulk(db_rows);

        self.last_update_time = DateTimeAsMicroseconds::now();
        result
    }

    #[inline]
    pub fn init_partition(&mut self, partition_key: String, db_partition: DbPartition) {
        self.partitions.insert(partition_key, db_partition);
        self.last_update_time = DateTimeAsMicroseconds::now();
    }
}

/// Delete Oprations
///
///

impl DbTable {
    pub fn remove_row(
        &mut self,
        partition_key: &str,
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
        partition_key: &str,
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

    #[cfg(feature = "master-node")]
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

    #[cfg(feature = "master-node")]
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
                result.insert(partition_key, partition);
                self.last_update_time = DateTimeAsMicroseconds::now();
            }
        }

        Some(result)
    }

    #[inline]
    pub fn remove_partition(&mut self, partition_key: &str) -> Option<DbPartition> {
        let removed_partition = self.partitions.remove(partition_key);

        if removed_partition.is_some() {
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

        self.last_update_time = DateTimeAsMicroseconds::now();
        Some(partitions)
    }
}

#[cfg(feature = "master-node")]
#[cfg(test)]
mod tests {
    use crate::db_json_entity::{DbJsonEntity, JsonTimeStamp};

    use super::*;

    #[test]
    fn test_insert_record() {
        let mut db_table = DbTable::new(
            "test-table".to_string(),
            DbTableAttributes::create_default(),
        );

        let now = JsonTimeStamp::now();

        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let db_row = db_json_entity.new_db_row(&now);

        let db_row = Arc::new(db_row);

        db_table.insert_row(&db_row);

        assert_eq!(db_table.get_table_size(), db_row.data.len());
        assert_eq!(db_table.get_partitions_amount(), 1);
    }

    #[test]
    fn test_insert_and_insert_or_replace() {
        let mut db_table = DbTable::new(
            "test-table".to_string(),
            DbTableAttributes::create_default(),
        );

        let now = JsonTimeStamp::now();

        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let db_row = db_json_entity.new_db_row(&now);

        let db_row = Arc::new(db_row);

        db_table.insert_row(&db_row);

        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "AAA": "111"
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let db_row2 = db_json_entity.new_db_row(&now);

        let db_row2 = Arc::new(db_row2);

        db_table.insert_or_replace_row(&db_row2);

        assert_eq!(db_table.get_table_size(), db_row2.data.len());
        assert_eq!(db_table.get_partitions_amount(), 1);
    }
}
