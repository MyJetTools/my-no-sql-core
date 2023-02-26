#[cfg(feature = "master-node")]
use rust_extensions::date_time::DateTimeAsMicroseconds;
use std::collections::{btree_map::Values, BTreeMap};

use crate::db::DbPartition;

pub struct DbPartitionsContainer {
    partitions: BTreeMap<String, DbPartition>,
    #[cfg(feature = "master-node")]
    partitions_to_expire_index: crate::ExpirationIndex<String>,
}

impl DbPartitionsContainer {
    pub fn new() -> Self {
        Self {
            partitions: BTreeMap::new(),
            #[cfg(feature = "master-node")]
            partitions_to_expire_index: crate::ExpirationIndex::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    pub fn get_partitions(&self) -> Values<String, DbPartition> {
        self.partitions.values()
    }

    pub fn get_partitions_mut(
        &mut self,
    ) -> std::collections::btree_map::ValuesMut<String, DbPartition> {
        self.partitions.values_mut()
    }
    #[cfg(feature = "master-node")]
    pub fn get_partitions_to_expire(&self, now: DateTimeAsMicroseconds) -> Option<Vec<&String>> {
        self.partitions_to_expire_index.get_items_to_expire(now)
    }

    pub fn get_all(&self) -> &BTreeMap<String, DbPartition> {
        &self.partitions
    }

    pub fn get(&self, partition_key: &str) -> Option<&DbPartition> {
        self.partitions.get(partition_key)
    }

    pub fn get_mut(&mut self, partition_key: &str) -> Option<&mut DbPartition> {
        self.partitions.get_mut(partition_key)
    }

    pub fn has_partition(&self, partition_key: &str) -> bool {
        self.partitions.contains_key(partition_key)
    }

    pub fn insert(&mut self, partition_key: &String, db_partition: DbPartition) {
        #[cfg(feature = "master-node")]
        let new_expires = db_partition.expires;

        let _removed_partition = self
            .partitions
            .insert(partition_key.to_string(), db_partition);

        #[cfg(feature = "master-node")]
        if let Some(removed_partition) = _removed_partition {
            self.partitions_to_expire_index
                .remove(removed_partition.expires, partition_key);
        }
        #[cfg(feature = "master-node")]
        self.partitions_to_expire_index
            .add(new_expires, partition_key);
    }

    pub fn remove(&mut self, partition_key: &String) -> Option<DbPartition> {
        let removed_partition = self.partitions.remove(partition_key);
        #[cfg(feature = "master-node")]
        if let Some(removed_partition) = &removed_partition {
            self.partitions_to_expire_index
                .remove(removed_partition.expires, partition_key);
        }

        removed_partition
    }

    pub fn clear(&mut self) -> Option<BTreeMap<String, DbPartition>> {
        if self.partitions.len() == 0 {
            return None;
        }

        let mut result = BTreeMap::new();

        std::mem::swap(&mut result, &mut self.partitions);
        #[cfg(feature = "master-node")]
        self.partitions_to_expire_index.clear();

        Some(result)
    }

    #[cfg(feature = "master-node")]
    pub fn get_partitions_to_gc_by_max_amount(
        &self,
        max_partitions_amount: usize,
    ) -> Option<Vec<&String>> {
        if self.partitions.len() <= max_partitions_amount {
            return None;
        }

        let mut partitions_to_gc = BTreeMap::new();

        for (partition_key, partition) in &self.partitions {
            let last_read_access = partition.get_last_read_moment();

            partitions_to_gc.insert(last_read_access.unix_microseconds, partition_key);
        }

        let amount_to_gc = self.partitions.len() - max_partitions_amount;

        let mut result = Vec::with_capacity(amount_to_gc);

        for (_, partition_key) in partitions_to_gc.into_iter().take(amount_to_gc) {
            result.push(partition_key);
        }

        Some(result)
    }
}
