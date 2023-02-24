impl DbTable {
    pub fn get_all_rows_and_update_expiration_time<'s>(
        &'s mut self,
        update_expiration_time: &UpdateExpirationTimeModel,
    ) -> Vec<Arc<DbRow>> {
        let mut result = Vec::new();
        for db_partition in self.partitions.values_mut() {
            result.extend(
                db_partition.get_all_rows_and_update_expiration_time(update_expiration_time),
            );
        }
        result
    }

    pub fn get_table_snapshot(&self) -> DbTableSnapshot {
        DbTableSnapshot {
            #[cfg(feature = "master-node")]
            attr: self.attributes.clone(),
            last_update_time: self.get_last_update_time(),
            by_partition: self.get_partitions_snapshot(),
        }
    }

    pub fn get_partitions_snapshot(&self) -> BTreeMap<String, DbPartitionSnapshot> {
        let mut result = BTreeMap::new();

        for (partition_key, db_partition) in &self.partitions {
            result.insert(partition_key.to_string(), db_partition.into());
        }

        result
    }
}
