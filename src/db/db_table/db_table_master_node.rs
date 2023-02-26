use std::collections::BTreeMap;

use rust_extensions::date_time::DateTimeAsMicroseconds;

use super::{DataToGc, DbPartitionsContainer, DbTable, DbTableAttributes};

impl DbTable {
    pub fn new(name: String, attributes: DbTableAttributes) -> Self {
        Self {
            name,
            partitions: DbPartitionsContainer::new(),
            last_write_moment: DateTimeAsMicroseconds::now(),
            attributes,
        }
    }

    pub fn get_expiration_index_rows_amount(&self) -> usize {
        let mut result = 0;

        for db_partition in self.partitions.get_partitions() {
            result += db_partition.get_expiration_index_rows_amount();
        }

        result
    }

    pub fn get_partitions_last_write_moment(&self) -> BTreeMap<String, DateTimeAsMicroseconds> {
        let mut result = BTreeMap::new();

        for (pk, db_partition) in self.partitions.get_all() {
            result.insert(pk.to_string(), db_partition.get_last_write_moment());
        }

        result
    }

    pub fn get_data_to_gc(&self, now: DateTimeAsMicroseconds) -> DataToGc {
        let mut result = DataToGc::new();

        if let Some(max_partitions_amount) = self.attributes.max_partitions_amount {
            if let Some(partitions_to_expire) = self
                .partitions
                .get_partitions_to_gc_by_max_amount(max_partitions_amount)
            {
                for partition_key in partitions_to_expire {
                    result.add_partition_to_expire(partition_key);
                }
            }
        }

        if let Some(partitions_to_expire) = self.partitions.get_partitions_to_expire(now) {
            for partition_key in partitions_to_expire {
                result.add_partition_to_expire(partition_key);
            }
        }

        //Find DbRows to expire
        for (partition_key, db_partition) in self.partitions.get_all() {
            if result.has_partition_to_gc(partition_key) {
                continue;
            }

            if let Some(rows_to_expire) = db_partition.get_rows_to_expire(now) {
                result.add_rows_to_expire(
                    partition_key,
                    rows_to_expire.iter().map(|itm| itm.row_key.to_string()),
                );
            }

            //Find DBRows to GC by max amount
            if let Some(max_rows_per_partition) = self.attributes.max_rows_per_partition_amount {
                if let Some(rows_to_gc) = db_partition
                    .rows
                    .get_rows_to_gc_by_max_amount(max_rows_per_partition)
                {
                    result.add_rows_to_expire(
                        partition_key,
                        rows_to_gc.iter().map(|itm| itm.row_key.to_string()),
                    );
                }
            }
        }

        result
    }
}

#[cfg(feature = "master-node")]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        db::DbTable,
        db_json_entity::{DbJsonEntity, JsonTimeStamp},
    };

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

        db_table.insert_row(&db_row, None);

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

        db_table.insert_row(&db_row, None);

        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "AAA": "111"
        }"#;

        let db_json_entity = DbJsonEntity::parse(test_json.as_bytes()).unwrap();

        let db_row2 = db_json_entity.new_db_row(&now);

        let db_row2 = Arc::new(db_row2);

        db_table.insert_or_replace_row(&db_row2, None);

        assert_eq!(db_table.get_table_size(), db_row2.data.len());
        assert_eq!(db_table.get_partitions_amount(), 1);
    }
}
