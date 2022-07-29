use std::collections::BTreeMap;

use my_json::json_writer::JsonArrayWriter;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::db::DbTable;

#[cfg(feature = "row_expiration")]
use crate::db::DbTableAttributes;

use super::DbPartitionSnapshot;

pub struct DbTableSnapshot {
    #[cfg(feature = "table_attributes")]
    pub attr: DbTableAttributes,
    pub last_update_time: DateTimeAsMicroseconds,
    pub by_partition: BTreeMap<String, DbPartitionSnapshot>,
}

impl DbTableSnapshot {
    pub fn new(last_update_time: DateTimeAsMicroseconds, db_table: &DbTable) -> Self {
        let mut by_partition = BTreeMap::new();

        for (partition_key, db_partition) in &db_table.partitions {
            by_partition.insert(partition_key.to_string(), db_partition.into());
        }

        Self {
            #[cfg(feature = "table_attributes")]
            attr: db_table.attributes.clone(),
            last_update_time,
            by_partition,
        }
    }

    pub fn as_json_array(&self) -> JsonArrayWriter {
        let mut json_array_writer = JsonArrayWriter::new();

        for db_partition_snapshot in self.by_partition.values() {
            for db_row in &db_partition_snapshot.db_rows_snapshot.db_rows {
                json_array_writer.write_raw_element(&db_row.data);
            }
        }

        json_array_writer
    }
}
