use std::{collections::VecDeque, sync::Arc};

use my_json::json_writer::JsonArrayWriter;
use rust_extensions::date_time::DateTimeAsMicroseconds;
use tokio::sync::RwLock;

use crate::db::{
    db_snapshots::{DbPartitionSnapshot, DbTableSnapshot},
    DbRow,
};

use super::{db_table_attributes::DbTableAttributes, DbTableAttributesSnapshot, DbTableInner};

pub struct DbTable {
    pub name: String,
    pub data: RwLock<DbTableInner>,
    pub attributes: DbTableAttributes,
}

impl DbTable {
    pub fn new(data: DbTableInner, attributes: DbTableAttributesSnapshot) -> Self {
        DbTable {
            attributes: attributes.into(),
            name: data.name.to_string(),
            data: RwLock::new(data),
        }
    }

    pub async fn get_last_update_time(&self) -> DateTimeAsMicroseconds {
        let read_access = self.data.read().await;
        read_access.get_last_update_time()
    }

    pub async fn get_table_size(&self) -> usize {
        let read_access = self.data.read().await;
        return read_access.get_table_size();
    }

    pub async fn get_partitions_amount(&self) -> usize {
        let read_access = self.data.read().await;
        return read_access.get_partitions_amount();
    }

    pub async fn get_table_as_json_array(&self) -> JsonArrayWriter {
        let read_access = self.data.read().await;
        read_access.get_table_as_json_array()
    }

    pub async fn get_all_as_vec_dequeue(&self) -> VecDeque<Arc<DbRow>> {
        let read_access = self.data.read().await;

        let mut result = VecDeque::new();

        for db_row in read_access.get_all_rows() {
            result.push_back(db_row.clone());
        }

        result
    }

    pub async fn get_table_snapshot(&self) -> DbTableSnapshot {
        let read_access = self.data.read().await;
        let read_access: &DbTableInner = &read_access;

        DbTableSnapshot {
            attr: self.attributes.get_snapshot(),
            created: read_access.created,
            last_update_time: read_access.get_last_update_time(),
            by_partition: read_access.into(),
        }
    }

    pub async fn get_partition_snapshot(&self, partition_key: &str) -> Option<DbPartitionSnapshot> {
        let read_access = self.data.read().await;
        let db_partition = read_access.get_partition(partition_key)?;
        let result: DbPartitionSnapshot = db_partition.into();
        result.into()
    }
}

#[cfg(test)]
mod tests {
    use crate::{db::DbTableInner, db_json_entity::JsonTimeStamp};

    use super::*;

    #[tokio::test]
    async fn test_insert_record() {
        let now = DateTimeAsMicroseconds::now();
        let table_inner = DbTableInner::new("test-table".to_string(), now);

        let db_table = DbTable::new(
            table_inner,
            DbTableAttributesSnapshot {
                persist: true,
                max_partitions_amount: None,
                created: now,
            },
        );

        let mut inner = db_table.data.write().await;

        let now = JsonTimeStamp::now();

        let db_row = DbRow::new(
            "partitionKey".to_string(),
            "rowKey".to_string(),
            vec![0u8, 1u8, 2u8],
            None,
            &now,
        );

        let db_row = Arc::new(db_row);

        inner.insert_row(&db_row, &now);

        assert_eq!(inner.get_table_size(), 3);
        assert_eq!(inner.get_partitions_amount(), 1);
    }

    #[tokio::test]
    async fn test_insert_and_insert_or_replace() {
        let now = DateTimeAsMicroseconds::now();
        let table_inner = DbTableInner::new("test-table".to_string(), now);

        let db_table = DbTable::new(
            table_inner,
            DbTableAttributesSnapshot {
                persist: true,
                max_partitions_amount: None,
                created: now,
            },
        );

        let mut inner = db_table.data.write().await;

        let now = JsonTimeStamp::now();

        let db_row = DbRow::new(
            "partitionKey".to_string(),
            "rowKey".to_string(),
            vec![0u8, 1u8, 2u8],
            None,
            &now,
        );

        let db_row = Arc::new(db_row);

        inner.insert_row(&db_row, &now);

        let db_row = DbRow::new(
            "partitionKey".to_string(),
            "rowKey".to_string(),
            vec![0u8, 1u8, 2u8, 3u8],
            None,
            &now,
        );

        let db_row = Arc::new(db_row);

        inner.insert_or_replace_row(&db_row, &now);

        assert_eq!(inner.get_table_size(), 4);
        assert_eq!(inner.get_partitions_amount(), 1);
    }
}
