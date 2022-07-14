use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use my_json::json_writer::JsonArrayWriter;
use rust_extensions::{
    date_time::{AtomicDateTimeAsMicroseconds, DateTimeAsMicroseconds},
    MyTimer,
};
use tokio::sync::{Mutex, RwLock};

use crate::db::{
    db_snapshots::{DbPartitionSnapshot, DbTableSnapshot},
    DbRow,
};

use super::{db_table_attributes::DbTableAttributes, DbTableAttributesSnapshot, DbTableInner};

pub struct DbTable {
    pub name: String,
    pub data: RwLock<DbTableInner>,
    pub attributes: DbTableAttributes,
    last_update_time: AtomicDateTimeAsMicroseconds,

    pub common_persist_thread: AtomicBool,
    pub dedicated_thread: Mutex<Option<MyTimer>>,
}

impl DbTable {
    pub fn new(data: DbTableInner, attributes: DbTableAttributesSnapshot) -> Self {
        let created = data.created.unix_microseconds;
        DbTable {
            attributes: attributes.into(),
            name: data.name.to_string(),
            data: RwLock::new(data),
            last_update_time: AtomicDateTimeAsMicroseconds::new(created),
            common_persist_thread: AtomicBool::new(true),
            dedicated_thread: Mutex::new(None),
        }
    }

    pub fn get_last_update_time(&self) -> DateTimeAsMicroseconds {
        self.last_update_time.as_date_time()
    }

    pub fn set_last_update_time(&self, value: DateTimeAsMicroseconds) {
        self.last_update_time.update(value);
    }

    pub async fn get_table_size(&self) -> usize {
        let read_access = self.data.read().await;
        return read_access.get_calculated_metrics().data_size;
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

    pub fn persist_using_common_thread(&self) -> bool {
        self.common_persist_thread.load(Ordering::Relaxed)
    }

    pub async fn get_table_snapshot(&self) -> DbTableSnapshot {
        let last_update_time = self.get_last_update_time();
        let read_access = self.data.read().await;
        let read_access: &DbTableInner = &read_access;

        DbTableSnapshot {
            attr: self.attributes.get_snapshot(),
            created: read_access.created,
            last_update_time,
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
