#[cfg(feature = "master_node")]
use rust_extensions::date_time::AtomicDateTimeAsMicroseconds;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::db_json_entity::{DbJsonEntity, JsonKeyValuePosition, JsonTimeStamp};

pub struct DbRow {
    pub partition_key: String,
    pub row_key: String,
    pub data: Vec<u8>,
    #[cfg(feature = "master_node")]
    pub expires: Option<DateTimeAsMicroseconds>,
    #[cfg(feature = "master_node")]
    pub expires_json_position: Option<JsonKeyValuePosition>,

    pub time_stamp: String,
    #[cfg(feature = "master_node")]
    pub last_read_access: AtomicDateTimeAsMicroseconds,
}

impl DbRow {
    pub fn new(
        db_json_entity: &DbJsonEntity,
        data: Vec<u8>,
        #[cfg(feature = "master_node")] time_stamp: &JsonTimeStamp,
    ) -> Self {
        Self {
            partition_key: db_json_entity.partition_key.to_string(),
            row_key: db_json_entity.row_key.to_string(),
            data,
            time_stamp: time_stamp.as_str().to_string(),
            #[cfg(feature = "master_node")]
            expires: db_json_entity.expires,
            expires_json_position: db_json_entity.expires_value_position.clone(),
            #[cfg(feature = "master_node")]
            last_read_access: AtomicDateTimeAsMicroseconds::new(
                time_stamp.date_time.unix_microseconds,
            ),
        }
    }

    #[cfg(feature = "master_node")]
    pub fn update_last_read_access(&self, now: rust_extensions::date_time::DateTimeAsMicroseconds) {
        self.last_read_access.update(now);
    }

    #[cfg(feature = "master_node")]
    pub fn create_with_new_expiration_time(
        &self,
        expiration_time: Option<DateTimeAsMicroseconds>,
    ) -> DbRow {
        if let Some(expiration_time) = expiration_time {
            let value = expiration_time.to_rfc3339();
            let (data, exp_position) = crate::db_json_entity::compile_data_with_new_expires(
                self,
                &value[0..value.len() - 1],
            );
            DbRow {
                partition_key: self.partition_key.to_string(),
                row_key: self.row_key.to_string(),
                data,
                expires: Some(expiration_time),
                expires_json_position: Some(exp_position),
                time_stamp: self.time_stamp.to_string(),
                last_read_access: AtomicDateTimeAsMicroseconds::new(
                    self.last_read_access.get_unix_microseconds(),
                ),
            }
        } else {
            DbRow {
                partition_key: self.partition_key.to_string(),
                row_key: self.row_key.to_string(),
                data: crate::db_json_entity::remove_expiration_time(self),
                expires: None,
                expires_json_position: None,
                time_stamp: self.time_stamp.to_string(),
                last_read_access: AtomicDateTimeAsMicroseconds::new(
                    self.last_read_access.get_unix_microseconds(),
                ),
            }
        }
    }
}
