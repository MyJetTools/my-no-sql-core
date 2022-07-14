use std::sync::atomic::AtomicI64;

use rust_extensions::date_time::{AtomicDateTimeAsMicroseconds, DateTimeAsMicroseconds};

use crate::db_json_entity::JsonTimeStamp;

pub struct DbRow {
    pub partition_key: String,
    pub row_key: String,
    pub data: Vec<u8>,
    expires: AtomicI64,
    pub time_stamp: String,
    pub last_read_access: AtomicDateTimeAsMicroseconds,
}

impl DbRow {
    pub fn new(
        partition_key: String,
        row_key: String,
        data: Vec<u8>,
        expires: Option<DateTimeAsMicroseconds>,
        time_stamp: &JsonTimeStamp,
    ) -> Self {
        let last_read_access =
            AtomicDateTimeAsMicroseconds::new(time_stamp.date_time.unix_microseconds);

        Self {
            partition_key,
            row_key,
            data,
            expires: AtomicI64::new(expires_to_i64(expires)),
            time_stamp: time_stamp.as_str().to_string(),
            last_read_access,
        }
    }

    pub fn update_last_access(&self, now: DateTimeAsMicroseconds) {
        self.last_read_access.update(now);
    }

    pub fn get_expires(&self) -> Option<DateTimeAsMicroseconds> {
        let result = self.expires.load(std::sync::atomic::Ordering::Relaxed);

        if result == NULL_EXPIRES {
            return None;
        }

        return Some(DateTimeAsMicroseconds::new(result));
    }

    pub fn update_expires(&self, expires: Option<DateTimeAsMicroseconds>) {
        self.expires
            .store(expires_to_i64(expires), std::sync::atomic::Ordering::SeqCst);
    }
}

const NULL_EXPIRES: i64 = 0;

fn expires_to_i64(expires: Option<DateTimeAsMicroseconds>) -> i64 {
    if let Some(expires) = expires {
        if expires.unix_microseconds == NULL_EXPIRES {
            return NULL_EXPIRES + 1;
        }

        return expires.unix_microseconds;
    }

    NULL_EXPIRES
}
