use crate::db_json_entity::JsonTimeStamp;
#[cfg(feature = "master_node")]
use rust_extensions::date_time::AtomicDateTimeAsMicroseconds;

pub struct DbRow {
    pub partition_key: String,
    pub row_key: String,
    pub data: Vec<u8>,
    #[cfg(feature = "master_node")]
    expires: std::sync::atomic::AtomicI64,

    pub time_stamp: String,
    #[cfg(feature = "master_node")]
    pub last_read_access: AtomicDateTimeAsMicroseconds,
}

impl DbRow {
    pub fn new(
        partition_key: String,
        row_key: String,
        data: Vec<u8>,
        #[cfg(feature = "master_node")] expires: Option<
            rust_extensions::date_time::DateTimeAsMicroseconds,
        >,

        time_stamp: &JsonTimeStamp,
    ) -> Self {
        #[cfg(feature = "master_node")]
        let last_read_access =
            AtomicDateTimeAsMicroseconds::new(time_stamp.date_time.unix_microseconds);

        Self {
            partition_key,
            row_key,
            data,
            #[cfg(feature = "master_node")]
            expires: std::sync::atomic::AtomicI64::new(expires_to_i64(expires)),
            time_stamp: time_stamp.as_str().to_string(),
            #[cfg(feature = "master_node")]
            last_read_access,
        }
    }

    pub fn restore(
        partition_key: String,
        row_key: String,
        data: Vec<u8>,
        #[cfg(feature = "master_node")] expires: Option<
            rust_extensions::date_time::DateTimeAsMicroseconds,
        >,

        time_stamp: String,
    ) -> Self {
        #[cfg(feature = "master_node")]
        let last_read_access = AtomicDateTimeAsMicroseconds::now();

        Self {
            partition_key,
            row_key,
            data,
            #[cfg(feature = "master_node")]
            expires: std::sync::atomic::AtomicI64::new(expires_to_i64(expires)),
            time_stamp,
            #[cfg(feature = "master_node")]
            last_read_access,
        }
    }

    #[cfg(feature = "master_node")]
    pub fn update_last_access(&self, now: rust_extensions::date_time::DateTimeAsMicroseconds) {
        self.last_read_access.update(now);
    }

    #[cfg(feature = "master_node")]
    pub fn get_expires(&self) -> Option<rust_extensions::date_time::DateTimeAsMicroseconds> {
        let result = self.expires.load(std::sync::atomic::Ordering::Relaxed);

        if result == NULL_EXPIRES {
            return None;
        }

        return Some(rust_extensions::date_time::DateTimeAsMicroseconds::new(
            result,
        ));
    }

    #[cfg(feature = "master_node")]
    pub fn update_expires(
        &self,
        expires: Option<rust_extensions::date_time::DateTimeAsMicroseconds>,
    ) {
        self.expires
            .store(expires_to_i64(expires), std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(feature = "master_node")]
const NULL_EXPIRES: i64 = 0;

#[cfg(feature = "master_node")]
fn expires_to_i64(expires: Option<rust_extensions::date_time::DateTimeAsMicroseconds>) -> i64 {
    if let Some(expires) = expires {
        if expires.unix_microseconds == NULL_EXPIRES {
            return NULL_EXPIRES + 1;
        }

        return expires.unix_microseconds;
    }

    NULL_EXPIRES
}
