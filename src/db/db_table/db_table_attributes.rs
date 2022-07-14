use std::sync::atomic::{AtomicBool, AtomicI32};

use rust_extensions::date_time::{AtomicDateTimeAsMicroseconds, DateTimeAsMicroseconds};

#[derive(Debug)]
pub struct DbTableAttributes {
    persist: AtomicBool,
    max_partitions_amount: AtomicI32,
    created: AtomicDateTimeAsMicroseconds,
}

impl DbTableAttributes {
    pub fn create_default() -> Self {
        Self {
            created: AtomicDateTimeAsMicroseconds::now(),
            persist: AtomicBool::new(true),
            max_partitions_amount: AtomicI32::new(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DbTableAttributesSnapshot {
    pub persist: bool,
    pub max_partitions_amount: Option<usize>,
    pub created: DateTimeAsMicroseconds,
}

impl DbTableAttributesSnapshot {
    pub fn create_default() -> Self {
        Self {
            created: DateTimeAsMicroseconds::now(),
            persist: true,
            max_partitions_amount: None,
        }
    }
}

impl DbTableAttributes {
    pub fn new(
        persist: bool,
        max_partitions_amount: Option<usize>,
        create: DateTimeAsMicroseconds,
    ) -> Self {
        Self {
            persist: AtomicBool::new(persist),
            created: AtomicDateTimeAsMicroseconds::new(create.unix_microseconds),
            max_partitions_amount: AtomicI32::new(max_partitions_into_atomic(
                max_partitions_amount,
            )),
        }
    }

    pub fn update(&self, persist_table: bool, max_partitions_amount: Option<usize>) -> bool {
        let mut result = false;

        if self.get_persist() != persist_table {
            self.persist
                .store(persist_table, std::sync::atomic::Ordering::SeqCst);
            result = true;
        }

        if self.get_max_partitions_amount() != max_partitions_amount {
            self.max_partitions_amount.store(
                max_partitions_into_atomic(max_partitions_amount),
                std::sync::atomic::Ordering::SeqCst,
            );

            result = true;
        }

        return result;
    }

    pub fn get_snapshot(&self) -> DbTableAttributesSnapshot {
        DbTableAttributesSnapshot {
            created: self.get_created(),
            max_partitions_amount: self.get_max_partitions_amount(),
            persist: self.get_persist(),
        }
    }

    pub fn get_persist(&self) -> bool {
        self.persist.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn get_max_partitions_amount(&self) -> Option<usize> {
        let result = self
            .max_partitions_amount
            .load(std::sync::atomic::Ordering::SeqCst);

        if result < 0 {
            return None;
        }

        return Some(result as usize);
    }

    pub fn get_created(&self) -> DateTimeAsMicroseconds {
        return self.created.as_date_time();
    }
}

impl Into<DbTableAttributes> for DbTableAttributesSnapshot {
    fn into(self) -> DbTableAttributes {
        DbTableAttributes::new(self.persist, self.max_partitions_amount, self.created)
    }
}

fn max_partitions_into_atomic(src: Option<usize>) -> i32 {
    if let Some(max_partitions_amount) = src {
        if max_partitions_amount == 0 {
            return -1;
        }

        return max_partitions_amount as i32;
    }
    return -1;
}
