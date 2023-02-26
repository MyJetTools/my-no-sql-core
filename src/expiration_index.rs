use std::collections::BTreeMap;

use rust_extensions::{date_time::DateTimeAsMicroseconds, lazy::LazyVec};

pub trait ExpirationItemsAreSame<T: Clone> {
    fn are_same(&self, other_one: &T) -> bool;
}

pub struct ExpirationIndex<T: Clone + ExpirationItemsAreSame<T>> {
    index: BTreeMap<i64, Vec<T>>,
    amount: usize,
}

impl<T: Clone + ExpirationItemsAreSame<T>> ExpirationIndex<T> {
    pub fn new() -> Self {
        Self {
            index: BTreeMap::new(),
            amount: 0,
        }
    }

    pub fn add(&mut self, expiration_moment: Option<DateTimeAsMicroseconds>, item: &T) {
        if expiration_moment.is_none() {
            return;
        }

        let expire_moment = expiration_moment.unwrap().unix_microseconds;

        match self.index.get_mut(&expire_moment) {
            Some(items) => {
                items.push(item.clone());
            }
            None => {
                self.index.insert(expire_moment, vec![item.clone()]);
            }
        }

        self.amount += 1;
    }

    pub fn remove(&mut self, expiration_moment: Option<DateTimeAsMicroseconds>, item: &T) {
        if expiration_moment.is_none() {
            return;
        }

        let expire_moment = expiration_moment.unwrap().unix_microseconds;

        if let Some(items) = self.index.get_mut(&expire_moment) {
            items.retain(|f| !item.are_same(f));
        }

        self.amount -= 1;
    }

    pub fn update(
        &mut self,
        old_expiration_moment: Option<DateTimeAsMicroseconds>,
        new_expiration_moment: Option<DateTimeAsMicroseconds>,
        item: &T,
    ) {
        self.remove(old_expiration_moment, item);
        self.add(new_expiration_moment, item);
    }

    pub fn get_items_to_expire(&self, now: DateTimeAsMicroseconds) -> Option<Vec<&T>> {
        let mut result = LazyVec::new();
        for (expiration_time, items) in &self.index {
            if *expiration_time > now.unix_microseconds {
                break;
            }

            for itm in items {
                result.add(itm);
            }
        }

        result.get_result()
    }

    pub fn get_items_to_expire_cloned(&self, now: DateTimeAsMicroseconds) -> Option<Vec<T>> {
        let mut result = LazyVec::new();
        for (expiration_time, items) in &self.index {
            if *expiration_time > now.unix_microseconds {
                break;
            }

            for itm in items {
                result.add(itm.clone());
            }
        }

        result.get_result()
    }

    pub fn has_data_with_expiration_moment(&self, expiration_moment: i64) -> bool {
        self.index.contains_key(&expiration_moment)
    }

    pub fn len(&self) -> usize {
        self.amount
    }

    pub fn clear(&mut self) {
        self.index.clear();
    }
}

impl ExpirationItemsAreSame<String> for String {
    fn are_same(&self, other_one: &String) -> bool {
        self == other_one
    }
}
