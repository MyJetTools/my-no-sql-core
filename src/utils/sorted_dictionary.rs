use std::collections::BTreeMap;

pub struct SortedDictionary<TKey, TValue> {
    items: BTreeMap<TKey, TValue>,
}

impl<TKey: Ord + Clone, TValue: Clone> SortedDictionary<TKey, TValue> {
    pub fn new() -> Self {
        Self {
            items: BTreeMap::new(),
        }
    }

    pub fn first(&mut self) -> Option<(TKey, TValue)> {
        for (key, value) in &self.items {
            return Some((key.clone(), value.clone()));
        }
        return None;
    }

    pub fn remove(&mut self, key: &TKey) -> Option<TValue> {
        return self.items.remove(key);
    }

    pub fn contains_key(&self, key: &TKey) -> bool {
        return self.items.contains_key(key);
    }

    pub fn insert(&mut self, key: TKey, value: TValue) {
        self.items.insert(key, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let mut sorted_dictionary: SortedDictionary<i64, String> = SortedDictionary::new();

        sorted_dictionary.insert(5, "5".to_string());
        sorted_dictionary.insert(4, "4".to_string());
        sorted_dictionary.insert(3, "3".to_string());

        assert_eq!(3, sorted_dictionary.first().unwrap().0);

        sorted_dictionary.remove(&3);

        assert_eq!(4, sorted_dictionary.first().unwrap().0);

        sorted_dictionary.remove(&4);

        assert_eq!(5, sorted_dictionary.first().unwrap().0);
    }
}
