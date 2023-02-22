use std::collections::{HashMap, VecDeque};
#[derive(Debug, Clone)]
pub struct UpdateRowsLastReadTimeEvent {
    pub table_name: String,
    pub partition_key: String,
    pub row_keys: HashMap<String, ()>,
}

pub struct UpdateRowsLastReadTimeQueue {
    queue: VecDeque<UpdateRowsLastReadTimeEvent>,
}

impl UpdateRowsLastReadTimeQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }

    pub fn add<'s, TRowKeys: Iterator<Item = &'s String>>(
        &mut self,
        table_name: &str,
        partition_key: &str,
        row_keys: TRowKeys,
    ) {
        if let Some(item) = self
            .queue
            .iter_mut()
            .find(|itm| itm.table_name == table_name && itm.partition_key == partition_key)
        {
            for row_key in row_keys {
                item.row_keys.insert(row_key.to_string(), ());
            }
            return;
        }

        let item = UpdateRowsLastReadTimeEvent {
            table_name: table_name.to_string(),
            partition_key: partition_key.to_string(),
            row_keys: row_keys.map(|k| (k.to_string(), ())).collect(),
        };

        self.queue.push_back(item);
    }

    pub fn return_event(&mut self, event: UpdateRowsLastReadTimeEvent) {
        self.queue.push_back(event);
    }

    pub fn dequeue(&mut self) -> Option<UpdateRowsLastReadTimeEvent> {
        self.queue.pop_front()
    }
}
