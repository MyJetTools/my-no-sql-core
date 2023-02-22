use std::collections::{HashMap, VecDeque};

use rust_extensions::date_time::DateTimeAsMicroseconds;

#[derive(Debug, Clone)]
pub struct UpdatePartitionExpirationEvent {
    pub table_name: String,
    pub partitions: HashMap<String, Option<DateTimeAsMicroseconds>>,
}

pub struct UpdatePartitionsExpirationTimeQueue {
    queue: VecDeque<UpdatePartitionExpirationEvent>,
}

impl UpdatePartitionsExpirationTimeQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }

    pub fn add(
        &mut self,
        table_name: &str,
        partition_key: &str,
        date_time: Option<DateTimeAsMicroseconds>,
    ) {
        if let Some(item) = self
            .queue
            .iter_mut()
            .find(|itm| itm.table_name == table_name)
        {
            item.partitions.insert(partition_key.to_string(), date_time);
            return;
        }

        let mut partitions = HashMap::new();
        partitions.insert(partition_key.to_string(), date_time);

        self.queue.push_back(UpdatePartitionExpirationEvent {
            table_name: table_name.to_string(),
            partitions,
        });
    }

    pub fn return_event(&mut self, event: UpdatePartitionExpirationEvent) {
        self.queue.push_back(event);
    }

    pub fn dequeue(&mut self) -> Option<UpdatePartitionExpirationEvent> {
        self.queue.pop_front()
    }
}
