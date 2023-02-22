use std::collections::{HashMap, VecDeque};

#[derive(Clone, Debug)]
pub struct UpdatePartitionsLastReadTimeEvent {
    pub table_name: String,
    pub partitions: HashMap<String, ()>,
}

pub struct UpdatePartitionsLastReadTimeQueue {
    queue: VecDeque<UpdatePartitionsLastReadTimeEvent>,
}

impl UpdatePartitionsLastReadTimeQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }

    pub fn add<'s, TPartitions: Iterator<Item = &'s String>>(
        &mut self,
        table_name: &str,
        partition_keys: TPartitions,
    ) {
        if let Some(item) = self
            .queue
            .iter_mut()
            .find(|itm| itm.table_name == table_name)
        {
            for partition_key in partition_keys {
                item.partitions.insert(partition_key.to_string(), ());
            }
            return;
        }

        let mut partitions = HashMap::new();
        for partition_key in partition_keys {
            partitions.insert(partition_key.to_string(), ());
        }

        self.queue.push_back(UpdatePartitionsLastReadTimeEvent {
            table_name: table_name.to_string(),
            partitions,
        });
    }

    pub fn return_event(&mut self, event: UpdatePartitionsLastReadTimeEvent) {
        self.queue.push_back(event);
    }

    pub fn dequeue(&mut self) -> Option<UpdatePartitionsLastReadTimeEvent> {
        self.queue.pop_front()
    }
}
