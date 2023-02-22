use std::sync::Arc;

use rust_extensions::{date_time::DateTimeAsMicroseconds, events_loop::EventsLoop};
use tokio::sync::Mutex;

use super::{sync_to_main_node_event::SyncToMainNodeEvent, *};

#[derive(Debug, Clone)]
pub enum DeliverToMainNodeEvent {
    UpdatePartitionsExpiration {
        event: UpdatePartitionExpirationEvent,
        confirmation_id: i64,
    },
    UpdatePartitionsLastReadTime {
        event: UpdatePartitionsLastReadTimeEvent,
        confirmation_id: i64,
    },
    UpdateRowsExpirationTime {
        event: UpdateRowsExpirationTimeEvent,
        confirmation_id: i64,
    },
    UpdateRowsLastReadTime {
        event: UpdateRowsLastReadTimeEvent,
        confirmation_id: i64,
    },
}

impl DeliverToMainNodeEvent {
    pub fn get_confirmation_id(&self) -> i64 {
        match self {
            DeliverToMainNodeEvent::UpdatePartitionsExpiration {
                event: _,
                confirmation_id,
            } => *confirmation_id,
            DeliverToMainNodeEvent::UpdatePartitionsLastReadTime {
                event: _,
                confirmation_id,
            } => *confirmation_id,
            DeliverToMainNodeEvent::UpdateRowsExpirationTime {
                event: _,
                confirmation_id,
            } => *confirmation_id,
            DeliverToMainNodeEvent::UpdateRowsLastReadTime {
                event: _,
                confirmation_id,
            } => *confirmation_id,
        }
    }
}

pub struct SyncQueuesInner<DataReaderTcpConnection: Send + Sync + 'static> {
    pub confirmation_id: i64,
    update_partition_expiration_time_update: UpdatePartitionsExpirationTimeQueue,
    update_partitions_last_read_time_queue: UpdatePartitionsLastReadTimeQueue,

    update_rows_expiration_time_queue: UpdateRowsExpirationTimeQueue,
    update_rows_last_read_time_queue: UpdateRowsLastReadTimeQueue,
    on_delivery: Option<DeliverToMainNodeEvent>,
    connection: Option<Arc<DataReaderTcpConnection>>,
}

impl<DataReaderTcpConnection: Send + Sync + 'static> SyncQueuesInner<DataReaderTcpConnection> {
    pub fn new() -> Self {
        Self {
            confirmation_id: 0,
            update_partition_expiration_time_update: UpdatePartitionsExpirationTimeQueue::new(),
            update_rows_expiration_time_queue: UpdateRowsExpirationTimeQueue::new(),
            update_rows_last_read_time_queue: UpdateRowsLastReadTimeQueue::new(),
            update_partitions_last_read_time_queue: UpdatePartitionsLastReadTimeQueue::new(),
            on_delivery: None,
            connection: None,
        }
    }

    fn get_confirmation_id(&mut self) -> i64 {
        self.confirmation_id += 1;
        self.confirmation_id
    }

    fn confirm_delivery(&mut self, delivery_id: i64) {
        let on_delivery = self.on_delivery.take();

        match on_delivery {
            Some(event) => {
                let on_devliery_confirmation_id = event.get_confirmation_id();

                if on_devliery_confirmation_id != delivery_id {
                    println!("Somehow we are waiting confirmation for delivery with id {}, but we go confirmation id {} which is not the same  as the one we are waiting for. This is a bug.", on_devliery_confirmation_id, delivery_id);
                }
            }
            None => {
                println!(
                    "Somehow we got confirmation for delivery, but there is no delivery in progress"
                );
            }
        }
    }
}

pub struct SyncToMainNodeQueues<DataReaderTcpConnection: Send + Sync + 'static> {
    inner: Mutex<SyncQueuesInner<DataReaderTcpConnection>>,
    pub event_loop: EventsLoop<SyncToMainNodeEvent<DataReaderTcpConnection>>,
}

impl<DataReaderTcpConnection: Send + Sync + 'static> SyncToMainNodeQueues<DataReaderTcpConnection> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(SyncQueuesInner::new()),
            event_loop: EventsLoop::new("SyncToMainNodeQueues".to_string()),
        }
    }

    pub async fn update_partition_expiration_time(
        &self,
        table_name: &str,
        partition_key: &str,
        date_time: Option<DateTimeAsMicroseconds>,
    ) {
        let mut inner = self.inner.lock().await;

        inner
            .update_partition_expiration_time_update
            .add(table_name, partition_key, date_time);

        self.event_loop.send(SyncToMainNodeEvent::PingToDeliver);
    }

    pub async fn update_rows_expiration_time<'s, TRowKeys: Iterator<Item = &'s String>>(
        &self,
        table_name: &str,
        partition_key: &str,
        row_keys: TRowKeys,
        date_time: Option<DateTimeAsMicroseconds>,
    ) {
        let mut inner = self.inner.lock().await;
        inner
            .update_rows_expiration_time_queue
            .add(table_name, partition_key, row_keys, date_time);

        self.event_loop.send(SyncToMainNodeEvent::PingToDeliver);
    }

    pub async fn update_rows_last_read_time<'s, TRowKeys: Iterator<Item = &'s String>>(
        &self,
        table_name: &str,
        partition_key: &str,
        row_keys: TRowKeys,
    ) {
        let mut inner = self.inner.lock().await;
        inner
            .update_rows_last_read_time_queue
            .add(table_name, partition_key, row_keys);

        self.event_loop.send(SyncToMainNodeEvent::PingToDeliver);
    }

    pub async fn update_partitions_last_read_time<'s, TPartitions: Iterator<Item = &'s String>>(
        &self,
        table_name: &str,
        partition_keys: TPartitions,
    ) {
        let mut inner = self.inner.lock().await;
        inner
            .update_partitions_last_read_time_queue
            .add(table_name, partition_keys);

        self.event_loop.send(SyncToMainNodeEvent::PingToDeliver);
    }

    pub async fn get_next_event_to_deliver(
        &self,
        delivery_id: Option<i64>,
    ) -> Option<(Arc<DataReaderTcpConnection>, DeliverToMainNodeEvent)> {
        let mut inner = self.inner.lock().await;

        if let Some(delivery_id) = delivery_id {
            inner.confirm_delivery(delivery_id);
        }

        if inner.on_delivery.is_some() {
            return None;
        }

        if inner.connection.is_none() {
            return None;
        }

        if let Some(event) = inner.update_partition_expiration_time_update.dequeue() {
            let confirmation_id = inner.get_confirmation_id();
            let result = DeliverToMainNodeEvent::UpdatePartitionsExpiration {
                event,
                confirmation_id,
            };

            inner.on_delivery = Some(result.clone());
            return Some((inner.connection.as_ref().unwrap().clone(), result));
        }

        if let Some(event) = inner.update_partitions_last_read_time_queue.dequeue() {
            let confirmation_id = inner.get_confirmation_id();
            let result = DeliverToMainNodeEvent::UpdatePartitionsLastReadTime {
                event,
                confirmation_id,
            };

            inner.on_delivery = Some(result.clone());
            return Some((inner.connection.as_ref().unwrap().clone(), result));
        }

        if let Some(event) = inner.update_rows_expiration_time_queue.dequeue() {
            let confirmation_id = inner.get_confirmation_id();
            let result = DeliverToMainNodeEvent::UpdateRowsExpirationTime {
                event,
                confirmation_id,
            };

            inner.on_delivery = Some(result.clone());
            return Some((inner.connection.as_ref().unwrap().clone(), result));
        }

        if let Some(event) = inner.update_rows_last_read_time_queue.dequeue() {
            let confirmation_id = inner.get_confirmation_id();
            let result = DeliverToMainNodeEvent::UpdateRowsLastReadTime {
                event,
                confirmation_id,
            };

            inner.on_delivery = Some(result.clone());
            return Some((inner.connection.as_ref().unwrap().clone(), result));
        }

        None
    }

    pub async fn new_connection(&self, connection: Arc<DataReaderTcpConnection>) {
        let mut inner = self.inner.lock().await;
        inner.connection = Some(connection);
    }

    pub async fn disconnected(&self) {
        let mut inner = self.inner.lock().await;
        inner.connection = None;

        let event_on_delivery = inner.on_delivery.take();

        if event_on_delivery.is_none() {
            return;
        }

        let event_on_delivery = event_on_delivery.unwrap();

        match event_on_delivery {
            DeliverToMainNodeEvent::UpdatePartitionsExpiration {
                event,
                confirmation_id: _,
            } => {
                inner
                    .update_partition_expiration_time_update
                    .return_event(event);
            }
            DeliverToMainNodeEvent::UpdatePartitionsLastReadTime {
                event,
                confirmation_id: _,
            } => {
                inner
                    .update_partitions_last_read_time_queue
                    .return_event(event);
            }
            DeliverToMainNodeEvent::UpdateRowsExpirationTime {
                event,
                confirmation_id: _,
            } => {
                inner.update_rows_expiration_time_queue.return_event(event);
            }
            DeliverToMainNodeEvent::UpdateRowsLastReadTime {
                event,
                confirmation_id: _,
            } => {
                inner.update_rows_last_read_time_queue.return_event(event);
            }
        }
    }
}
