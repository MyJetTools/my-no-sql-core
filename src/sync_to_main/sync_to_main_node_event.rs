use std::sync::Arc;

pub enum SyncToMainNodeEvent<DataReaderTcpConnection: Send + Sync + 'static> {
    Connected(Arc<DataReaderTcpConnection>),
    Disconnected(Arc<DataReaderTcpConnection>),
    PingToDeliver,
    Delivered(i64),
}
