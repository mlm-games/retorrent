mod error;
mod peer_store;
mod protocol;
mod routing_table;
mod node;

pub use error::{Error, Result};
pub use node::{DhtBuilder, DhtNode, DhtStats};
pub use routing_table::{RoutingTable, RoutingTableNode, LeafBucket, InsertResult, NodeStatus};
pub use protocol::Message;

use std::time::Duration;

pub(crate) const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const REQUERY_INTERVAL: Duration = Duration::from_secs(60);
pub(crate) const INACTIVITY_TIMEOUT: Duration = Duration::from_secs(15 * 60);

pub const DHT_BOOTSTRAP: &[&str] = &[
    "dht.transmissionbt.com:6881",
    "dht.libtorrent.org:25401",
    "router.bittorrent.com:6881",
    "dht.aelitis.com:6881",
];
