mod bep42;
mod error;
mod node;
mod peer_store;
mod persistence;
mod protocol;
mod routing_table;

pub use error::{Error, Result};
pub use node::{DhtBuilder, DhtNode, DhtStats};
pub use protocol::Message;
pub use routing_table::{InsertResult, LeafBucket, NodeStatus, RoutingTable, RoutingTableNode};

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
