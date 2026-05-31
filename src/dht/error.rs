use std::time::Duration;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error binding UDP socket: {0}")]
    Bind(std::io::Error),

    #[error("bootstrapping failed")]
    BootstrapFailed,

    #[error("{0} failed: {1}")]
    TaskFailed(&'static str, Box<Error>),

    #[error("{0} finished unexpectedly")]
    TaskQuit(&'static str),

    #[error("no successful lookups, {errors} errors")]
    NoSuccessfulLookups { errors: usize },

    #[error("dht is dead")]
    DhtDead,

    #[error("receiver is dead")]
    ReceiverDead,

    #[error("error response from node")]
    ErrorResponse,

    #[error("timeout after {0:?}")]
    ResponseTimeout(Duration),

    #[error("bad transaction id")]
    BadTransactionId,

    #[error("outstanding request not found")]
    RequestNotFound,

    #[error("error looking up {hostname}: {err}")]
    BootstrapLookup { hostname: String, err: std::io::Error },

    #[error("error sending: {0}")]
    Send(std::io::Error),

    #[error("error receiving: {0}")]
    Recv(std::io::Error),

    #[error("bencode error: {0}")]
    Bencode(String),
}

pub type Result<T> = std::result::Result<T, Error>;
