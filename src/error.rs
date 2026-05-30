use thiserror::Error;

#[derive(Error, Debug)]
pub enum TorrentError {
    #[error("Bencode parse error: {0}")]
    BencodeParse(String),

    #[error("Invalid metainfo: {0}")]
    InvalidMetaInfo(String),

    #[error("Tracker error: {0}")]
    Tracker(String),

    #[error("Peer error: {0}")]
    Peer(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Timeout")]
    Timeout,

    #[error("Magnet parse error: {0}")]
    MagnetParse(String),

    #[error("Resume data error: {0}")]
    ResumeData(String),
}

pub type Result<T> = std::result::Result<T, TorrentError>;
