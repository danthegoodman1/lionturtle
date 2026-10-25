use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
    #[error("corruption: {0}")]
    Corruption(String),
    #[error("store stopped after an earlier write failure; reopen it to recover")]
    Poisoned,
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn corruption(msg: impl Into<String>) -> Error {
    Error::Corruption(msg.into())
}
