use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConnectorError {
    /// ReqwestError.
    #[error("ReqwestError error: {0}")]
    ReqwestError(String),

    /// HTTP response error.
    #[error("HTTP error: `{0}`, `{1}`, {2}`")]
    HttpError(String, String, reqwest::StatusCode),

    /// Library error.
    #[error("Library error: `{0}`")]
    LibraryError(String),

    /// Polars error.
    #[error("Polars error: `{0}`")]
    PolarsError(String),

    /// Generic error.
    #[error(transparent)]
    GenericError(#[from] Box<dyn std::error::Error + Send>),

    /// Parse error.
    #[error("Parsing error")]
    ParseError,

    /// File i/o error
    #[error("File i/o error")]
    FileIOError
}


impl From<url::ParseError> for ConnectorError {
    fn from(_: url::ParseError) -> Self {
        ConnectorError::ParseError
    }
}

impl From<std::io::Error> for ConnectorError {
    fn from(_: std::io::Error) -> Self {
        ConnectorError::FileIOError
    }
}
