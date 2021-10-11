use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConnectorError {
    /// Meteomatics API error.
    #[error("API error")]
    ApiError { source: reqwest::Error },

    /// HTTP response error.
    #[error("HTTP error: `{0}`, `{1}`, {2}`")]
    HttpError(String, String, reqwest::StatusCode),

    /// Library error.
    #[error("Library error: `{0}`")]
    LibraryError(String),

    /// Generic error.
    #[error(transparent)]
    GenericError(#[from] Box<dyn std::error::Error>),
}
