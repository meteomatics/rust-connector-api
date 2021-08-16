use crate::entities::connector_error::ConnectorError;

#[derive(Clone, Debug)]
pub struct ConnectorResponse {
    body: String,
    error: ConnectorError,
}

impl ConnectorResponse {}
