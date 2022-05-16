use polars::prelude::*;

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectorResponse {
    pub response_body: ResponseBody,
    pub http_status_code: String,
    pub http_status_message: String,
}

pub type ResponseHeader = Vec<String>;
pub type ResponseRecord = DataFrame;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResponseBody {
    pub response_header: ResponseHeader,
    pub response_df: ResponseRecord,
}

impl ResponseBody {
    pub fn new() -> Self {
        Self {
            response_header: Default::default(),
            response_df: Default::default(),
        }
    }

    pub fn add_header(&mut self, header: String) {
        self.response_header.push(header);
    }

    pub fn add_dataframe(&mut self, df: polars::prelude::DataFrame) {
        self.response_df = df;
    }
}
