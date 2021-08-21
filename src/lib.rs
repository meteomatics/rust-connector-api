use crate::configuration::api_client::APIClient;
use reqwest::Response;

mod configuration;
mod connector_components;
mod entities;

pub use crate::connector_components::*;

#[macro_use]
extern crate derive_builder;
extern crate linked_hash_set;

#[derive(Clone, Debug)]
pub struct MeteomaticsConnector {
    api_client: APIClient,
}

impl MeteomaticsConnector {
    pub fn new(username: String, password: String, timeout_seconds: u64) -> Self {
        Self {
            api_client: APIClient::new(username, password, timeout_seconds),
        }
    }

    pub async fn query_time_series(&self, url_fragment: &str) -> Result<Response, reqwest::Error> {
        let response = self.api_client.do_http_get(url_fragment).await?;
        Ok(response)
    }
}
