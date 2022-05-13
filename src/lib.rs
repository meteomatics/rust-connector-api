mod configuration;
mod connector_components;
mod entities;

pub use connector_components::*;
pub use crate::entities::*;

use crate::configuration::api_client::APIClient;
use crate::connector_error::ConnectorError;
use crate::connector_response::ConnectorResponse;
use crate::valid_date_time::ValidDateTime;

#[macro_use]
extern crate derive_builder;

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

    pub async fn query_time_series(
        &self,
        vdt: ValidDateTime,
        parameters: Vec<String>,
        coordinates: Vec<Vec<f32>>,
        optionals: Option<Vec<String>>,
    ) -> Result<ConnectorResponse, ConnectorError> {
        self.api_client
            .query_time_series(vdt, parameters, coordinates, optionals)
            .await
    }
}

// Unit testing section
#[cfg(test)]
mod tests {

    use crate::connector_response::ResponseBody;
    use crate::valid_date_time::{
        PeriodDate, PeriodTime, VDTOffset, ValidDateTime, ValidDateTimeBuilder,
    };
    use crate::MeteomaticsConnector;
    use chrono::{Duration, Utc};
    use dotenv::dotenv;
    use std::env;


    #[tokio::test]
    async fn call_query_time_series_with_options() {
        println!("\n##### call_query_time_series_with_options:");

        // Credentials
        dotenv().ok();
        let api_key: String = env::var("METEOMATICS_PW").unwrap();
        let api_user: String = env::var("METEOMATICS_USER").unwrap();

        // Create API connector
        let meteomatics_connector = MeteomaticsConnector::new(
            api_user,
            api_key,
            10,
        );

        // Create ValidDateTime
        let now = Utc::now();
        let yesterday = VDTOffset::Utc(now.clone() - Duration::days(1));
        let now = VDTOffset::Utc(now);
        let time_step = PeriodTime::Hours(1);
        let utc_vdt: ValidDateTime = ValidDateTimeBuilder::default()
            .start_date_time(yesterday)
            .end_date_time(now)
            .time_step(time_step)
            .build()
            .unwrap();

        // Create Parameters
        let mut parameters = Vec::new();
        parameters.push(String::from("t_2m:C"));
        parameters.push(String::from("precip_1h:mm"));

        // Create Locations
        let coordinates: Vec<Vec<f32>> = vec![vec![46.685, 7.953], vec![46.759, -76.027]];

        // Create Optionals
        let mut optionals = Vec::new();
        optionals.push(String::from("source=mix"));
        optionals.push(String::from("calibrated=true"));

        // Call endpoint
        let result = meteomatics_connector
            .query_time_series(
                utc_vdt, parameters, coordinates, Option::from(optionals)
            )
            .await;

        match result {
            Ok(ref response) => {
                let response_body = &response.response_body;
                println!("\n>>>>>>>>>> ResponseBody:\n{:?}", response_body);
                println!(
                    ">>>>>>>>>> ResponseHeaders:\n{}\n",
                    response_body.response_header.to_vec().join(",")
                );
                println!(">>>>>>>>>> ResponseRecords: NEW");
                println!("{:?}", response_body.response_df);
                assert_eq!(response.http_status_code, "200");
                assert_eq!(response.http_status_message, "200 OK");
                assert_ne!(
                    response.response_body,
                    ResponseBody {
                        response_header: vec![],
                        response_df: polars::prelude::DataFrame::default(),
                    }
                );
            }
            Err(ref connector_error) => {
                println!(">>>>>>>>>> ConnectorError: {:#?}", connector_error);
                assert!(result.is_err());
            }
        }

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn call_query_time_series_without_options() {
        println!("\n##### call_query_time_series_without_options:");

        // Credentials
        dotenv().ok();
        let api_key: String = env::var("METEOMATICS_PW").unwrap();
        let api_user: String = env::var("METEOMATICS_USER").unwrap();
        
        // Create API connector
        let meteomatics_connector = MeteomaticsConnector::new(
            api_user,
            api_key,
            10,
        );

        // Create ValidDateTime
        let now = Utc::now();
        let yesterday = VDTOffset::Utc(now.clone() - Duration::days(1));
        let now = VDTOffset::Utc(now);
        let period_date = PeriodDate::Days(1);
        let utc_vdt: ValidDateTime = ValidDateTimeBuilder::default()
            .start_date_time(yesterday)
            .end_date_time(now)
            .period_date(period_date)
            .build()
            .unwrap();

        // Create Parameters
        let mut parameters = Vec::new();
        parameters.push(String::from("t_2m:C"));
        parameters.push(String::from("precip_1h:mm"));

        // Create Locations
        let coords: Vec<Vec<f32>> = vec![vec![47.419708, 9.358478]];

        // Call endpoint
        let result = meteomatics_connector
            .query_time_series(utc_vdt, parameters, coords, None)
            .await;

        match result {
            Ok(ref response) => {
                println!(">>>>>>>>>> ResponseBody:\n{:?}", response.response_body);
                assert_eq!(response.http_status_code, "200");
                // assert_ne!(response.response_body.to_string(), "");
            }
            Err(ref connector_error) => {
                println!(">>>>>>>>>> ConnectorError: {:#?}", connector_error);
                assert!(result.is_err());
            }
        }

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn use_period_date_and_time_step_simultaneously() {
        println!("\n##### use_period_date_and_time_step_simultaneously:");

        // Credentials
        dotenv().ok();
        let api_key: String = env::var("METEOMATICS_PW").unwrap();
        let api_user: String = env::var("METEOMATICS_USER").unwrap();
        
        // Create API connector
        let meteomatics_connector = MeteomaticsConnector::new(
            api_user,
            api_key,
            10,
        );

        // Create ValidDateTime
        let now = Utc::now();
        let yesterday = VDTOffset::Utc(now.clone() - Duration::days(1));
        let now = VDTOffset::Utc(now);
        let period_date = PeriodDate::Days(1);
        let time_step = PeriodTime::Hours(1);
        let utc_vdt: ValidDateTime = ValidDateTimeBuilder::default()
            .start_date_time(yesterday)
            .end_date_time(now)
            .period_date(period_date)
            .time_step(time_step)
            .build()
            .unwrap();

        // Create Parameters
        let mut parameters = Vec::new();
        parameters.push(String::from("t_2m:C"));
        parameters.push(String::from("precip_1h:mm"));

        // Create Locations
        let coords: Vec<Vec<f32>> = vec![vec![47.419708, 9.358478]];

        // Call endpoint
        let result = meteomatics_connector
            .query_time_series(utc_vdt, parameters, coords, None)
            .await;

        match result {
            Ok(_) => {}
            Err(ref connector_error) => {
                println!(">>>>>>>>>> ConnectorError: {:#?}", connector_error);
                assert!(result.is_err());
            }
        }

        assert!(!result.is_ok());
    }
}
