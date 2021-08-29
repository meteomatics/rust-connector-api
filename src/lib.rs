use crate::configuration::api_client::APIClient;
use reqwest::Response;

mod configuration;
mod connector_components;
mod entities;

pub use crate::connector_components::*;

use crate::locations::Locations;
use crate::optionals::Optionals;
use crate::parameters::Parameters;
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
        parameters: Parameters<'_>,
        locations: Locations<'_>,
        optionals: Option<Optionals<'_>>,
    ) -> Result<Response, reqwest::Error> {
        let response = self
            .api_client
            .query_time_series(vdt, parameters, locations, optionals)
            .await?;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {

    use crate::locations::{Coordinates, Locations};
    use crate::optionals::{Opt, OptSet, Optionals};
    use crate::parameters::{PSet, Parameters, P};
    use crate::valid_date_time::{VDTOffset, ValidDateTime, ValidDateTimeBuilder};
    use crate::MeteomaticsConnector;
    use chrono::{Duration, Utc};
    use std::iter::FromIterator;

    #[tokio::test]
    async fn call_query_time_series_with_options() {
        println!("##### call_query_time_series_with_options:");

        // Create API connector
        let meteomatics_connector = MeteomaticsConnector::new(
            "python-community".to_string(),
            "Umivipawe179".to_string(),
            10,
        );

        // Create ValidDateTime
        let now = Utc::now();
        let yesterday = VDTOffset::Utc(now.clone() - Duration::days(1));
        let now = VDTOffset::Utc(now);
        let utc_vdt: ValidDateTime = ValidDateTimeBuilder::default()
            .start_date_time(yesterday)
            .end_date_time(now)
            .build()
            .unwrap();

        // Create Parameters
        let parameters: Parameters = Parameters {
            p_values: PSet::from_iter([
                P {
                    k: "t_2m",
                    v: Some("C"),
                },
                P {
                    k: "precip_1h",
                    v: Some("mm"),
                },
            ]),
        };

        // Create Locations
        let locations: Locations = Locations {
            coordinates: Coordinates::from(["47.419708", "9.358478"]),
        };

        // Create Optionals
        let optionals: Optionals = Optionals {
            opt_values: OptSet::from_iter([
                Opt {
                    k: "source",
                    v: "mix",
                },
                Opt {
                    k: "calibrated",
                    v: "true",
                },
            ]),
        };

        // Call endpoint
        let response = meteomatics_connector
            .query_time_series(utc_vdt, parameters, locations, Option::from(optionals))
            .await
            .unwrap();

        let status = format!("{}", response.status());
        println!(">>>>>>>>>> Status: {}", status);
        println!(">>>>>>>>>> Headers:\n{:#?}", response.headers());

        let body = response.text().await.unwrap();
        println!(">>>>>>>>>> Body:\n{}", body);

        assert_eq!(status, "200 OK");
        assert_ne!(body, "");
    }

    #[tokio::test]
    async fn call_query_time_series_without_options() {
        println!("##### call_query_time_series_without_options:");

        // Create API connector
        let meteomatics_connector = MeteomaticsConnector::new(
            "python-community".to_string(),
            "Umivipawe179".to_string(),
            10,
        );

        // Create ValidDateTime
        let now = Utc::now();
        let yesterday = VDTOffset::Utc(now.clone() - Duration::days(1));
        let now = VDTOffset::Utc(now);
        let utc_vdt: ValidDateTime = ValidDateTimeBuilder::default()
            .start_date_time(yesterday)
            .end_date_time(now)
            .build()
            .unwrap();

        // Create Parameters
        let parameters: Parameters = Parameters {
            p_values: PSet::from_iter([
                P {
                    k: "t_2m",
                    v: Some("C"),
                },
                P {
                    k: "precip_1h",
                    v: Some("mm"),
                },
            ]),
        };

        // Create Locations
        let locations: Locations = Locations {
            coordinates: Coordinates::from(["47.419708", "9.358478"]),
        };

        // Call endpoint
        let response = meteomatics_connector
            .query_time_series(utc_vdt, parameters, locations, None)
            .await
            .unwrap();

        let status = format!("{}", response.status());
        println!(">>>>>>>>>> Status: {}", status);
        println!(">>>>>>>>>> Headers:\n{:#?}", response.headers());

        let body = response.text().await.unwrap();
        println!(">>>>>>>>>> Body:\n{}", body);

        assert_eq!(status, "200 OK");
        assert_ne!(body, "");
    }
}
