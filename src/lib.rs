mod configuration;
mod connector_components;
mod entities;

pub use crate::connector_components::*;
pub use crate::entities::*;

use crate::configuration::api_client::APIClient;
use crate::connector_error::ConnectorError;
use crate::connector_response::ConnectorResponse;
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
    ) -> Result<ConnectorResponse, ConnectorError> {
        self.api_client
            .query_time_series(vdt, parameters, locations, optionals)
            .await
    }
}

#[cfg(test)]
mod tests {

    use crate::connector_response::CSVBody;
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
        let result = meteomatics_connector
            .query_time_series(utc_vdt, parameters, locations, Option::from(optionals))
            .await;

        match result {
            Ok(response) => {
                println!(">>>>>>>>>> CSV body:\n{}", response.body);
                assert_eq!(response.http_status, "200 OK");
                assert_ne!(
                    response.body,
                    CSVBody {
                        csv_headers: vec![],
                        csv_records: vec![]
                    }
                );
            }
            Err(connector_error) => {
                println!(">>>>>>>>>> ConnectorError: {:#?}", connector_error);
            }
        }
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
        let result = meteomatics_connector
            .query_time_series(utc_vdt, parameters, locations, None)
            .await;

        match result {
            Ok(response) => {
                println!(">>>>>>>>>> CSV body:\n{}", response.body);
                assert_eq!(response.http_status, "200 OK");
                assert_ne!(response.body.to_string(), "");
            }
            _ => {
                println!(">>>>>>>>>> error: {:#?}", result);
                assert!(result.is_err())
            }
        }
    }
}
