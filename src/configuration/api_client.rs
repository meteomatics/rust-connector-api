use crate::connector_error::ConnectorError;
use crate::connector_response::{CSVBody, ConnectorResponse};
use crate::format::Format;
use crate::locations::Locations;
use crate::optionals::Optionals;
use crate::parameters::Parameters;
use crate::valid_date_time::ValidDateTime;
use reqwest::{Client, Response, StatusCode};
use url::{ParseError, Url};

const DEFAULT_API_BASE_URL: &str = "https://api.meteomatics.com";

#[derive(Clone, Debug)]
pub struct APIClient {
    http_client: Client,
    username: String,
    password: String,
}

impl APIClient {
    pub fn new(username: String, password: String, timeout_seconds: u64) -> Self {
        let http_client = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_seconds))
            .build()
            .unwrap();
        Self {
            http_client,
            username,
            password,
        }
    }

    pub async fn query_time_series(
        &self,
        vdt: ValidDateTime,
        parameters: Parameters<'_>,
        locations: Locations<'_>,
        optionals: Option<Optionals<'_>>,
    ) -> Result<ConnectorResponse, ConnectorError> {
        let url_fragment = match optionals {
            None => {
                format!(
                    "{}--{}/{}/{}/{}",
                    vdt.start_date_time,
                    vdt.end_date_time.unwrap(),
                    parameters,
                    locations,
                    Format::CSV.to_string()
                )
            }
            Some(_) => {
                format!(
                    "{}--{}/{}/{}/{}?{}",
                    vdt.start_date_time,
                    vdt.end_date_time.unwrap(),
                    parameters,
                    locations,
                    Format::CSV.to_string(),
                    optionals.unwrap()
                )
            }
        };

        let result = self.do_http_get(&url_fragment).await;

        match result {
            Ok(response) => match response.status() {
                StatusCode::OK => {
                    let connector_response: ConnectorResponse =
                        self.create_response(response, parameters).await?;
                    Ok(connector_response)
                }
                status => Err(ConnectorError::HttpError(
                    status.to_string(),
                    response.text().await.unwrap(),
                    status,
                )),
            },
            Err(connector_error) => Err(ConnectorError::ApiError {
                source: connector_error,
            }),
        }
    }

    async fn do_http_get(&self, url_fragment: &str) -> Result<Response, reqwest::Error> {
        let full_url = build_url(url_fragment)
            .await
            .expect("URL fragment must be valid");

        println!(">>>>>>>>>> full_url: {}", full_url);

        self.http_client
            .get(full_url)
            .basic_auth(&self.username, Some(String::from(&self.password)))
            .send()
            .await
    }

    async fn create_response(
        &self,
        response: Response,
        parameters: Parameters<'_>,
    ) -> Result<ConnectorResponse, ConnectorError> {
        let status = response.status();
        println!(">>>>>>>>>> Status: {}", status);
        println!(">>>>>>>>>> Headers:\n{:#?}", response.headers());

        let body = response.text().await.unwrap();
        println!(">>>>>>>>>> Body:\n{}", body);

        let mut csv_body: CSVBody = CSVBody::new();
        for p_value in parameters.p_values {
            csv_body.add_header(p_value.to_string()).await;
        }

        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(body.as_bytes());

        csv_body
            .populate_records(&mut rdr)
            .await
            .map_err(|error| ConnectorError::LibraryError(error))?;
        print!(">>>>>>>>>> CSV body:\n{}", csv_body);

        Ok(ConnectorResponse {
            body: csv_body,
            http_status: status.to_string(),
        })
    }
}

async fn build_url(url_fragment: &str) -> Result<Url, ParseError> {
    let base_url = Url::parse(DEFAULT_API_BASE_URL).expect("Base URL is known to be valid");
    let full_url = base_url.join(url_fragment)?;
    Ok(full_url)
}

#[cfg(test)]
mod tests {

    use crate::configuration::api_client::APIClient;
    use crate::connector_components::format::Format;
    use crate::entities::connector_response::CSVBody;
    use crate::locations::{Coordinates, Locations};
    use crate::parameters::{PSet, Parameters, P};
    use crate::valid_date_time::{VDTOffset, ValidDateTime, ValidDateTimeBuilder};
    use chrono::{Duration, Local};
    use reqwest::StatusCode;
    use std::iter::FromIterator;

    #[tokio::test]
    async fn client_fires_get_request_to_base_url() {
        println!("##### client_fires_get_request_to_base_url:");

        // Change to correct username and password.
        let api_client = APIClient::new(
            "python-community".to_string(),
            "Umivipawe179".to_string(),
            10,
        );
        println!(">>>>>>>>>> api_client: {:?}", api_client);

        let now = Local::now();
        let yesterday = now.clone() - Duration::days(1);
        println!(">>>>>>>>>> yesterday (local) {:?}", yesterday);
        println!(">>>>>>>>>> now (local) {:?}", now);
        let yesterday = VDTOffset::Local(now.clone() - Duration::days(1));
        let now = VDTOffset::Local(now);
        let local_vdt: ValidDateTime = ValidDateTimeBuilder::default()
            .start_date_time(yesterday)
            .end_date_time(now)
            .build()
            .unwrap();

        // Create Parameters
        let parameters: Parameters = Parameters {
            p_values: PSet::from_iter([P {
                k: "t_2m",
                v: Some("C"),
            }]),
        };

        // Create Locations
        let locations: Locations = Locations {
            coordinates: Coordinates::from(["52.520551", "13.461804"]),
        };

        let url_fragment = &*format!(
            "{}--{}/{}/{}/{}",
            local_vdt.start_date_time,
            local_vdt.end_date_time.unwrap(),
            parameters,
            locations,
            Format::CSV.to_string()
        );
        println!(">>>>>>>>>> url_fragment: {:?}", url_fragment);

        let result = api_client.do_http_get(url_fragment).await;
        // println!("response: {:?}", response);

        match result {
            Ok(response) => match response.status() {
                StatusCode::OK => {
                    let status = response.status();
                    println!(">>>>>>>>>> Status: {}", status);
                    // println!(">>>>>>>>>> Headers:\n{:#?}", response.headers());

                    let body = response.text().await.unwrap();
                    println!(">>>>>>>>>> Body:\n{}", body);

                    let mut csv_body: CSVBody = CSVBody::new();
                    for p_value in parameters.p_values {
                        csv_body.add_header(p_value.to_string()).await;
                    }

                    let mut rdr = csv::ReaderBuilder::new()
                        .delimiter(b';')
                        .from_reader(body.as_bytes());
                    csv_body.populate_records(&mut rdr).await.unwrap();
                    print!(">>>>>>>>>> CSV body:\n{}", csv_body);

                    print!("\n>>>>>>>>>> CSV headers:\n");
                    println!("{}", csv_body.csv_headers.to_vec().join(","));

                    print!("\n>>>>>>>>>> CSV records:\n");
                    for csv_record in csv_body.csv_records {
                        println!("{}", csv_record.to_vec().join(","));
                    }

                    assert_eq!(status.as_str(), "200");
                    assert_ne!(body, "");
                }
                status => {
                    println!(">>>>>>>>>> error: {:#?}", status.to_string());
                    assert_ne!(status.as_str(), "200");
                }
            },
            _ => {
                println!(">>>>>>>>>> error: {:#?}", result);
                assert!(result.is_err())
            }
        }
    }
}
