use crate::errors::ConnectorError;
use reqwest::{Client, Response, StatusCode};
use url::{ParseError, Url};
use crate::location::Point;

const DEFAULT_API_BASE_URL: &str = "https://api.meteomatics.com";

/// This is the entry point for users of the library.
/// Please be aware that the password and username are **not** encrypted!
#[derive(Clone, Debug)]
pub struct APIClient {
    http_client: Client,
    username: String,
    password: String,
}

impl APIClient {
    /// Creates a new instance of the APIClient
    pub fn new(username: String, password: String, timeout_seconds: u64) -> Self {
        // safe to use unwrap, since we want to panic if the client builder fails.
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

    /// Use this to get a DataFrame from the API for a series of coordinates 
    /// (e.g. 47.423336, 9.377225) or postal codes (e.g. postal_CH9000).
    /// Users can specify optional parameters. These parameters are then inserted
    /// at the end of the URL (e.g.  /html?**model=mix**)
    pub async fn query_time_series(
        &self,
        start_date: &chrono::DateTime<chrono::Utc>,
        end_date: &chrono::DateTime<chrono::Utc>,
        interval: &chrono::Duration,
        parameters: &Vec<String>,
        coordinates: &Vec<Point>,
        optionals: &Option<Vec<String>>,
    ) -> Result<polars::frame::DataFrame, ConnectorError> {

        // Create the coordinates
        let coords_str = points_to_str(&coordinates).await;

        // Create the query specifications (time, location, etc.)
        let query_specs = build_query_specs(
            &start_date, &end_date, &interval, &parameters, &coords_str, &optionals
        ).await;

        // Create the complete URL
        let full_url = build_url(&query_specs).await.map_err(|_| ConnectorError::ParseError)?;

        // Get the query result
        let result = self.do_http_get(full_url).await;

        // Match the result
        match result {
            Ok(response) => match response.status() {
                StatusCode::OK => {
                    let dataframe: polars::frame::DataFrame = parse_response_to_dataframe(response)
                        .await?;
        
                    Ok(dataframe)
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
    
    /// This function handles the actual http request using the reqwest crate. 
    async fn do_http_get(&self, full_url: Url) -> Result<Response, reqwest::Error> {
        self.http_client
            .get(full_url)
            .basic_auth(&self.username, Some(String::from(&self.password)))
            .send()
            .await
    }
}

async fn parse_response_to_dataframe(
    response: Response,
) -> Result<polars::frame::DataFrame, polars::error::PolarsError> {
    // Get the response text:
    let body = response.text().await.unwrap();

    // Parse the response to a DataFrame
    let file = std::io::Cursor::new(&body);
    use polars::prelude::*; 
    let df = polars::io::csv::CsvReader::new(file)
        .infer_schema(Some(100))
        .with_delimiter(b';')
        .has_header(true)
        .with_parse_dates(false)
        .with_ignore_parser_errors(false)
        .finish();
    df 
}

async fn build_query_specs(
    start_date: &chrono::DateTime<chrono::Utc>,
    end_date: &chrono::DateTime<chrono::Utc>,
    interval: &chrono::Duration,
    parameters: &Vec<String>,
    coords_str: &str,
    optionals: &Option<Vec<String>>
) -> String {

    let query_specs = format!(
        "{}--{}:{}/{}/{}/csv",
        start_date.to_rfc3339(),
        end_date.to_rfc3339(),
        interval.to_string(),
        parameters.join(","),
        coords_str,
    );

    // Handles optional parameters 
    let query_specs = match optionals {
        None => query_specs,
        Some(_) => {
            format!(
                "{}?{}",
                query_specs,
                optionals.as_ref().unwrap().join("&")
            )
        }
    };
    
    return query_specs
}

async fn build_url(url_fragment: &str) -> Result<Url, ParseError> {
    let base_url = Url::parse(DEFAULT_API_BASE_URL).expect("Base URL is known to be valid");
    let full_url = base_url.join(url_fragment)?;
    Ok(full_url)
}


async fn points_to_str(coords: &Vec<Point>) -> String {
    coords.iter().map(|p| format!("{}", p)).collect::<Vec<String>>().join("+")
}


#[cfg(test)]
mod tests {

    use crate::APIClient;
    use dotenv::dotenv;
    use std::env;
    use chrono::prelude::*;
    use chrono::Duration;
    use crate::location::Point;

    #[tokio::test]
    // checks if the location specifier is correctly created
    async fn check_locations_string() {
        let p1: Point = Point { lat: 52.520551, lon: 13.461804};
        let p2: Point = Point { lat: -52.520551, lon: 13.461804};
        let coords: Vec<Point> = vec![p1, p2];
        let coord_str = crate::client::points_to_str(&coords).await;
        assert_eq!("52.520551,13.461804+-52.520551,13.461804", coord_str);
    }

    #[tokio::test]
    // checks if the query specs are correctly built
    async fn check_query_specs_string() {
        // seconds
        let start_date = Utc.ymd(2022, 5, 17).and_hms(12, 00, 00);
        let end_date = start_date + Duration::days(1);
        let interval = Duration::hours(1);

        let parameters: Vec<String> = vec![String::from("t_2m:C")];
        let p1: Point = Point { lat: 52.520551, lon: 13.461804};
        let coords: Vec<Point> = vec![p1];
        let coord_str = crate::client::points_to_str(&coords).await;

        let query_s = crate::client::build_query_specs(
            &start_date, &end_date, &interval, &parameters, &coord_str, &None
        ).await;
        assert_eq!(
            "2022-05-17T12:00:00+00:00--2022-05-18T12:00:00+00:00:PT3600S/t_2m:C/52.520551,13.461804/csv", 
            query_s
        );
        
        // microseconds
        let start_date = Utc.ymd(2022, 5, 17).and_hms_micro(12, 00, 00, 453_829);
        let end_date = start_date + Duration::days(1);
        let interval = Duration::hours(1);

        let query_ms = crate::client::build_query_specs(
            &start_date, &end_date, &interval, &parameters, &coord_str, &None
        ).await;
        assert_eq!(
            "2022-05-17T12:00:00.453829+00:00--2022-05-18T12:00:00.453829+00:00:PT3600S/t_2m:C/52.520551,13.461804/csv", 
            query_ms
        );

        // nanoseconds
        let start_date = Utc.ymd(2022, 5, 17).and_hms_nano(12, 00, 00, 453_829_123);
        let end_date = start_date + Duration::days(1);
        let interval = Duration::hours(1);

        let query_ns = crate::client::build_query_specs(
            &start_date, &end_date, &interval, &parameters, &coord_str, &None
        ).await;
        assert_eq!(
            "2022-05-17T12:00:00.453829123+00:00--2022-05-18T12:00:00.453829123+00:00:PT3600S/t_2m:C/52.520551,13.461804/csv", 
            query_ns
        );
    }

    #[tokio::test]
    // TODO: This test does way more than just testing the base url!
    async fn client_fires_get_request() {
        let query = crate::client::build_url(
            &String::from("2022-05-17T12:00:00.000Z/t_2m:C/51.5073219,-0.1276474/csv")
        ).await.unwrap();

        // Credentials
        dotenv().ok();
        let api_key: String = env::var("METEOMATICS_PW").unwrap();
        let api_user: String = env::var("METEOMATICS_USER").unwrap();
        let api_client = APIClient::new(
            api_user,
            api_key,
            10,
        );

        let result = api_client.do_http_get(query).await;

        match result {
            // reqwest got a HTTP response
            Ok(response) => match response.status() {
                reqwest::StatusCode::OK => {
                    let status = response.status();
                    assert_eq!(status.as_str(), "200");
                }
                // matches all other non-ok status codes. 
                // !This is not to say that reqwest raised an error!
                status => {
                    println!(">>>>>>>>>> StatusCode error: {:#?}", status.to_string());
                    assert_eq!(status.as_str(), "200"); // Assert to fail
                }
            },
            // reqwest raised an error
            Err(ref error) => {
                println!(">>>>>>>>>> error: {:#?}", error);
                assert!(result.is_ok());
            }
        }
    }
}