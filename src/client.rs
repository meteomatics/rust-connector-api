use crate::errors::ConnectorError;
use reqwest::{Client, Response, StatusCode};
use url::{ParseError, Url};
use crate::location::{Point, BBox};

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

    /// Download a ```polars``` DataFrame from the API for one or more ```Point``` locations.
    pub async fn query_time_series(
        &self,
        start_date: &chrono::DateTime<chrono::Utc>,
        end_date: &chrono::DateTime<chrono::Utc>,
        interval: &chrono::Duration,
        parameters: &Vec<String>,
        coordinates: &Vec<Point>,
        optionals: &Option<Vec<String>>,
    ) -> Result<polars::frame::DataFrame, ConnectorError> {
        // Check if there is only a single Point in the coordinates. This is important because in this
        // case the HTTP CSV response does not contain the information about the location (-.-). To 
        // produce a consistent DataFrame we need to create a lat and lon column (as does the python
        // connector).
        let needs_latlon: bool = if coordinates.len() == 1 { true } else { false };

        // Create the coordinates
        let coords_str = points_to_str(&coordinates).await;

        // Create the query specifications (time, location, etc.)
        let query_specs = build_ts_query_specs(
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
                    if needs_latlon {
                        let df = parse_response_to_df(
                            response).await?;
                        let df = df_add_latlon(df, coordinates.get(0).unwrap()).await?;
                        Ok(df)
                    } else {
                        let df = parse_response_to_df(
                            response).await?;
                        Ok(df)
                    }
                }
                status => Err(ConnectorError::HttpError(
                    status.to_string(),
                    response.text().await.unwrap(),
                    status,
                )),
            },
            Err(_) => Err(ConnectorError::ReqwestError),
        }
    }

    /// Download a ```polars``` DataFrame from the API for one or more postal code location identifiers
    /// (e.g. postal_CH8000, postal_CH9000).
    pub async fn query_time_series_postal(&self,
        start_date: &chrono::DateTime<chrono::Utc>,
        end_date: &chrono::DateTime<chrono::Utc>,
        interval: &chrono::Duration,
        parameters: &Vec<String>,
        postals: &Vec<String>,
        optionals: &Option<Vec<String>>,
    ) -> Result<polars::frame::DataFrame, ConnectorError> {
        // Check if there is only a single zipcode in the postals. This is important because in this
        // case the HTTP CSV response does not contain the information about the location (-.-). To 
        // produce a consistent DataFrame we need to create a postal_code column (as does the python
        // connector).
        let needs_latlon: bool = if postals.len() == 1 { true } else { false };

        // Create the coordinates
        let coords_str = postals.join("+");

        // Create the query specifications (time, location, etc.)
        let query_specs = build_ts_query_specs(
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
                    if needs_latlon {
                        let df = parse_response_to_df(
                            response).await?;
                        let df = df_add_postal(df, postals.get(0).unwrap()).await?;
                        Ok(df)
                    } else {
                        let df = parse_response_to_df(
                            response).await?;
                        Ok(df)
                    }
                }
                status => Err(ConnectorError::HttpError(
                    status.to_string(),
                    response.text().await.unwrap(),
                    status,
                )),
            },
            Err(_) => Err(ConnectorError::ReqwestError),
        }
    }

    /// Download a ```polars``` DataFrame from the API for a grid of locations bounded by a 
    /// bounding box object ```BBox``` and a single parameter. 
    pub async fn query_grid_pivoted(&self,
        start_date: &chrono::DateTime<chrono::Utc>,
        parameter: &String,
        bbox: &BBox,
        optionals: &Option<Vec<String>>,
    ) -> Result<polars::frame::DataFrame, ConnectorError> {
        // Create the bounding box string according to API specification.
        let coords_str = format!("{}", bbox);

        // Create the query specifications (time, location, etc.)
        let query_specs = build_grid_query_specs(
            &start_date, parameter, &coords_str, &optionals
        ).await;

        // Create the complete URL
        let full_url = build_url(&query_specs).await.map_err(|_| ConnectorError::ParseError)?;

        // Get the query result
        let result = self.do_http_get(full_url).await;

        // Match the result
        match result {
            Ok(response) => match response.status() {
                StatusCode::OK => {
                    let df = parse_grid_response_to_df(
                        response).await?;
                    Ok(df)
                }
                status => Err(ConnectorError::HttpError(
                    status.to_string(),
                    response.text().await.unwrap(),
                    status,
                )),
            },
            Err(_) => Err(ConnectorError::ReqwestError),
        }
    }

    /// Download a ```polars``` DataFrame from the API for a grid of locations bounded by a 
    /// bounding box object ```BBox``` and an arbitray number of parameters and a unique point in time. 
    pub async fn query_grid_unpivoted(&self,
        start_date: &chrono::DateTime<chrono::Utc>,
        parameters: &Vec<String>,
        bbox: &BBox,
        optionals: &Option<Vec<String>>,
    ) -> Result<polars::frame::DataFrame, ConnectorError> {
        // Create the bounding box string according to API specification.
        let coords_str = format!("{}", bbox);

        // Parameters
        let params = parameters.join(",");

        // Create the query specifications (time, location, etc.)
        let query_specs = build_grid_query_specs(
            &start_date, &params, &coords_str, &optionals
        ).await;

        // Create the complete URL
        let full_url = build_url(&query_specs).await.map_err(|_| ConnectorError::ParseError)?;

        // Get the query result
        let result = self.do_http_get(full_url).await;

        // Match the result
        match result {
            Ok(response) => match response.status() {
                StatusCode::OK => {
                    let df = parse_response_to_df(
                        response).await?;
                    Ok(df)
                }
                status => Err(ConnectorError::HttpError(
                    status.to_string(),
                    response.text().await.unwrap(),
                    status,
                )),
            },
            Err(_) => Err(ConnectorError::ReqwestError),
        }
    }

    /// Download a ```polars``` DataFrame from the API for a grid of locations bounded by a 
    /// bounding box object ```BBox``` and an arbitray number of parameters and a time series.  
    pub async fn query_grid_unpivoted_time_series(&self,
        start_date: &chrono::DateTime<chrono::Utc>,
        end_date: &chrono::DateTime<chrono::Utc>,
        interval: &chrono::Duration,
        parameters: &Vec<String>,
        bbox: &BBox,
        optionals: &Option<Vec<String>>
    ) -> Result<polars::frame::DataFrame, ConnectorError> {
        // Create the bounding box string according to API specification.
        let coords_str = format!("{}", bbox);

        // Create the query specifications (time, location, etc.)
        let query_specs = build_ts_query_specs(
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
                    let df = parse_response_to_df(
                        response).await?;
                    Ok(df)
                }
                status => Err(ConnectorError::HttpError(
                    status.to_string(),
                    response.text().await.unwrap(),
                    status,
                )),
            },
            Err(_) => Err(ConnectorError::ReqwestError),
        }
    }

    
    /// Handles the actual HTTP request using the ```reqwest``` crate. 
    async fn do_http_get(&self, full_url: Url) -> Result<Response, reqwest::Error> {
        self.http_client
            .get(full_url)
            .basic_auth(&self.username, Some(String::from(&self.password)))
            .send()
            .await
    }
}

/// Creates a new DataFrame latitude and longitude to the DataFrame created from the HTTP response.
async fn df_add_latlon(df_in: polars::frame::DataFrame, point: &Point) -> 
    Result<polars::frame::DataFrame, polars::error::PolarsError> {
    use polars::prelude::*;
    // https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html#method.shape
    // Get (height, width) of the DataFrame. Get width:
    let n = df_in.height();
    let mut lat: Vec<f64> = Vec::new();
    let mut lon: Vec<f64> = Vec::new();
    for _ in 0..n {
        lat.push(point.lat);
        lon.push(point.lon);
    }
    // https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html#method.extend
    // https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html#method.get_column_names
    let df_tmp = df!("lat" => &lat, "lon" => &lon)?;
    let df_out: DataFrame = df_tmp.hstack(df_in.get_columns())?;
    Ok(df_out)
}

/// Creates a new DataFrame latitude and longitude to the DataFrame created from the HTTP response.
async fn df_add_postal(df_in: polars::frame::DataFrame, postal: &String) -> 
    Result<polars::frame::DataFrame, polars::error::PolarsError> {
    use polars::prelude::*;
    // https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html#method.shape
    // Get (height, width) of the DataFrame. Get width:
    let n = df_in.height();
    let mut col_zipcode: Vec<String> = Vec::new();
    for _ in 0..n {
        col_zipcode.push(postal.clone());
    }
    // https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html#method.extend
    // https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html#method.get_column_names
    let df_tmp = df!("station_id" => &col_zipcode)?;
    let df_out: DataFrame = df_tmp.hstack(df_in.get_columns())?;
    Ok(df_out)
}

/// Convert the HTTP response body text for time series (i.e. the downloaded CSV) into a ```polars``` DataFrame. 
/// Consumes the HTTP response.
async fn parse_response_to_df(
    response: Response,
) -> Result<polars::frame::DataFrame, polars::error::PolarsError> {
    // Get the response text:
    let body = response.text().await.unwrap();

    // Parse the response to a DataFrame
    let file = std::io::Cursor::new(&body);
    use polars::prelude::*; 
    let df1 = polars::io::csv::CsvReader::new(file)
        .infer_schema(Some(100))
        .with_delimiter(b';')
        .has_header(true)
        .with_parse_dates(false)
        .with_ignore_parser_errors(false)
        .finish()?;

    Ok(df1)
}

async fn parse_grid_response_to_df(
    response: Response,
) -> Result<polars::frame::DataFrame, polars::error::PolarsError> {
        // Get the response text:
        let body = response.text().await.unwrap();

        // Parse the response to a DataFrame
        let file = std::io::Cursor::new(&body);
        use polars::prelude::*; 
        let df1 = polars::io::csv::CsvReader::new(file)
            .infer_schema(Some(100))
            .with_delimiter(b';')
            .has_header(true)
            .with_skip_rows(2)
            .with_parse_dates(false)
            .with_ignore_parser_errors(false)
            .finish()?;
    
        Ok(df1)
}

/// Build the part of the query (in case of time series requests) that contains information about 
/// the request time, location, parameters and optional specifications. This is then combined with 
/// the base API URL.
async fn build_ts_query_specs(
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

/// Build the part of the query (in case of grid data requests) that contains information about 
/// the request time, location, parameters and optional specifications. This is then combined with 
/// the base API URL.
async fn build_grid_query_specs(
    start_date: &chrono::DateTime<chrono::Utc>,
    parameter: &String,
    coords_str: &str,
    optionals: &Option<Vec<String>>
) -> String {
    let query_specs = format!(
        "{}/{}/{}/csv",
        start_date.to_rfc3339(),
        parameter.to_string(),
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


/// Combines the default base API URL with the query specific information.
async fn build_url(url_fragment: &str) -> Result<Url, ParseError> {
    let base_url = Url::parse(DEFAULT_API_BASE_URL).expect("Base URL is known to be valid");
    let full_url = base_url.join(url_fragment)?;
    Ok(full_url)
}

/// Convert a number of Points to a String according to the Meteomatics API specifications.
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
    use crate::location::{Point, BBox};

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
    async fn check_ts_query_specs_string() {
        // seconds
        let start_date = Utc.ymd(2022, 5, 17).and_hms(12, 00, 00);
        let end_date = start_date + Duration::days(1);
        let interval = Duration::hours(1);

        let parameters: Vec<String> = vec![String::from("t_2m:C")];
        let p1: Point = Point { lat: 52.520551, lon: 13.461804};
        let coords: Vec<Point> = vec![p1];
        let coord_str = crate::client::points_to_str(&coords).await;

        let query_s = crate::client::build_ts_query_specs(
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

        let query_ms = crate::client::build_ts_query_specs(
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

        let query_ns = crate::client::build_ts_query_specs(
            &start_date, &end_date, &interval, &parameters, &coord_str, &None
        ).await;
        assert_eq!(
            "2022-05-17T12:00:00.453829123+00:00--2022-05-18T12:00:00.453829123+00:00:PT3600S/t_2m:C/52.520551,13.461804/csv", 
            query_ns
        );
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn check_grid_string() {
        let bbox: BBox = BBox {
            lat_min: -90.0, 
            lat_max: 90.0,
            lon_min: -180.0,
            lon_max: 180.0,
            lat_res: 5.0,
            lon_res: 5.0,
        };
        let coord_str = format!("{}", bbox);
        assert_eq!("90,-180_-90,180:5,5", coord_str);
    }
}
