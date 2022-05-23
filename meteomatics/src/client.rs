//! # Client 
//! The ```APIClient``` provides access to different query types.
use crate::errors::ConnectorError;
use reqwest::{Client, Response, StatusCode};
use url::Url;
use crate::location::{Point, BBox};
use crate::util::*;

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

    /// Check your options.
    pub async fn query_user_features(&self) -> Result<UStatsResponse, ConnectorError>{
        let query_specs = String::from("user_stats_json");
        let full_url = build_url(&query_specs).await.map_err(|_| ConnectorError::ParseError)?;
        let result = self.do_http_get(full_url).await;
        match result {
            Ok(response) => match response.status() {
                StatusCode::OK => {
                    let user_stats = extract_user_statistics(response).await?;
                    Ok(user_stats)
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

    /// Download a ```polars``` DataFrame from the API for one or more ```Point``` locations.
    pub async fn query_time_series(
        &self,
        start_date: &chrono::DateTime<chrono::Utc>,
        end_date: &chrono::DateTime<chrono::Utc>,
        interval: &chrono::Duration,
        parameters: &[String],
        coordinates: &[Point],
        optionals: &Option<Vec<String>>,
    ) -> Result<polars::frame::DataFrame, ConnectorError> {
        // Check if there is only a single Point in the coordinates. This is important because in this
        // case the HTTP CSV response does not contain the information about the location (-.-). To 
        // produce a consistent DataFrame we need to create a lat and lon column (as does the python
        // connector).
        let needs_latlon: bool = coordinates.len() == 1;

        // Create the coordinates
        let coords_str = points_to_str(coordinates).await;

        // Create the query specifications (time, location, etc.)
        let query_specs = build_ts_query_specs(
            start_date, end_date, interval, parameters, &coords_str, optionals, &String::from("csv")
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
        parameters: &[String],
        postals: &[String],
        optionals: &Option<Vec<String>>,
    ) -> Result<polars::frame::DataFrame, ConnectorError> {
        // Check if there is only a single zipcode in the postals. This is important because in this
        // case the HTTP CSV response does not contain the information about the location (-.-). To 
        // produce a consistent DataFrame we need to create a postal_code column (as does the python
        // connector).
        let needs_latlon: bool = postals.len() == 1;

        // Create the coordinates
        let coords_str = postals.join("+");

        // Create the query specifications (time, location, etc.)
        let query_specs = build_ts_query_specs(
            start_date, end_date, interval, parameters, &coords_str, optionals, &String::from("csv")
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
            start_date, parameter, &coords_str, optionals, &String::from("csv")
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
        parameters: &[String],
        bbox: &BBox,
        optionals: &Option<Vec<String>>,
    ) -> Result<polars::frame::DataFrame, ConnectorError> {
        // Create the bounding box string according to API specification.
        let coords_str = format!("{}", bbox);

        // Parameters
        let params = parameters.join(",");

        // Create the query specifications (time, location, etc.)
        let query_specs = build_grid_query_specs(
            start_date, &params, &coords_str, optionals, &String::from("csv")
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
        parameters: &[String],
        bbox: &BBox,
        optionals: &Option<Vec<String>>
    ) -> Result<polars::frame::DataFrame, ConnectorError> {
        // Create the bounding box string according to API specification.
        let coords_str = format!("{}", bbox);

        // Create the query specifications (time, location, etc.)
        let query_specs = build_ts_query_specs(
            start_date, end_date, interval, parameters, &coords_str, optionals, &String::from("csv")
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

    /// Download a ```NetCDF``` from the API for a grid of locations bounded by a 
    /// bounding box object ```BBox``` and a single parameters and a time series.  
    pub async fn query_netcdf(&self,
        start_date: &chrono::DateTime<chrono::Utc>,
        end_date: &chrono::DateTime<chrono::Utc>,
        interval: &chrono::Duration,
        parameter: &String,
        bbox: &BBox,
        file_name: &String,
        optionals: &Option<Vec<String>>
    ) -> Result<(), ConnectorError> {

        create_path(file_name).await?;

        // Create the bounding box string according to API specification.
        let coords_str = format!("{}", bbox);

        // Create the query specifications (time, location, etc.)
        let query_specs = build_grid_ts_query_specs(
            start_date, end_date, interval, parameter, &coords_str, "netcdf", optionals
        ).await;

        // Create the complete URL
        let full_url = build_url(&query_specs).await.map_err(|_| ConnectorError::ParseError)?;

        // Get the query result
        let result = self.do_http_get(full_url).await;
        
        // Match the result
        match result {
            Ok(response) => match response.status() {
                StatusCode::OK => {
                    write_file(response, file_name).await?;
                    Ok(())
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

    /// Download a ```PNG``` from the API for a grid of locations bounded by a bounding box object 
    /// ```BBox``` and an single parameter and a single point in time.
    pub async fn query_grid_png(&self,
        date: &chrono::DateTime<chrono::Utc>,
        parameter: &String,
        bbox: &BBox,
        file_name: &String,
        optionals: &Option<Vec<String>>
    ) -> Result<(), ConnectorError> {

        create_path(file_name).await?;

        // Create the bounding box string according to API specification.
        let coords_str = format!("{}", bbox);

        // Create the query specifications (time, location, etc.)
        let query_specs = build_grid_query_specs(
            date, parameter, &coords_str, optionals, &String::from("png")
        ).await;

        // Create the complete URL
        let full_url = build_url(&query_specs).await.map_err(|_| ConnectorError::ParseError)?;

        // Get the query result
        let result = self.do_http_get(full_url).await;
        
        // Match the result
        match result {
            Ok(response) => match response.status() {
                StatusCode::OK => {
                    write_file(response, file_name).await?;
                    Ok(())
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

    /// Download a series of ```PNG``` files from the API for a grid of locations bounded by a 
    /// bounding box object ```BBox``` and a single parameter in the form of a time series.
    pub async fn query_grid_png_timeseries(&self,
        start_date: &chrono::DateTime<chrono::Utc>,
        end_date: &chrono::DateTime<chrono::Utc>,
        interval: &chrono::Duration,
        parameter: &String,
        bbox: &BBox,
        prefixpath: &String,
        optionals: &Option<Vec<String>>
    ) -> Result<(), ConnectorError> {

        // Iterate the time series
        let mut dt_cur = start_date.clone();
        let fmt = "%Y%m%d_%H%M%S";
        while dt_cur <= end_date.clone() {
            let cur_file_name = format!("{}_{}.png", prefixpath, dt_cur.format(fmt));
            self.query_grid_png(&dt_cur, parameter, bbox, &cur_file_name, optionals).await?;
            dt_cur = dt_cur + interval.clone();
        };
        Ok(())
    }
    
    /// Handles the actual HTTP request using the ```reqwest``` crate. 
    async fn do_http_get(&self, full_url: Url) -> Result<Response, ConnectorError> {
        self.http_client
            .get(full_url)
            .basic_auth(&self.username, Some(String::from(&self.password)))
            .send()
            .await
            .map_err(|_| ConnectorError::ReqwestError)
    }
}

#[cfg(test)]
mod tests {

    use crate::APIClient;
    use dotenv::dotenv;
    use std::env;
    
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
}
