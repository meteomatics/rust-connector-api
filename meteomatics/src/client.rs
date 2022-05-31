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
    /// 
    /// # Examples
    ///
    /// ```rust, no_run
    /// use rust_connector_api::APIClient;
    /// // New client with username, password and 10 second request timeout.
    /// let client = APIClient::new("ferris_loves_rustaceans".to_string(), "0123456789".to_string(), 10);
    /// ```
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

    /// Route query using postal codes.
    ///  
    /// # Examples
    ///
    /// ```rust, no_run
    /// use chrono::{Utc, Duration};
    /// use rust_connector_api::APIClient;
    /// 
    /// #[tokio::main] 
    /// async fn main() {
    ///     let client = APIClient::new("ferris_loves_rustaceans".to_string(), "0123456789".to_string(), 10);
    ///     let dates = vec![Utc::now(), Utc::now(), Utc::now()];
    ///     let pcodes = vec!["postal_CH8000".to_string(), "postal_CH9000".to_string()];
    ///     let params = vec!["t_2m:C".to_string(), "precip_1h:mm".to_string()];
    ///     let df_route = client.route_query_postal(&dates, &pcodes, &params).await.unwrap();
    /// }
    /// ```
    pub async fn route_query_postal(
        &self,
        dates: &[chrono::DateTime<chrono::Utc>],
        pcodes: &[String],
        params: &[String],
    ) -> std::result::Result<polars::frame::DataFrame, ConnectorError> {
        // Create the dates formatted string
        let dates_str: String = dates.iter().map(|d| d.to_rfc3339()).collect::<Vec<String>>().join(",");

        // Create the points formatted string
        let points_str: String = pcodes.join("+");

        // Create the parameters formatted string
        let params_str: String = params.join(",");

        // Create the query specs
        let query_specs = build_route_query_specs(&dates_str, &params_str, &points_str).await;

        // Create the full URL
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

    /// Route query using points. 
    /// # Examples
    ///
    /// ```rust, no_run
    /// use chrono::{Utc, Duration, TimeZone};
    /// use rust_connector_api::{APIClient, Point};
    /// 
    /// #[tokio::main] 
    /// async fn main() {
    ///     let client = APIClient::new("ferris_loves_rustaceans".to_string(), "0123456789".to_string(), 10);
    /// 
    ///     // Create time information
    ///     let date1 = Utc.ymd(2021, 5, 25).and_hms_micro(12, 0, 0, 0);
    ///     let date2 = Utc.ymd(2021, 5, 25).and_hms_micro(13, 0, 0, 0);
    ///     let dates = vec![date1, date2];
    ///
    ///     // Create Parameters
    ///     let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm")];
    ///
    ///     // Create Locations
    ///     let p1: Point = Point { lat: 47.423938, lon: 9.372858};
    ///     let p2: Point = Point { lat: 47.499419, lon: 8.726517};
    ///     let coords = vec![p1, p2];
    /// 
    ///     let df_route = client.route_query_points(&dates, &coords, &parameters).await.unwrap();
    /// }
    /// ```
    pub async fn route_query_points(
        &self,
        dates: &[chrono::DateTime<chrono::Utc>],
        points: &[crate::location::Point],
        params: &[String],
    ) -> std::result::Result<polars::frame::DataFrame, ConnectorError> {
        // Create the dates formatted string
        let dates_str: String = dates.iter().map(|d| d.to_rfc3339()).collect::<Vec<String>>().join(",");

        // Create the points formatted string
        let points_str: String = points.iter().map(|p| p.to_string()).collect::<Vec<String>>().join("+");

        // Create the parameters formatted string
        let params_str: String = params.join(",");

        // Create the query specs
        let query_specs = build_route_query_specs(&dates_str, &params_str, &points_str).await;

        // Create the full URL
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

    /// Query lightning in a grid
    /// 
    /// # Examples
    /// 
    /// ```rust, no_run
    /// use chrono::{Utc, Duration, TimeZone};
    /// use rust_connector_api::{APIClient, BBox, TimeSeries};
    /// 
    /// #[tokio::main] 
    /// async fn main() {
    ///     let client = APIClient::new("ferris_loves_rustaceans".to_string(), "0123456789".to_string(), 10);
    ///     
    ///     // Create time information
    ///     let start_date = Utc.ymd(2022, 5, 20).and_hms_micro(10, 0, 0, 0);
    ///     let time_series = TimeSeries {
    ///         start: start_date,
    ///         end: start_date + Duration::days(1),
    ///         timedelta: None
    ///     };
    ///     
    ///     // Create Location
    ///     let bbox: BBox = BBox {
    ///         lat_min: 45.8179716,
    ///         lat_max: 47.8084648,
    ///         lon_min: 5.9559113,
    ///         lon_max: 10.4922941,
    ///         lat_res: 0.0,
    ///         lon_res: 0.0
    ///     };
    ///     
    ///     let df_lightning = client.query_lightning(&time_series, &bbox).await.unwrap();
    /// }
    /// ```
    pub async fn query_lightning(
        &self,
        time_series: &TimeSeries,
        bbox: &BBox
    ) -> std::result::Result<polars::frame::DataFrame, ConnectorError> {
        // Create the bounding box string according to API specification.
        let coords_str = format!(
            "{},{}_{},{}", 
            bbox.lat_max,
            bbox.lon_min,
            bbox.lat_min,
            bbox.lon_max
        );

        // Create the query for lightning
        let query_specs = build_grid_ts_lightning_query_specs(time_series, &coords_str).await;

        // Create the full URL
        let full_url = build_url(&query_specs).await.map_err(|_| ConnectorError::ParseError)?;

        // Get the query result
        let result = self.do_http_get(full_url).await;

        // Match the result
        match result {
            Ok(response) => match response.status() {
                StatusCode::OK => {
                    let mut df = parse_response_to_df(
                        response).await?;
                    df.rename("stroke_time:sql", "validdate")?;
                    df.rename("stroke_lat:d", "lat")?;
                    df.rename("stroke_lon:d", "lon")?;
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

    /// Returns a struct with information about your account.
    /// 
    /// # Examples
    /// ```rust, no_run
    /// use rust_connector_api::APIClient;
    /// 
    /// #[tokio::main] 
    /// async fn main() {
    ///     let client = APIClient::new("ferris_loves_rustaceans".to_string(), "0123456789".to_string(), 10);
    ///     let ustats = client.query_user_features().await.unwrap();
    ///     println!("user: {}, total requests: {}", ustats.stats.username, ustats.stats.total.used);
    /// }
    /// ```
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
    /// 
    /// # Examples
    /// 
    /// ```rust, no_run
    /// use chrono::{Utc, Duration, TimeZone};
    /// use rust_connector_api::{APIClient, Point, TimeSeries};
    /// 
    /// #[tokio::main] 
    /// async fn main() {
    ///     let client = APIClient::new("ferris_loves_rustaceans".to_string(), "0123456789".to_string(), 10);
    ///     
    ///     // Create time information
    ///     let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    ///     let time_series = TimeSeries {
    ///         start: start_date,
    ///         end: start_date + Duration::days(1),
    ///         timedelta: Option::from(Duration::hours(12))
    ///     };
    /// 
    ///     // Create Parameters
    ///     let parameters = vec![String::from("t_2m:C")];
    /// 
    ///     // Create Locations
    ///     let p1 = Point { lat: 52.52, lon: 13.405};
    ///     let coords = vec![p1];
    /// 
    ///     // Get query result 
    ///     let df_point_ts = client
    ///         .query_time_series(&time_series, &parameters, &coords, &None)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub async fn query_time_series(
        &self,
        time_series: &TimeSeries,
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
            time_series, parameters, &coords_str, optionals, &String::from("csv")
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
    /// 
    /// # Examples
    /// 
    /// ```rust, no_run
    /// use chrono::{Utc, Duration, TimeZone};
    /// use rust_connector_api::{APIClient, Point, TimeSeries};
    /// 
    /// #[tokio::main] 
    /// async fn main() {
    ///     let client = APIClient::new("ferris_loves_rustaceans".to_string(), "0123456789".to_string(), 10);
    ///     
    ///     // Create time information
    ///     let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    ///     let time_series = TimeSeries {
    ///         start: start_date,
    ///         end: start_date + Duration::days(1),
    ///         timedelta: Option::from(Duration::hours(12))
    ///     };
    /// 
    ///     // Create Parameters
    ///     let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm")];
    /// 
    ///     // Create Locations
    ///     let postal1 = vec![String::from("postal_CH8000"), String::from("postal_CH9000")];
    /// 
    ///     // Call endpoint
    ///     let df_ts_postal = client
    ///         .query_time_series_postal(&time_series, &parameters, &postal1, &None)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub async fn query_time_series_postal(&self,
        time_series: &TimeSeries,
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
            time_series, parameters, &coords_str, optionals, &String::from("csv")
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
    /// 
    /// # Examples
    /// 
    /// ```rust, no_run
    /// use chrono::{Utc, Duration, TimeZone};
    /// use rust_connector_api::{APIClient, BBox};
    /// 
    /// #[tokio::main] 
    /// async fn main() {
    ///     let client = APIClient::new("ferris_loves_rustaceans".to_string(), "0123456789".to_string(), 10);
    ///     
    ///     // Create time information
    ///     let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    /// 
    ///     // Create Parameters
    ///     let parameter = String::from("t_2m:C");
    /// 
    ///     // Create Location
    ///     let bbox = BBox {
    ///         lat_min: 52.40,
    ///         lat_max: 52.50,
    ///         lon_min: 13.40,
    ///         lon_max: 13.50,
    ///         lat_res: 0.05,
    ///         lon_res: 0.05
    ///     };
    /// 
    ///     // Call endpoint
    ///     let df_grid_piv = client.query_grid_pivoted(
    ///         &start_date, &parameter, &bbox, &None
    ///         )
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
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

    /// Download a ```polars``` DataFrame from the API for a grid of locations bounded by a bounding
    /// box object ```BBox``` and an arbitray number of parameters and a unique point in time. 
    /// 
    /// # Examples
    /// 
    /// ```rust, no_run
    /// use chrono::{Utc, Duration, TimeZone};
    /// use rust_connector_api::{APIClient, BBox};
    /// 
    /// #[tokio::main] 
    /// async fn main() {
    ///     let client = APIClient::new("ferris_loves_rustaceans".to_string(), "0123456789".to_string(), 10);
    ///     
    ///     // Create time information
    ///     let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    /// 
    ///     // Create Parameters
    ///     let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm")];
    /// 
    ///     // Create Location
    ///     let bbox = BBox {
    ///         lat_min: 52.40,
    ///         lat_max: 52.50,
    ///         lon_min: 13.40,
    ///         lon_max: 13.50,
    ///         lat_res: 0.05,
    ///         lon_res: 0.05
    ///     };
    /// 
    ///     // Call endpoint
    ///     let df_grid_unpiv = client.query_grid_unpivoted(
    ///         &start_date, &parameters, &bbox, &None
    ///         )
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
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

    /// Download a ```polars``` DataFrame from the API for a grid of locations bounded by a bounding
    /// box object ```BBox``` and an arbitray number of parameters and a time series.  
    /// 
    /// # Examples
    /// 
    /// ```rust, no_run
    /// use chrono::{Utc, Duration, TimeZone};
    /// use rust_connector_api::{APIClient, BBox, TimeSeries};
    /// 
    /// #[tokio::main] 
    /// async fn main() {
    ///     let client = APIClient::new("ferris_loves_rustaceans".to_string(), "0123456789".to_string(), 10);
    ///     
    ///     // Create time information
    ///     // 1989-11-09 19:00:00 --> 18:00:00 UTC
    ///     let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    ///     let time_series = TimeSeries {
    ///         start: start_date,
    ///         end: start_date + Duration::days(1),
    ///         timedelta: Option::from(Duration::hours(12))
    ///     };
    /// 
    ///     // Create Parameters
    ///     let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm")];
    /// 
    ///     // Create Location
    ///     let bbox = BBox {
    ///         lat_min: 52.40,
    ///         lat_max: 52.50,
    ///         lon_min: 13.40,
    ///         lon_max: 13.50,
    ///         lat_res: 0.05,
    ///         lon_res: 0.05
    ///     };
    /// 
    ///     // Call endpoint
    ///     let df_grid_unpiv_ts = client
    ///         .query_grid_unpivoted_time_series(&time_series, &parameters, &bbox, &None)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub async fn query_grid_unpivoted_time_series(&self,
        time_series: &TimeSeries,
        parameters: &[String],
        bbox: &BBox,
        optionals: &Option<Vec<String>>
    ) -> Result<polars::frame::DataFrame, ConnectorError> {
        // Create the bounding box string according to API specification.
        let coords_str = format!("{}", bbox);

        // Create the query specifications (time, location, etc.)
        let query_specs = build_ts_query_specs(
            time_series, parameters, &coords_str, optionals, &String::from("csv")
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

    /// Download a ```NetCDF``` from the API for a grid of locations bounded by a bounding box object
    /// ```BBox``` and a single parameters and a time series.
    /// 
    /// # Examples
    /// 
    /// ```rust, no_run
    /// use chrono::{Utc, Duration, TimeZone};
    /// use rust_connector_api::{APIClient, BBox, TimeSeries};
    /// 
    /// #[tokio::main] 
    /// async fn main() {
    ///     let client = APIClient::new("ferris_loves_rustaceans".to_string(), "0123456789".to_string(), 10);
    ///     
    ///     // Create time information
    ///     // 1989-11-09 19:00:00 --> 18:00:00 UTC
    ///     let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    ///     let time_series = TimeSeries {
    ///         start: start_date,
    ///         end: start_date + Duration::days(1),
    ///         timedelta: Option::from(Duration::hours(12))
    ///     };
    /// 
    ///     // Create Parameters
    ///     let parameter =String::from("t_2m:C");
    /// 
    ///     // Create Location
    ///     let bbox = BBox {
    ///         lat_min: 52.40,
    ///         lat_max: 52.50,
    ///         lon_min: 13.40,
    ///         lon_max: 13.50,
    ///         lat_res: 0.05,
    ///         lon_res: 0.05
    ///     };
    /// 
    ///     // Create file name
    ///     let file_name = String::from("tests/netcdf/my_netcdf.nc");
    /// 
    ///     // Call endpoint
    ///     client.query_netcdf(&time_series, &parameter, &bbox, &file_name, &None)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub async fn query_netcdf(&self,
        time_series: &TimeSeries,
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
            time_series, parameter, &coords_str, "netcdf", optionals
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
    /// 
    /// # Examples
    /// 
    /// ```rust, no_run
    /// use chrono::{Utc, Duration, TimeZone};
    /// use rust_connector_api::{APIClient, BBox};
    /// 
    /// #[tokio::main] 
    /// async fn main() {
    ///     let client = APIClient::new("ferris_loves_rustaceans".to_string(), "0123456789".to_string(), 10);
    ///     
    ///     // Create time information
    ///     // 1989-11-09 19:00:00 --> 18:00:00 UTC
    ///     let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    /// 
    ///     // Create Parameters
    ///     let parameter = String::from("t_2m:C");
    /// 
    ///     // Create Location
    ///     let bbox = BBox {
    ///         lat_min: 45.8179716,
    ///         lat_max: 47.8084648,
    ///         lon_min: 5.9559113,
    ///         lon_max: 10.4922941,
    ///         lat_res: 0.01,
    ///         lon_res: 0.01
    ///     };
    /// 
    ///     // Create file name
    ///     let file_name = String::from("tests/png/my_png.png");
    /// 
    ///     // Call endpoint
    ///     client.query_grid_png(&start_date, &parameter, &bbox, &file_name, &None)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
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
    /// 
    /// # Examples
    /// 
    /// ```rust, no_run
    /// use chrono::{Utc, Duration, TimeZone};
    /// use rust_connector_api::{APIClient, BBox, TimeSeries};
    /// 
    /// #[tokio::main] 
    /// async fn main() {
    ///     let client = APIClient::new("ferris_loves_rustaceans".to_string(), "0123456789".to_string(), 10);
    ///     
    ///     // Create time information
    ///     // 1989-11-09 19:00:00 --> 18:00:00 UTC
    ///     let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    ///     let time_series = TimeSeries {
    ///         start: start_date,
    ///         end: start_date + Duration::days(1),
    ///         timedelta: Option::from(Duration::hours(12))
    ///     };
    /// 
    ///     // Create Parameters
    ///     let parameter = String::from("t_2m:C");
    /// 
    ///     // Create Location
    ///     let bbox = BBox {
    ///         lat_min: 45.8179716,
    ///         lat_max: 47.8084648,
    ///         lon_min: 5.9559113,
    ///         lon_max: 10.4922941,
    ///         lat_res: 0.01,
    ///         lon_res: 0.01
    ///     };
    /// 
    ///     // Create file name
    ///     let prefixpath: String = String::from("tests/png_series/test_series");
    /// 
    ///     // Call endpoint
    ///     client.query_grid_png_timeseries(&time_series, &parameter, &bbox, &prefixpath, &None)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    pub async fn query_grid_png_timeseries(&self,
        time_series: &TimeSeries,
        parameter: &String,
        bbox: &BBox,
        prefixpath: &String,
        optionals: &Option<Vec<String>>
    ) -> Result<(), ConnectorError> {

        // Iterate the time series
        let mut dt_cur = time_series.start;
        let fmt = "%Y%m%d_%H%M%S";
        while dt_cur <= time_series.end {
            let cur_file_name = format!("{}_{}.png", prefixpath, dt_cur.format(fmt));
            self.query_grid_png(&dt_cur, parameter, bbox, &cur_file_name, optionals).await?;
            dt_cur = dt_cur + time_series.timedelta.unwrap(); // panic when timedelta absent
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
    
    #[tokio::test]
    async fn client_fires_get_request() {

        // Query to the mockup server running at Postman
        let query = crate::client::build_url(
            &"status".to_string()
        ).await.unwrap();

        // Credentials
        let api_key = "test_password".to_string();
        let api_user = "test_user".to_string();
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
