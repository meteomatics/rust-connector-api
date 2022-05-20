//! # Util
//! This module bundles different utilities for the client.

// Crates
use serde::{Deserialize, Serialize};
use std::fs::File;
use reqwest::Response;
use url::{ParseError, Url};
use crate::errors::ConnectorError;
use std::path::Path;
use std::fs;
use polars::prelude::*;
use crate::location::Point;

// Default API URL
const BASE_URL: &str = "https://api.meteomatics.com";

/// Top-level struct for the De-serialization of the query results for <https://api.meteomatics.com/user_stats_json>.
/// This query gives an overview about the request activity of your account as well as information about
/// the feature availability for your account (e.g. if you are allowed to download gridded data or not.)
/// The response contains a message together with all the statistics for your account.
#[derive(Debug, Deserialize, Serialize)]
pub struct UStatsResponse{
    pub message: String,
    #[serde(rename(serialize = "user statistics", deserialize = "user statistics"))]
    pub stats: UserStats,
}

/// This contains the actual information about your account statistics.
#[derive(Debug, Deserialize, Serialize)]
pub struct UserStats{
    pub username: String,
    #[serde(rename(serialize = "requests total", deserialize = "requests total"))]
    pub total: Limit,
    #[serde(rename(serialize = "requests since last UTC midnight", deserialize = "requests since last UTC midnight"))]
    pub since_midnight: Limit,
    #[serde(rename(serialize = "requests since HH:00:00", deserialize = "requests since HH:00:00"))]
    pub since_0: Limit,
    #[serde(rename(serialize = "requests in the last 60 seconds", deserialize = "requests in the last 60 seconds"))]
    pub since_60s: Limit,
    #[serde(rename(serialize = "requests in parallel", deserialize = "requests in parallel"))]
    pub parallel: Limit,
    #[serde(rename(serialize = "historic request option", deserialize = "historic request option"))]
    pub hist: String,
    #[serde(rename(serialize = "area request option", deserialize = "area request option"))]
    pub area: bool,
    #[serde(rename(serialize = "model select option", deserialize = "model select option"))]
    pub models: Vec<String>,
    #[serde(rename(serialize = "error message", deserialize = "error message"))]
    pub error: String,
    #[serde(rename(serialize = "contact emails", deserialize = "contact emails"))]
    pub contact: Vec<String>,
}

/// The Limit struct is used to de-serialize the limit attributes of your account (e.g. how many 
/// requests you can make in parallel or in total per day, etc.)
#[derive(Debug, Deserialize, Serialize)]
pub struct Limit{
    pub used: u32,
    #[serde(rename(serialize = "soft limit", deserialize = "soft limit"))]
    pub soft_lim: u32,
    #[serde(rename(serialize = "hard limit", deserialize = "hard limit"))]
    pub hard_lim: u32
}

pub async fn extract_user_statistics(response: Response) -> std::result::Result<UStatsResponse, ConnectorError> {
    let json: UStatsResponse = response.json::<UStatsResponse>().await?;
    Ok(json)
}

/// Write to file
pub async fn write_file(response: Response, file_name: &String) -> std::result::Result<(), ConnectorError> {
    let body = response.bytes().await?;
    let mut content = std::io::Cursor::new(body);

    let mut file = File::create(file_name)?;
    std::io::copy(&mut content, &mut file)?;
    Ok(())
}

/// Creates a path if it does not already exist.
pub async fn create_path(file_name: &String) -> std::result::Result<(), ConnectorError> {
    // https://www.programming-idioms.org/idiom/212/check-if-folder-exists
    let dir: &Path = Path::new(file_name).parent().unwrap();
    let b: bool = dir.is_dir();

    if !b {
       match fs::create_dir_all(dir) {
           Ok(_) => Ok(()),
           Err(_) => Err(ConnectorError::FileIOError)
       }
    } else {
        Ok(())
    }

}

/// Creates a new DataFrame with added latitude and longitude extracted from the provided ```Point```.
pub async fn df_add_latlon(df_in: polars::frame::DataFrame, point: &Point) -> 
std::result::Result<polars::frame::DataFrame, polars::error::PolarsError> {
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

/// Creates a new DataFrame with added postal_Code extracted from the provided postal code.
pub async fn df_add_postal(df_in: polars::frame::DataFrame, postal: &String) -> 
std::result::Result<polars::frame::DataFrame, polars::error::PolarsError> {
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
pub async fn parse_response_to_df(
    response: Response,
) -> std::result::Result<polars::frame::DataFrame, polars::error::PolarsError> {
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

pub async fn parse_grid_response_to_df(
    response: Response,
) -> std::result::Result<polars::frame::DataFrame, polars::error::PolarsError> {
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
pub async fn build_ts_query_specs(
    start_date: &chrono::DateTime<chrono::Utc>,
    end_date: &chrono::DateTime<chrono::Utc>,
    interval: &chrono::Duration,
    parameters: &Vec<String>,
    coords_str: &str,
    optionals: &Option<Vec<String>>,
    format: &String,
) -> String {

    let query_specs = format!(
        "{}--{}:{}/{}/{}/{}",
        start_date.to_rfc3339(),
        end_date.to_rfc3339(),
        interval.to_string(),
        parameters.join(","),
        coords_str,
        format
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
pub async fn build_grid_query_specs(
    start_date: &chrono::DateTime<chrono::Utc>,
    parameter: &String,
    coords_str: &str,
    optionals: &Option<Vec<String>>,
    format: &String,
) -> String {
    let query_specs = format!(
        "{}/{}/{}/{}",
        start_date.to_rfc3339(),
        parameter.to_string(),
        coords_str,
        format
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

pub async fn build_netcdf_query_specs(
    start_date: &chrono::DateTime<chrono::Utc>,
    end_date: &chrono::DateTime<chrono::Utc>,
    interval: &chrono::Duration,
    parameter: &String,
    coords_str: &str,
    optionals: &Option<Vec<String>>,
) -> String {
    let query_specs = format!(
        "{}--{}:{}/{}/{}/netcdf",
        start_date.to_rfc3339(),
        end_date.to_rfc3339(),
        interval.to_string(),
        parameter,
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
pub async fn build_url(url_fragment: &str) -> std::result::Result<Url, ParseError> {
    let base_url = Url::parse(BASE_URL).expect("Base URL is known to be valid");
    let full_url = base_url.join(url_fragment)?;
    Ok(full_url)
}

/// Convert a number of Points to a String according to the Meteomatics API specifications.
pub async fn points_to_str(coords: &Vec<Point>) -> String {
    coords.iter().map(|p| format!("{}", p)).collect::<Vec<String>>().join("+")
}

#[cfg(test)]
mod tests {

    use chrono::prelude::*;
    use chrono::Duration;
    use crate::location::{Point, BBox};
    use std::path::Path;
    use std::fs;


    #[tokio::test]
    async fn check_path_creation_nonexistent() {
        let file_name: String = String::from("tests/netcdfs/my_netcdf.nc");
        crate::util::create_path(&file_name).await.unwrap();
        let dir: &Path = Path::new(&file_name).parent().unwrap();
        let check: bool = dir.is_dir();
        assert_eq!(check, true);
        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    // checks if the location specifier is correctly created
    async fn check_locations_string() {
        let p1: Point = Point { lat: 52.520551, lon: 13.461804};
        let p2: Point = Point { lat: -52.520551, lon: 13.461804};
        let coords: Vec<Point> = vec![p1, p2];
        let coord_str = crate::util::points_to_str(&coords).await;
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
        let coord_str = crate::util::points_to_str(&coords).await;

        let query_s = crate::util::build_ts_query_specs(
            &start_date, &end_date, &interval, &parameters, &coord_str, &None, &String::from("csv")
        ).await;
        assert_eq!(
            "2022-05-17T12:00:00+00:00--2022-05-18T12:00:00+00:00:PT3600S/t_2m:C/52.520551,13.461804/csv", 
            query_s
        );
        
        // microseconds
        let start_date = Utc.ymd(2022, 5, 17).and_hms_micro(12, 00, 00, 453_829);
        let end_date = start_date + Duration::days(1);
        let interval = Duration::hours(1);

        let query_ms = crate::util::build_ts_query_specs(
            &start_date, &end_date, &interval, &parameters, &coord_str, &None, &String::from("csv")
        ).await;
        assert_eq!(
            "2022-05-17T12:00:00.453829+00:00--2022-05-18T12:00:00.453829+00:00:PT3600S/t_2m:C/52.520551,13.461804/csv", 
            query_ms
        );

        // nanoseconds
        let start_date = Utc.ymd(2022, 5, 17).and_hms_nano(12, 00, 00, 453_829_123);
        let end_date = start_date + Duration::days(1);
        let interval = Duration::hours(1);

        let query_ns = crate::util::build_ts_query_specs(
            &start_date, &end_date, &interval, &parameters, &coord_str, &None, &String::from("csv")
        ).await;
        assert_eq!(
            "2022-05-17T12:00:00.453829123+00:00--2022-05-18T12:00:00.453829123+00:00:PT3600S/t_2m:C/52.520551,13.461804/csv", 
            query_ns
        );
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