//! # Util
//! This module bundles different utilities for the client. It should not be necessary to access these
//! tools outside the client. They are mainly grouped into utilities for de- and serialization of JSON
//! HTTP responses, the creation of ```polars::frame::DataFrame``` objects from CSV HTTP responses and
//! some modifications of the created DataFrames. Lastly there are some utilities for the creation of
//! PNG and NetCDF files based on the HTTP response.

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

/// This contains the actual information about the account statistics, quota and permissions.
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
    #[serde(rename(serialize = "model set", deserialize = "model set"))]
    pub models: Vec<String>,
    #[serde(rename(serialize = "error message", deserialize = "error message"))]
    pub error: String,
    #[serde(rename(serialize = "contact emails", deserialize = "contact emails"))]
    pub contact: Vec<String>,
}

/// The Limit struct is used to de-serialize the limit attributes of the account (e.g. how many 
/// requests in parallel are allowed etc.)
#[derive(Debug, Deserialize, Serialize)]
pub struct Limit{
    pub used: u32,
    #[serde(rename(serialize = "soft limit", deserialize = "soft limit"))]
    pub soft_lim: u32,
    #[serde(rename(serialize = "hard limit", deserialize = "hard limit"))]
    pub hard_lim: u32
}

// Deserializes the response for the user_stats_json query.
pub async fn extract_user_statistics(response: Response) -> std::result::Result<UStatsResponse, ConnectorError> {
    let json: UStatsResponse = response.json::<UStatsResponse>().await?;
    Ok(json)
}

/// Writes the HTTP response to a file. 
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
    // TODO: Convert to simplified version see postal code
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
pub async fn df_add_postal(df_in: polars::frame::DataFrame, postal: &str) -> 
std::result::Result<polars::frame::DataFrame, polars::error::PolarsError> {
    // https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html#method.shape
    // Get (height, width) of the DataFrame. Get width:
    let n = df_in.height();
    let col_zipcode = vec![postal; n];
    // https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html#method.extend
    // https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html#method.get_column_names
    let df_tmp = df!("station_id" => col_zipcode)?;
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
    // TODO: rename variable
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

// Converts the HTTP response into a polars DataFrame.
pub async fn parse_grid_response_to_df(
    response: Response,
) -> std::result::Result<polars::frame::DataFrame, polars::error::PolarsError> {
        // Get the response text:
        let body = response.text().await.unwrap();

        // Parse the response to a DataFrame
        // TODO: Rename variable
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

/// Builds the query specifications ('specs') for a time series query according to the Meteomatics API
/// format rules. Optionally parses a number of provided extra specifiers (e.g. 'model=mix'). The
/// dates are formatted according to ISO8601 (<https://en.wikipedia.org/wiki/ISO_8601>). The format
/// parameter specifies the requested file type (e.g. "csv" or "netcdf" or "png").
pub async fn build_ts_query_specs(
    start_date: &chrono::DateTime<chrono::Utc>,
    end_date: &chrono::DateTime<chrono::Utc>,
    interval: &chrono::Duration,
    parameters: &[String],
    coords_str: &str,
    optionals: &Option<Vec<String>>,
    format: &String,
) -> String {

    let query_specs = format!(
        "{}--{}:{}/{}/{}/{}",
        start_date.to_rfc3339(),
        end_date.to_rfc3339(),
        interval,
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
    
    query_specs
}

/// Builds the query specifications ('specs') for a grid query according to the Meteomatics API
/// format rules. Optionally parses a number of provided extra specifiers (e.g. 'model=mix'). The
/// date is formatted according to ISO8601 (<https://en.wikipedia.org/wiki/ISO_8601>). The format
/// parameter specifies the requested file type (e.g. "csv" or "netcdf" or "png").
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
        parameter,
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
    
    query_specs
}

/// Builds the query specifications ('specs') for a time series grid query according to the Meteomatics 
/// API format rules. Optionally parses a number of provided extra specifiers (e.g. 'model=mix'). The
/// dates are formatted according to ISO8601 (<https://en.wikipedia.org/wiki/ISO_8601>). The format
/// parameter specifies the requested file type (e.g. "csv" or "netcdf" or "png").
pub async fn build_grid_ts_query_specs(
    start_date: &chrono::DateTime<chrono::Utc>,
    end_date: &chrono::DateTime<chrono::Utc>,
    interval: &chrono::Duration,
    parameter: &String,
    coords_str: &str,
    format: &str,
    optionals: &Option<Vec<String>>,
) -> String {
    let query_specs = format!(
        "{}--{}:{}/{}/{}/{}",
        start_date.to_rfc3339(),
        end_date.to_rfc3339(),
        interval,
        parameter,
        coords_str,
        format,
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
    
    query_specs
}

/// This query is used to get information about lightning in a defined area and over a certain amount
/// of time (defined by ```start_date``` and ```end_date```).
pub async fn build_grid_ts_lightning_query_specs(
    start_date: &chrono::DateTime<chrono::Utc>,
    end_date: &chrono::DateTime<chrono::Utc>,
    coords_str: &str
) -> String {
    let query_specs = format!(
        "get_lightning_list?time_range={}--{}&bounding_box={}&format=csv",
        start_date.to_rfc3339(),
        end_date.to_rfc3339(),
        coords_str
    );
    query_specs 
}

/// Creates the query specs for the route query type.
pub async fn build_route_query_specs(
    dates: &str,
    parameters: &str,
    coords_str: &str
) -> String {
    let query_specs = format!(
        "{}/{}/{}/csv?route=true",
        dates,
        parameters,
        coords_str
    );
    query_specs
}

/// Combines the default base API URL with the query specific information.
pub async fn build_url(url_fragment: &str) -> std::result::Result<Url, ParseError> {
    let base_url = Url::parse(BASE_URL).expect("Base URL is known to be valid");
    let full_url = base_url.join(url_fragment)?;
    Ok(full_url)
}

/// Convert a number of Points to a String according to the Meteomatics API specifications.
pub async fn points_to_str(coords: &[Point]) -> String {
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