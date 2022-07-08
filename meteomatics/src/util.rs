//! # Util
//! This module bundles different utilities for the client. It should not be necessary to access these
//! tools outside the client. They are mainly grouped into utilities for de- and serialization of JSON
//! HTTP responses, the creation of [`DataFrame`](polars::frame::DataFrame) objects from CSV HTTP responses and
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
use std::fmt;

// Default API URL
const BASE_URL: &str = "https://api.meteomatics.com";

/// Container for time series information. This allows functions to use less parameters. 
/// 
/// # Arguments
/// 
/// * `start` - Specify date and time for the start of the time series.
/// * `end` - Specify date and time for the end of the time series.
/// * `timedelta` - Optionally used to specify the time step of the time series.
/// 
/// # Examples
/// 
/// ```rust, no_run
/// use meteomatics::TimeSeries;
/// use chrono::{DateTime, Duration, Utc, TimeZone};
/// let dt_start = Utc::now();
/// let time_series = TimeSeries {
///     start: dt_start,
///     end: dt_start + Duration::days(1),
///     timedelta: Option::from(Duration::hours(3))
/// };
/// 
/// println!("Time series: {}", time_series);
/// ```
#[derive(Debug)]
pub struct TimeSeries{
    pub start: chrono::DateTime<chrono::Utc>,
    pub end: chrono::DateTime<chrono::Utc>,
    pub timedelta: Option<chrono::Duration>
}

impl fmt::Display for TimeSeries {
    fn fmt(&self, f:&mut fmt::Formatter) -> fmt::Result {
        write!(
            f, 
            "{}--{}:{}", 
            &self.start.to_rfc3339(),
            &self.end.to_rfc3339(),
            &self.timedelta.unwrap()
        )
    }
}

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
    match response.json::<UStatsResponse>()
        .await {
            Ok(json) => Ok(json),
            Err(e) => Err(ConnectorError::ReqwestError(e.to_string())),
        }
}

/// Writes the HTTP response to a file. 
/// 
/// # Arguments
/// 
/// * `response` - The HTTP response object. 
/// * `file_name` - The name for the file to be written (complete with path). 
/// 
pub async fn write_file(response: Response, file_name: &String) -> std::result::Result<(), ConnectorError> {
    let body = response.bytes().await.map_err(|e| ConnectorError::ReqwestError(e.to_string())).unwrap();
    let mut content = std::io::Cursor::new(body);

    let mut file = File::create(file_name)?;
    std::io::copy(&mut content, &mut file)?;
    Ok(())
}

/// Creates a path if it does not already exist.
/// 
/// # Arguments
/// 
/// * `file_name` - The full path (incl. name) to a specific file. 
/// 
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
/// 
/// # Arguments
/// 
/// * `df_in` - DataFrame as derived from the HTTP response, with missing columns for lat/lon
/// * `point` - The specific point in space (with latitude and longitude) from which to extract the 
/// lat / lon value for the column.
/// 
pub async fn df_add_latlon(df_in: polars::frame::DataFrame, point: &Point) -> 
std::result::Result<polars::frame::DataFrame, polars::error::PolarsError> {
    use polars::prelude::*;
    // https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html#method.shape
    // Get (height, width) of the DataFrame. Get width:
    let n = df_in.height();
    let lat = vec![point.lat; n];
    let lon = vec![point.lon; n];
    // https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html#method.extend
    // https://docs.rs/polars/latest/polars/frame/struct.DataFrame.html#method.get_column_names
    let df_tmp = df!("lat" => &lat, "lon" => &lon)?;
    let df_out: DataFrame = df_tmp.hstack(df_in.get_columns())?;
    Ok(df_out)
}

/// Creates a new DataFrame with added postal_Code extracted from the provided postal code.
/// 
/// # Arguments
/// 
/// * `df_in` - DataFrame as derived from the HTTP response, with missing a column with the postal code.
/// * `postal` - The specific postal code for which we need to add a column.
///
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

/// Convert the HTTP response into a [`DataFrame`](polars::frame::DataFrame). Consumes the HTTP response.
/// This is used in all cases where the API response is a tidy CSV.
/// 
/// # Arguments
/// 
/// * `response` - The HTTP response from the query to the meteomatics API.
/// 
pub async fn parse_response_to_df(
    response: Response,
) -> std::result::Result<polars::frame::DataFrame, polars::error::PolarsError> {
    // Get the response text:
    let body = response.text().await.unwrap();

    // Parse the response to a DataFrame
    let file = std::io::Cursor::new(&body);
    use polars::prelude::*; 
    let dataframe = polars::io::csv::CsvReader::new(file)
        .infer_schema(Some(100))
        .with_delimiter(b';')
        .has_header(true)
        .with_parse_dates(false)
        .with_ignore_parser_errors(false)
        .finish()?;

    Ok(dataframe)
}

/// Convert the HTTP response into a [`DataFrame`](polars::frame::DataFrame). Consumes the HTTP response.
/// This is used in cases where the API response is not a tidy CSV. For example when downloading a
/// CSV grid for a single point the returned CSV is pivoted and not in a tidy column-oriented format.
/// 
/// # Arguments
/// 
/// * `response` - The HTTP response from the query to the meteomatics API.
/// 
pub async fn parse_grid_response_to_df(
    response: Response,
) -> std::result::Result<polars::frame::DataFrame, polars::error::PolarsError> {
        // Get the response text:
        let body = response.text().await.unwrap();

        // Parse the response to a DataFrame
        let file = std::io::Cursor::new(&body);
        use polars::prelude::*; 
        let dataframe = polars::io::csv::CsvReader::new(file)
            .infer_schema(Some(100))
            .with_delimiter(b';')
            .has_header(true)
            .with_skip_rows(2)
            .with_parse_dates(false)
            .with_ignore_parser_errors(false)
            .finish()?;
    
        Ok(dataframe)
}

/// Builds the query specifications ('specs') for a time series query according to the Meteomatics API
/// format rules. Optionally parses a number of provided extra specifiers (e.g. 'model=mix'). The
/// dates are formatted according to ISO8601 (<https://en.wikipedia.org/wiki/ISO_8601>). The format
/// parameter specifies the requested file type (e.g. "csv" or "netcdf" or "png").
/// 
/// # Arguments
/// 
/// * `time_series` - Defines the temporal extent (time and date of start and a timedelta).
/// * `parameters` - Names of individual parameters (e.g. "t_2m:C", "wind_speed_10m:ms"). 
/// * `coords_str` - Specifies the locations for the API (formatted according to the API rules, e.g.
/// '47.0,8+46.5,9')
/// * `optionals` - Optional parameters for the request (e.g. "calibrated=true").
/// * `format` - Specifies the file format for the request (e.g. "csv" or "netcdf")
/// 
pub async fn build_ts_query_specs(
    time_series: &TimeSeries,
    parameters: &[String],
    coords_str: &str,
    optionals: &Option<Vec<String>>,
    format: &str,
) -> String {
    let query_specs = format!(
        "{}/{}/{}/{}",
        time_series,
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
/// 
/// # Arguments
/// 
/// * `timestamp` - Date and time for the request.
/// * `parameter` - Name of an individual parameter (e.g. "t_2m:C" or "wind_speed_10m:ms"). 
/// * `coords_str` - Specifies the locations for the API (formatted according to the API rules, e.g.
/// '47.0,8+46.5,9')
/// * `optionals` - Optional parameters for the request (e.g. "calibrated=true").
/// * `format` - Specifies the file format for the request (e.g. "csv" or "netcdf")
/// 
pub async fn build_grid_query_specs(
    timestamp: &chrono::DateTime<chrono::Utc>,
    parameter: &String,
    coords_str: &str,
    optionals: &Option<Vec<String>>,
    format: &str,
) -> String {
    let query_specs = format!(
        "{}/{}/{}/{}",
        timestamp.to_rfc3339(),
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
/// 
///  # Arguments
/// 
/// * `time_series` - Defines the temporal extent (time and date of start and a timedelta).
/// * `parameter` - Name of an individual parameter (e.g. "t_2m:C" or "wind_speed_10m:ms"). 
/// * `coords_str` - Specifies the locations for the API (formatted according to the API rules, e.g.
/// '47.0,8+46.5,9')
/// * `format` - Specifies the file format for the request (e.g. "csv" or "netcdf")
/// * `optionals` - Optional parameters for the request (e.g. "calibrated=true").
/// 
pub async fn build_grid_ts_query_specs(
    time_series: &TimeSeries,
    parameter: &String,
    coords_str: &str,
    format: &str,
    optionals: &Option<Vec<String>>,
) -> String {
    let query_specs = format!(
        "{}/{}/{}/{}",
        time_series,
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
/// 
/// # Arguments
/// 
/// * `time_series` - Defines the temporal extent (time and date of start and a timedelta).
/// * `coords_str` - Specifies the locations for the API (formatted according to the API rules, e.g.
/// '47.0,8+46.5,9')
/// 
pub async fn build_grid_ts_lightning_query_specs(
    time_series: &TimeSeries,
    coords_str: &str
) -> String {
    let query_specs = format!(
        "get_lightning_list?time_range={}--{}&bounding_box={}&format=csv",
        time_series.start.to_rfc3339(),
        time_series.end.to_rfc3339(),
        coords_str
    );
    query_specs 
}

/// Creates the query specs for the route query type.
/// 
/// # Arguments
/// 
/// * `dates` - These dates specify the points in time for the respective locations. 
/// * `parameters` - Names of individual parameters (e.g. "t_2m:C" or "wind_speed_10m:ms").
/// * `coords_str` - Specifies the locations for the API (formatted according to the API rules, e.g.
/// '47.0,8+46.5,9')
/// 
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
    use serde_json;
    use crate::util::{UStatsResponse, TimeSeries};


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
        let time_series = TimeSeries{
            start: start_date,
            end: start_date + Duration::days(1),
            timedelta: Option::from(Duration::hours(1))
        };

        let parameters: Vec<String> = vec![String::from("t_2m:C")];
        let p1: Point = Point { lat: 52.520551, lon: 13.461804};
        let coords: Vec<Point> = vec![p1];
        let coord_str = crate::util::points_to_str(&coords).await;

        let query_s = crate::util::build_ts_query_specs(
            &time_series, &parameters, &coord_str, &None, &String::from("csv")
        ).await;
        assert_eq!(
            "2022-05-17T12:00:00+00:00--2022-05-18T12:00:00+00:00:PT3600S/t_2m:C/52.520551,13.461804/csv", 
            query_s
        );
        
        // microseconds
        let start_date = Utc.ymd(2022, 5, 17).and_hms_micro(12, 00, 00, 453_829);
        let time_series = TimeSeries{
            start: start_date,
            end: start_date + Duration::days(1),
            timedelta: Option::from(Duration::hours(1))
        };

        let query_ms = crate::util::build_ts_query_specs(
            &time_series, &parameters, &coord_str, &None, &String::from("csv")
        ).await;
        assert_eq!(
            "2022-05-17T12:00:00.453829+00:00--2022-05-18T12:00:00.453829+00:00:PT3600S/t_2m:C/52.520551,13.461804/csv", 
            query_ms
        );

        // nanoseconds
        let start_date = Utc.ymd(2022, 5, 17).and_hms_nano(12, 00, 00, 453_829_123);
        let time_series = TimeSeries{
            start: start_date,
            end: start_date + Duration::days(1),
            timedelta: Option::from(Duration::hours(1))
        };

        let query_ns = crate::util::build_ts_query_specs(
            &time_series, &parameters, &coord_str, &None, &String::from("csv")
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

    #[tokio::test]
    async fn check_deserialization() {
        let s1 = r#"{"message" : "In case the limits don't match your understanding of the contr"#;
        let s2 = r#"act, please contact us (api@meteomatics.com). For other inquiries please wri"#;
        let s3 = r#"te to (support@meteomatics.com). Soft or hard limit values of 0 mean that th"#;
        let s4 = r#"e corresponding limit is not set.", "user statistics" : {"username" : "rusty"#;
        let s5 = r#"thecrab", "requests total" : {"used" : 4280, "soft limit" : 0, "hard limi"#;
        let s6 = r#"t" : 0}, "requests since last UTC midnight" : {"used" : 85, "soft limit" : 1"#;
        let s7 = r#"00000, "hard limit" : 0}, "requests since HH:00:00" : {"used" : 85, "soft lim"#;
        let s8 = r#"it" : 10000, "hard limit" : 0}, "requests in the last 60 seconds" : {"used" :"#;
        let s9 = r#" 0, "soft limit" : 0, "hard limit" : 6000}, "requests in parallel" : {"used" "#;
        let s10 = r#": 0, "soft limit" : 20, "hard limit" : 500}, "historic request option" : "19"#;
        let s11 = r#"00-01-01T00:00:00Z--2100-01-01T00:00:00Z", "area request option" : true, "mo"#;
        let s12 = r#"del set" : ["all_minus_euro1k"], "error message" : "", "contact emails" : [""#;
        let s13 = r#"rustythecrab@meteomatics.com"]}}"#;
        let s = [s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13].concat();

        let json: UStatsResponse = serde_json::from_str(&s).unwrap();

        // Check if the message was correctly deserialized.
        assert_eq!(
            json.message, 
            "In case the limits don't match your understanding of the contract, \
            please contact us (api@meteomatics.com). For other inquiries please \
            write to (support@meteomatics.com). Soft or hard limit values of 0 \
            mean that the corresponding limit is not set."
        );

        // Check if the username was correctly deserialized.
        assert_eq!(json.stats.username, "rustythecrab");

        // Check if the total requests were correctly dersialized using the Limits struct
        assert_eq!(json.stats.total.used, 4280);
        assert_eq!(json.stats.total.soft_lim, 0);
        assert_eq!(json.stats.total.hard_lim, 0);

        // Check if the area request option was correctly deserialized.
        assert!(json.stats.area);

        // Check if the model set was correctly deserialized.
        assert_eq!(json.stats.models[0], "all_minus_euro1k");

        // Check if the contact was correctly deserialized.
        assert_eq!(json.stats.contact[0], "rustythecrab@meteomatics.com");
    }
}