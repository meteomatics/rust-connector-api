//! # Rust Meteomatics API connector: *<small>rust-connector-api<small>*
//! 
//! ```rust-connector-api``` is a native rust library that presents an easy interface for various types
//! of queries to the Meteomatics weather and climate API (<https://www.meteomatics.com/en/>). The key
//! functionality is based around the [`APIClient`] together with abstractions for location ([`Point`])
//! and grid ([`BBox`]) information. The [`APIClient`] exposes functions that allow to asynchronously
//! request data from the API. The functions usually require some information about the time and place
//! of the desired information. Based on the given information the client then builds the relevant 
//! query URL and handles the HTTP response appropriately. 
//! 
//! 
//! ## Polars
//! 
//! ```text
//! Polars is a DataFrame library for Rust. It is based on Apache Arrow’s memory model. Apache arrow
//! provides very cache efficient columnar data structures and is becoming the defacto standard for 
//! columnar data.
//! ```
//! Often times the HTTP response from the API can be converted to a polars [`DataFrame`]. These DataFrames
//! allow fast and efficient data access and modification (see the examples for more details on this).
//! For more information on polars itself please check <https://docs.rs/polars/latest/polars/index.html>. 
//! 
//! 
//! ## Chrono
//! 
//! Time is represented using the [`chrono`] library's [`DateTime`] and [`Duration`]
//! features in the [`Utc`] timezone. This means that you either convert the requested time from your 
//! local time to Utc using [`Local`] and [`DateTime::from_utc()`] or that you directly 
//! create the time information using ```Utc.ymd().and_hms_micro()```. More information about chrono
//! can be found here <https://docs.rs/chrono/latest/chrono/index.html>.
//! 
//! ```rust, no_run
//! use chrono::{DateTime, Local, Utc, TimeZone};
//! 
//! // This is a DateTime represented in the Local time zone.
//! let dt_local = Local.ymd(2014, 7, 8).and_hms_micro(15, 0, 0, 0);
//! // This creates a DateTime in the Utc time zone.
//! let dt_utc = DateTime::<Utc>::from_utc(dt_local.naive_utc(), Utc);
//! println!("Local: {} and corresponding Utc: {}.", dt_local.to_rfc3339(), dt_utc.to_rfc3339());
//! ```
//! 
//! ## Overview
//! 
//! The example below illustrates how the information for the APIClient can be created using the ```Point```
//! abstraction together with information about the time and parameter (temperature 2 m above the ground). 
//! The time series runs from 1989-11-09T18:00:00.0Z to 1989-11-10T18:00:00.0Z in 12 hour steps. We can
//! therefore expect temperature values for three points in time and a single point in space (lat: 52.52, 
//! lon: 13.405).
//! 
//! ### Parameters
//! 
//! The Meteomatics API provides an extensive list of weather parameters to chose from. A list of all
//! available parameters can be found here <https://www.meteomatics.com/en/api/available-parameters/alphabetic-list/>.
//!
//! ```rust, no_run
//! use rust_connector_api::{APIClient, Point, TimeSeries};
//! use chrono::{Duration, Utc, TimeZone};
//! 
//! #[tokio::main]
//! async fn main(){
//!     // Credentials
//!     let api_key = "my_password";
//!     let api_user = "my_username";
//! 
//!     // Create client
//!     let client = APIClient::new(
//!         api_user,
//!         api_key,
//!         10, // < HTTP request timeout
//!     );
//! 
//!     // Time series specification
//!     let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
//!     let time_series = TimeSeries {
//!         start: start_date,
//!         end: start_date + Duration::days(1),
//!         timedelta: Option::from(Duration::hours(12))
//!     };
//!
//!     // Parameter selection
//!     let param = vec![String::from("t_2m:C")];
//!
//!     // Location selection
//!     let coords = vec![Point { lat: 52.52, lon: 13.405 }];
//!
//!     // Use API 
//!     let df = client
//!         .query_time_series(&time_series, &param, &coords, &None)
//!         .await
//!         .unwrap();
//! 
//!     println!("{:?}", df);
//! }
//! ```
//! ```text
//! ┌───────┬────────┬──────────────────────┬────────┐
//! │ lat   ┆ lon    ┆ validdate            ┆ t_2m:C │
//! │ ---   ┆ ---    ┆ ---                  ┆ ---    │
//! │ f64   ┆ f64    ┆ str                  ┆ f64    │
//! ╞═══════╪════════╪══════════════════════╪════════╡
//! │ 52.52 ┆ 13.405 ┆ 1989-11-09T18:00:00Z ┆ 6.8    │
//! ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
//! │ 52.52 ┆ 13.405 ┆ 1989-11-10T06:00:00Z ┆ 1.4    │
//! ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
//! │ 52.52 ┆ 13.405 ┆ 1989-11-10T18:00:00Z ┆ 5.3    │
//! └───────┴────────┴──────────────────────┴────────┘
//! ```

pub mod errors;
pub mod client;
pub mod location;
pub mod util;
pub use client::APIClient;
pub use location::Point;
pub use location::BBox;
pub use util::TimeSeries;
pub use chrono::{Duration, DateTime, Local, Utc};
pub use polars::frame::DataFrame;