//! # Rust Meteomatics API connector: *<small>rust-connector-api<small>*
//! 
//! ```rust-connector-api``` is a native rust application that lets users connect to the Meteomatics
//! weather and climate API (<https://www.meteomatics.com/en/>) to fetch data. Currently the App supports
//! the download of point and multi-point time series as well as grid downloads. The downloaded data
//! is returned in the format of ```polars``` DataFrames (<https://docs.rs/polars/latest/polars/index.html>).
//!
//! ## Overview
//! Users familiar with the Pandas ecosystem for Python will be up and running with Polars DataFrames
//! in no time. 
//!
//! ```ignore
//! // Define a time series
//! let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
//! let end_date = start_date + Duration::days(1);
//! let interval = Duration::hours(12);
//!
//! // Specify a parameter
//! let param = vec![String::from("t_2m:C")]
//!
//!    // Specify a location
//!    let coords: Vec<Point> = vec![Point { lat: 52.52, lon: 13.405 }];
//!
//! // Call endpoint
//! let df = meteomatics_connector
//!     .query_time_series(&start_date, &end_date, &interval, &parameters, &coords, &None)
//!     .await
//!     .unwrap();
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

// TODO: check where to pass references and where to pass ownership