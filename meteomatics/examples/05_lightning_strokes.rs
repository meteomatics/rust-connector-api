//! # Query Lightnings
//! Lightning queries allow you to for reports of lightning in an area (defined by a bounding box) in 
//! a certain time frame. The bounding box or [`BBox`](`rust_connector_api::BBox`) is
//! defined by the upper left (i.e. North Western) corner and lower right(i.e. South Eastern) corner). 
//! For this query we don't need to define a latidue and longitude resolution (just set it to 0.0).
//! 
//!# The Example
//! The example demonstrates how to request lightning event reports for Switzerland for the past 24 hours.
//! 
//! # The account
//! You can use the provided credentials or your own if you already have them. 
//! Check out <https://www.meteomatics.com/en/request-business-wather-api-package/> to request an 
//! API package.


use chrono::{Utc, Duration};
use meteomatics::{APIClient, BBox, TimeSeries};
use meteomatics::errors::ConnectorError;
use polars::prelude::*;

// Demonstrates how to use the rust connector to query the Meteomatics API for gridded data. Also 
// demonstrates how to work with the resulting ```DataFrame```.
#[tokio::main]
async fn main(){
    // Create Client
    let api: APIClient = APIClient::new("rust-community", "5GhAwL3HCpFB", 10);

    let df_ts = example_request(&api).await.unwrap();

    // Print the query result
    println!("{:?}", df_ts);

}

/// Demonstrates how to query a time series for a single point in time (now), a grid and two parameters.
async fn example_request(api: &APIClient) -> std::result::Result<DataFrame, ConnectorError>{
    // Time series definition
    let dt_end = Utc::now();
    let time_series = TimeSeries{
        start: dt_end - Duration::days(1),
        end: dt_end,
        timedelta: Option::from(Duration::hours(1))
    };

    // Location definition
    let ch: BBox = BBox {
        lat_min: 45.8,
        lat_max: 47.8,
        lon_min: 6.0,
        lon_max: 10.5,
        lat_res: 0.0,
        lon_res: 0.0,
    };
    
    let result = api.query_lightning(&time_series, &ch).await;

    result
}