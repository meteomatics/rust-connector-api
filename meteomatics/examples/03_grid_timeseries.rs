//! # Time Series Grid Query 
//! Grid queries can also be executed for a time series. The detials for the grid are demonstrated 
//! in the examples 01 and 02. 
//! 
//!# The Example
//! The example demonstrates how to request an hourly temperature and precipitation forecast between 
//! now and a day from now for Switzerland. The grid is spaced in 0.1 ° (or about 7 km cell width and cell height). 
//! There are several optional parameters you can pass to the meteomatics API that will change the 
//! data you get back. In the example we specify that we would like to receive the parameters based 
//! on the mix ```model = String::from("model=mix");```. The Meteomatics Mix combines different model
//! s and sources into an intelligent blend, such that the best data source is chosen for each time 
//! and location (<https://www.meteomatics.com/en/api/request/optional-parameters/data-source/>).
//! 
//! # The account
//! You can use the provided credentials or your own if you already have them. 
//! Check out <https://www.meteomatics.com/en/request-business-wather-api-package/> to request an 
//! API package.

use chrono::{Utc, Duration};
use rust_connector_api::{APIClient, BBox, TimeSeries};
use rust_connector_api::errors::ConnectorError;
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

    // Do some calculations
    for col in vec!["t_2m:C", "precip_1h:mm"] {
        let mean: f64 = df_ts[col].mean().unwrap();
        let max: f64 = df_ts[col].max().unwrap();
        let min: f64 = df_ts[col].min().unwrap();
        println!("{} statistics: mean = {}; max = {}, min = {} in the last 24 h.", col, mean, max, min);
    }

    // Do some groupby calculations
    for col in vec!["t_2m:C", "precip_1h:mm"] {
        let lat_means = df_ts.groupby(["lat"]).unwrap().select(&[col]).mean().unwrap();
        let lon_means = df_ts.groupby(["lon"]).unwrap().select(&[col]).mean().unwrap();
        println!("{:?}", lat_means);
        println!("{:?}", lon_means);
    }
}

/// Demonstrates how to query a time series for a single point in time (now), a grid and two parameters.
async fn example_request(api: &APIClient) -> std::result::Result<DataFrame, ConnectorError>{
    // Time series definition
    let dt_start = Utc::now();
    let time_series = TimeSeries{
        start: dt_start,
        end: dt_start + Duration::days(1),
        timedelta: Option::from(Duration::hours(1))
    };

    // Location definition
    let ch: BBox = BBox {
        lat_min: 45.8,
        lat_max: 47.8,
        lon_min: 6.0,
        lon_max: 10.5,
        lat_res: 0.1,
        lon_res: 0.1,
    };

    // Parameter selection
    let temp2m = String::from("t_2m:C");
    let precip1h = String::from("precip_1h:mm");
    let params = vec![temp2m, precip1h];

    // Optionals
    let model_mix = String::from("model=mix");
    let optionals = Option::from(vec![model_mix]);
    
    let result = api.query_grid_unpivoted_time_series(&time_series, &params, &ch, &optionals).await;

    result
}
