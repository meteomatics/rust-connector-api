//! # Grid Query 
//! This is a demonstration of how to download a DataFrame for an area or "grid" defined by a bounding-box 
//! and a grid resolution (e.g. 0.1 °). 
//! To run the example either change ```u_name``` and ```u_pw```
//! directly *or* create a file called ```.env``` and put the following lines in there:
//! ```text
//! METEOMATICS_PW=your_password
//! METEOMATICS_USER=your_username
//! ```
//! Make sure to include ```.env``` in your ```.gitignore```. This is a safer variant for developers 
//! to work with API credentials as you will never accidentally commit/push your credentials.

use chrono::{Utc};
use rust_connector_api::APIClient;
use rust_connector_api::location::BBox;
use rust_connector_api::errors::ConnectorError;
use polars::prelude::*;
use std::env;
use dotenv::dotenv;

// Demonstrates how to use the rust connector to query the Meteomatics API for gridded data. Also 
// demonstrates how to work with the resulting ```DataFrame```.
#[tokio::main]
async fn main(){
    // Credentials
    dotenv().ok();
    let u_pw: String = env::var("METEOMATICS_PW").unwrap();
    let u_name: String = env::var("METEOMATICS_USER").unwrap();

    // Create Client
    let api: APIClient = APIClient::new(u_name,u_pw,10);
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
    let start_date = Utc::now();

    // Location definition
    let ch: BBox = BBox {
        lat_min: 45.8179716,
        lat_max: 47.8084648,
        lon_min: 5.9559113,
        lon_max: 10.4922941,
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
    
    let result = api.query_grid_unpivoted(
        &start_date, &params, &ch, &optionals
    ).await;

    result
}
