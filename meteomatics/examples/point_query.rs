//! # Point Query 
//! This is a demonstration of how to download a DataFrame for one or many locations and one or many
//! points in time (i.e. a time series). The locations are either defined as "postal_codes" or as
//! ```Point``` objects. Point objects contain the latitude and longitude coordinates of a location.
//! To run the example either change ```u_name``` and ```u_pw```
//! directly *or* create a file called ```.env``` and put the following lines in there:
//! ```text
//! METEOMATICS_PW=your_password
//! METEOMATICS_USER=your_username
//! ```
//! Make sure to include ```.env``` in your ```.gitignore```. This is a safer variant for developers 
//! to work with API credentials as you will never accidentally commit/push your credentials.

use chrono::{Utc, Duration};
use rust_connector_api::{APIClient, Point, TimeSeries};
use rust_connector_api::errors::ConnectorError;
use polars::prelude::*;
use std::env;
use dotenv::dotenv;

#[tokio::main]
async fn main(){
    // Credentials
    dotenv().ok();
    let u_pw: String = env::var("METEOMATICS_PW").unwrap();
    let u_name: String = env::var("METEOMATICS_USER").unwrap();

    // Create Client
    let api: APIClient = APIClient::new(&u_name, &u_pw, 10);
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
   
}

/// Query a time series for a single point and two parameters.
async fn example_request(api: &APIClient) -> std::result::Result<DataFrame, ConnectorError>{
    // Time series definition
    let start_date = Utc::now();
    let time_seris = TimeSeries {
        start: start_date,
        end: start_date + Duration::days(1),
        timedelta: Option::from(Duration::hours(1))
    };

    // Location definition
    let p1 = Point { lat: 47.249297, lon: 9.342854 };
    let p2 = Point { lat: 50.0, lon: 10.0 };
    let coords = vec![p1, p2];

    // Parameter selection
    let t_2m = String::from("t_2m:C");
    let precip_1h = String::from("precip_1h:mm");
    let params = vec![t_2m, precip_1h];

    // Optionals
    let model_mix = String::from("model=mix");
    let optionals = Option::from(vec![model_mix]);
    
    let result = api.query_time_series(&time_seris, &params, &coords, &optionals).await;

    result
}
