//! # Time series
//! 
//! Time series queries allow you to request weather and climate data for specific places on the globe 
//! defined by their latitude and longitude coordinates in a time series. You can query any number of
//! points and any number of parameters. The respective method is [`rust_connector_api::APIClient::query_time_series`].
//! 
//! # The Example
//! The example demonstrates how to request temperature and precipitation data for every hour between
//! "now" and "tomorrow" for two places. The two places are Geneva (<https://www.geneve.ch/en>) and 
//! Zermatt (<https://www.zermatt.ch/en>). There are several optional parameters you can pass to the
//! meteomatics API that will change the data you get back. In the example we specify that we would 
//! like to receive the parameters based on the mix ```model = String::from("model=mix");```. The 
//! Meteomatics Mix combines different models and sources into an intelligent blend, such that the 
//! best data source is chosen for each time and location. 
//! (<https://www.meteomatics.com/en/api/request/optional-parameters/data-source/>)
//! 
//! # The account
//! You can use the provided credentials or your own if you already have them. 
//! Check out <https://www.meteomatics.com/en/request-business-wather-api-package/> to request an 
//! API package.

use chrono::{Utc, Duration};
use rust_connector_api::{APIClient, Point, TimeSeries};
use rust_connector_api::errors::ConnectorError;
use polars::prelude::*;

#[tokio::main]
async fn main(){
    // Credentials
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
    let geneva = Point { lat: 46.210565, lon: 6.143981};
    let zermatt = Point { lat: 46.026331, lon: 7.748809 };
    let coords = vec![geneva, zermatt];

    // Parameter selection
    let t_2m = String::from("t_2m:C");
    let precip_1h = String::from("precip_1h:mm");
    let params = vec![t_2m, precip_1h];

    // Optionals
    let model = String::from("model=mix");
    let optionals = Option::from(vec![model]);
    
    let result = api.query_time_series(&time_seris, &params, &coords, &optionals).await;

    result
}
