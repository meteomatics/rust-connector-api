use chrono::{Utc, DateTime};
use rust_connector_api::APIClient;
use rust_connector_api::location::BBox;
use rust_connector_api::errors::ConnectorError;
use polars::prelude::*;

#[tokio::main]
async fn main(){
    // Credentials  
    let u_name: String = String::from("NA");
    let u_pw: String = String::from("NA");
    // let u_name: String = String::from("python-community");
    // let u_pw: String = String::from("Umivipawe179");

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

/// Query a time series for a single point and two parameters.
async fn example_request(api: &APIClient) -> std::result::Result<DataFrame, ConnectorError>{
    // Time series definition
    let start_date: DateTime<Utc> = Utc::now();

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
    let param1: String = String::from("t_2m:C");
    let param2: String = String::from("precip_1h:mm");
    let params: Vec<String> = vec![param1, param2];

    // Optionals
    let opt1: String = String::from("model=mix");
    let optionals: Option<Vec<String>> = Option::from(vec![opt1]);
    
    let result = api.query_grid_unpivoted(
        &start_date, &params, &ch, &optionals
    ).await;

    result
}
