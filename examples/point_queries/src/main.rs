use chrono::{Utc, DateTime, Duration};
use rust_connector_api::APIClient;
use rust_connector_api::location::Point;
use rust_connector_api::errors::ConnectorError;
use polars::prelude::*;

#[tokio::main]
async fn main(){
    // Credentials
    let u_name: String = String::from("python-community");
    let u_pw: String = String::from("Umivipawe179");

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
   
}

/// Query a time series for a single point and two parameters.
async fn example_request(api: &APIClient) -> std::result::Result<DataFrame, ConnectorError>{
    // Time series definition
    let start_date: DateTime<Utc> = Utc::now();
    let end_date: DateTime<Utc> = start_date + Duration::days(1);
    let interval: Duration = Duration::hours(1);

    // Location definition
    let p1: Point = Point { lat: 47.249297, lon: 9.342854 };
    let p2: Point = Point { lat: 50.0, lon: 10.0 };
    let coords: Vec<Point> = vec![p1, p2];

    // Parameter selection
    let param1: String = String::from("t_2m:C");
    let param2: String = String::from("precip_1h:mm");
    let params: Vec<String> = vec![param1, param2];

    // Optionals
    let opt1: String = String::from("model=mix");
    let optionals: Option<Vec<String>> = Option::from(vec![opt1]);
    
    let result = api.query_time_series(
        &start_date, &end_date, &interval, &params, &coords, &optionals
    ).await;

    result
}
