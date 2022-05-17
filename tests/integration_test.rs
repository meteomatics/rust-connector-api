use rust_connector_api::APIClient;
use chrono::{Duration, Utc, prelude::*};
use dotenv::dotenv;
use std::env;

// Unit testing section
// TODO: Create more meaningful tests
// TODO: Add option to query for a grid
// TODO: Add option to query for a grid timeseries
// TODO: Add more meaningful errors. 
#[tokio::test]
async fn call_query_time_series_with_options() {
    println!("\n##### call_query_time_series_with_options:");

    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();

    // Create API connector
    let meteomatics_connector = APIClient::new(
        api_user,
        api_key,
        10,
    );

    // Create time information
    let start_date = Utc.ymd(2022, 5, 17).and_hms(12, 00, 00);
    let end_date = start_date + Duration::days(1);
    let interval = Duration::hours(1);

    // Create Parameters
    let mut parameters = Vec::new();
    parameters.push(String::from("t_2m:C"));
    parameters.push(String::from("precip_1h:mm"));

    // Create Locations
    let coordinates: Vec<Vec<f64>> = vec![vec![46.685, 7.953], vec![46.759, -76.027]];

    // Create Optionals
    let mut optionals = Vec::new();
    optionals.push(String::from("source=mix"));
    optionals.push(String::from("calibrated=true"));

    // Call endpoint
    let result = meteomatics_connector
        .query_time_series(
            &start_date, &end_date, &interval, &parameters, &coordinates, &Option::from(optionals)
        )
        .await;

    match result {
        Ok(ref df) => {
            println!("{:?}", df);
        }
        Err(ref connector_error) => {
            println!(">>>>>>>>>> ConnectorError: {:#?}", connector_error);
            assert!(result.is_err());
        }
    }

    assert!(result.is_ok());
}

#[tokio::test]
async fn call_query_time_series_without_options() {
    println!("\n##### call_query_time_series_without_options:");

    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        api_user,
        api_key,
        10,
    );

    // Create time information
    let start_date = Utc.ymd(2022, 5, 17).and_hms(12, 00, 00);
    let end_date = start_date + Duration::days(1);
    let interval = Duration::hours(1);

    // Create Parameters
    let mut parameters = Vec::new();
    parameters.push(String::from("t_2m:C"));
    parameters.push(String::from("precip_1h:mm"));

    // Create Locations
    let coords: Vec<Vec<f64>> = vec![vec![47.419708, 9.358478]];

    // Call endpoint
    let result = meteomatics_connector
        .query_time_series(&start_date, &end_date, &interval, &parameters, &coords, &None)
        .await;

    match result {
        Ok(ref df) => {
           println!("{:?}", df);
        }
        Err(ref connector_error) => {
            println!(">>>>>>>>>> ConnectorError: {:#?}", connector_error);
            assert!(result.is_err());
        }
    }

    assert!(result.is_ok());
}