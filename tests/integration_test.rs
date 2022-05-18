use rust_connector_api::APIClient;
use chrono::{Duration, Utc, prelude::*};
use dotenv::dotenv;
use std::env;
use rust_connector_api::location::Point;
use polars::prelude::*;
use std::io::Cursor;

// Unit testing section
// TODO: Add option to query for a grid
// TODO: Add option to query for a grid timeseries
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
    let p1: Point = Point { lat: 52.520551, lon: 13.461804};
    let p2: Point = Point { lat: -52.520551, lon: 13.461804};
    let coords: Vec<Point> = vec![p1, p2];

    // Create Optionals
    let mut optionals = Vec::new();
    optionals.push(String::from("source=mix"));
    optionals.push(String::from("calibrated=true"));

    // Call endpoint
    let result = meteomatics_connector
        .query_time_series(
            &start_date, &end_date, &interval, &parameters, &coords, &Option::from(optionals)
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
    let p1: Point = Point { lat: 52.520551, lon: 13.461804};
    let p2: Point = Point { lat: -52.520551, lon: 13.461804};
    let coords: Vec<Point> = vec![p1, p2];

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

#[tokio::test]
async fn query_time_series_one_point_one_param() {
    // Reference data from python connector
    let s = r#"lat,lon,validdate,t_2m:C
    47.11,11.47,2022-05-18T00:00:00Z,11.5
    47.11,11.47,2022-05-18T06:00:00Z,10.3
    47.11,11.47,2022-05-18T12:00:00Z,17.9
    47.11,11.47,2022-05-18T18:00:00Z,15.7
    47.11,11.47,2022-05-19T00:00:00Z,13.3
    "#;
    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_ignore_parser_errors(true)
        .finish()
        .unwrap();

    // Query using rust connector
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
    // 2022-05-18 00:00:00+00:00
    let start_date = Utc.ymd(2022, 5, 18).and_hms_micro(0, 0, 0, 0);
    let end_date = start_date + Duration::days(1);
    let interval = Duration::hours(6);

    // Create Parameters
    let mut parameters = Vec::new();
    parameters.push(String::from("t_2m:C"));

    // Create Locations
    let p1: Point = Point { lat: 47.11, lon: 11.47};
    let coords: Vec<Point> = vec![p1];

    // Call endpoint
    let df_q = meteomatics_connector
        .query_time_series(&start_date, &end_date, &interval, &parameters, &coords, &None)
        .await
        .unwrap();
    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn query_time_series_one_point_two_param() {
    // Reference data from python connector
    let s = r#"lat,lon,validdate,t_2m:C,precip_1h:mm
    47.11,11.47,2022-05-18T00:00:00Z,11.5,0.000
    47.11,11.47,2022-05-18T06:00:00Z,10.3,0.000
    47.11,11.47,2022-05-18T12:00:00Z,17.9,0.000
    47.11,11.47,2022-05-18T18:00:00Z,15.7,0.000
    47.11,11.47,2022-05-19T00:00:00Z,13.3,0.000
    "#;
    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_ignore_parser_errors(true)
        .finish()
        .unwrap();

    // Query using rust connector
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
    // 2022-05-18 00:00:00+00:00
    let start_date = Utc.ymd(2022, 5, 18).and_hms_micro(0, 0, 0, 0);
    let end_date = start_date + Duration::days(1);
    let interval = Duration::hours(6);

    // Create Parameters
    let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm")];

    // Create Locations
    let p1: Point = Point { lat: 47.11, lon: 11.47};
    let coords: Vec<Point> = vec![p1];

    // Call endpoint
    let df_q = meteomatics_connector
        .query_time_series(&start_date, &end_date, &interval, &parameters, &coords, &None)
        .await
        .unwrap();
    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn query_time_series_two_point_two_param() {
    // Reference data from python connector
    let s = r#"lat,lon,validdate,t_2m:C,precip_1h:mm
    47.11,11.47,2022-05-18T00:00:00Z,11.5,0.0
    47.11,11.47,2022-05-18T06:00:00Z,10.3,0.0
    47.11,11.47,2022-05-18T12:00:00Z,17.9,0.0
    47.11,11.47,2022-05-18T18:00:00Z,15.7,0.0
    47.11,11.47,2022-05-19T00:00:00Z,13.3,0.0
    46.11,10.47,2022-05-18T00:00:00Z,6.7,0.0
    46.11,10.47,2022-05-18T06:00:00Z,7.4,0.0
    46.11,10.47,2022-05-18T12:00:00Z,10.6,0.0
    46.11,10.47,2022-05-18T18:00:00Z,7.9,0.0
    46.11,10.47,2022-05-19T00:00:00Z,7.2,0.0
    "#;
    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_ignore_parser_errors(true)
        .finish()
        .unwrap();

    // Query using rust connector
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
    // 2022-05-18 00:00:00+00:00
    let start_date = Utc.ymd(2022, 5, 18).and_hms_micro(0, 0, 0, 0);
    let end_date = start_date + Duration::days(1);
    let interval = Duration::hours(6);

    // Create Parameters
    let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm")];

    // Create Locations
    let p1: Point = Point { lat: 47.11, lon: 11.47};
    let p2: Point = Point { lat: 46.11, lon: 10.47};
    let coords: Vec<Point> = vec![p1, p2];

    // Call endpoint
    let df_q = meteomatics_connector
        .query_time_series(&start_date, &end_date, &interval, &parameters, &coords, &None)
        .await
        .unwrap();
    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn query_time_series_one_postal_one_param() {
    // Reference data from python connector
    let s = r#"station_id,validdate,t_2m:C
    postal_CH9000,2022-05-18T00:00:00Z,13.6
    postal_CH9000,2022-05-18T06:00:00Z,14.0
    postal_CH9000,2022-05-18T12:00:00Z,21.4
    postal_CH9000,2022-05-18T18:00:00Z,20.3
    postal_CH9000,2022-05-19T00:00:00Z,14.2
    "#;
    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_ignore_parser_errors(true)
        .finish()
        .unwrap();

    // Query using rust connector
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
    // 2022-05-18 00:00:00+00:00
    let start_date = Utc.ymd(2022, 5, 18).and_hms_micro(0, 0, 0, 0);
    let end_date = start_date + Duration::days(1);
    let interval = Duration::hours(6);

    // Create Parameters
    let parameters = vec![String::from("t_2m:C")];

    // Create Locations
    let postal1: Vec<String> = vec![String::from("postal_CH9000")];

    // Call endpoint
    let df_q = meteomatics_connector
        .query_time_series_postal(&start_date, &end_date, &interval, &parameters, &postal1, &None)
        .await
        .unwrap();
    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn query_time_series_two_postal_two_param() {
    // Reference data from python connector
    let s = r#"station_id,validdate,t_2m:C,precip_1h:mm
    postal_CH8000,2022-05-18T00:00:00Z,15.2,0.0
    postal_CH8000,2022-05-18T06:00:00Z,15.6,0.0
    postal_CH8000,2022-05-18T12:00:00Z,25.0,0.0
    postal_CH8000,2022-05-18T18:00:00Z,23.5,0.0
    postal_CH8000,2022-05-19T00:00:00Z,15.1,0.0
    postal_CH9000,2022-05-18T00:00:00Z,13.6,0.0
    postal_CH9000,2022-05-18T06:00:00Z,14.0,0.0
    postal_CH9000,2022-05-18T12:00:00Z,21.4,0.0
    postal_CH9000,2022-05-18T18:00:00Z,20.3,0.0
    postal_CH9000,2022-05-19T00:00:00Z,14.2,0.0
    "#;
    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_ignore_parser_errors(true)
        .finish()
        .unwrap();

    // Query using rust connector
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
    // 2022-05-18 00:00:00+00:00
    let start_date = Utc.ymd(2022, 5, 18).and_hms_micro(0, 0, 0, 0);
    let end_date = start_date + Duration::days(1);
    let interval = Duration::hours(6);

    // Create Parameters
    let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm")];

    // Create Locations
    let postal1: Vec<String> = vec![String::from("postal_CH8000"), String::from("postal_CH9000")];

    // Call endpoint
    let df_q = meteomatics_connector
        .query_time_series_postal(&start_date, &end_date, &interval, &parameters, &postal1, &None)
        .await
        .unwrap();
    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}