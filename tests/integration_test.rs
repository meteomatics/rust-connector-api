use rust_connector_api::APIClient;
use chrono::{Duration, Utc, prelude::*};
use dotenv::dotenv;
use std::env;
use rust_connector_api::location::{Point, BBox};
use polars::prelude::*;
use std::io::Cursor;
use std::fs;
use std::path::Path;
use netcdf::*;

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
    52.52,13.405,1989-11-09T18:00:00Z,6.8
    52.52,13.405,1989-11-10T06:00:00Z,1.4
    52.52,13.405,1989-11-10T18:00:00Z,5.3
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
    let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let end_date = start_date + Duration::days(1);
    let interval = Duration::hours(12);

    // Create Parameters
    let mut parameters = Vec::new();
    parameters.push(String::from("t_2m:C"));

    // Create Locations
    let p1: Point = Point { lat: 52.52, lon: 13.405};
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
    52.52,13.405,1989-11-09T18:00:00Z,6.8,0.06
    52.52,13.405,1989-11-10T06:00:00Z,1.4,0.0
    52.52,13.405,1989-11-10T18:00:00Z,5.3,0.0
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
    let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let end_date = start_date + Duration::days(1);
    let interval = Duration::hours(12);

    // Create Parameters
    let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm")];

    // Create Locations
    let p1: Point = Point { lat: 52.52, lon: 13.405};
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
    52.5,13.4,1989-11-09T18:00:00Z,6.8,0.05
    52.5,13.4,1989-11-10T06:00:00Z,1.4,0.0
    52.5,13.4,1989-11-10T18:00:00Z,5.4,0.0
    52.4,13.5,1989-11-09T18:00:00Z,6.9,0.0
    52.4,13.5,1989-11-10T06:00:00Z,1.5,0.0
    52.4,13.5,1989-11-10T18:00:00Z,5.3,0.0
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
    let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let end_date = start_date + Duration::days(1);
    let interval = Duration::hours(12);

    // Create Parameters
    let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm")];

    // Create Locations
    let p1: Point = Point { lat: 52.50, lon: 13.40};
    let p2: Point = Point { lat: 52.40, lon: 13.50};
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
    postal_CH9000,1989-11-09T18:00:00Z,4.6
    postal_CH9000,1989-11-10T06:00:00Z,0.9
    postal_CH9000,1989-11-10T18:00:00Z,3.1
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
    let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let end_date = start_date + Duration::days(1);
    let interval = Duration::hours(12);

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
    postal_CH8000,1989-11-09T18:00:00Z,5.8,0.0
    postal_CH8000,1989-11-10T06:00:00Z,3.1,0.0
    postal_CH8000,1989-11-10T18:00:00Z,5.5,0.0
    postal_CH9000,1989-11-09T18:00:00Z,4.6,0.0
    postal_CH9000,1989-11-10T06:00:00Z,0.9,0.0
    postal_CH9000,1989-11-10T18:00:00Z,3.1,0.0
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
    let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let end_date = start_date + Duration::days(1);
    let interval = Duration::hours(12);

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

#[tokio::test]
async fn query_grid_pivoted() {
    // This is a bit hacked, because the python connetor changes 'data' to 'lat'
    let s = r#"data,13.4,13.45,13.5
    52.5,6.8,6.9,6.9
    52.45,6.8,6.8,6.9
    52.4,6.8,6.9,6.9
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
    let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);

    // Create Parameters
    let parameter = String::from("t_2m:C");

    // Create Location
    let bbox: BBox = BBox {
        lat_min: 52.40,
        lat_max: 52.50,
        lon_min: 13.40,
        lon_max: 13.50,
        lat_res: 0.05,
        lon_res: 0.05
    };

    // Call endpoint
    let df_q = meteomatics_connector
        .query_grid_pivoted(&start_date, &parameter, &bbox, &None)
        .await
        .unwrap();
    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn query_grid_unpivoted() {
    // directly downloaded from the API
    // https://api.meteomatics.com/1989-11-09T18:00:00.000Z/t_2m:C,precip_1h:mm/52.50,13.40_52.40,13.50:0.05,0.05/csv?model=mix
    let s = r#"lat;lon;validdate;t_2m:C;precip_1h:mm
    52.4;13.4;1989-11-09T18:00:00Z;6.8;0.00
    52.4;13.45;1989-11-09T18:00:00Z;6.9;0.00
    52.4;13.5;1989-11-09T18:00:00Z;6.9;0.00
    52.45;13.4;1989-11-09T18:00:00Z;6.8;0.00
    52.45;13.45;1989-11-09T18:00:00Z;6.8;0.00
    52.45;13.5;1989-11-09T18:00:00Z;6.9;0.00
    52.5;13.4;1989-11-09T18:00:00Z;6.8;0.05
    52.5;13.45;1989-11-09T18:00:00Z;6.9;0.00
    52.5;13.5;1989-11-09T18:00:00Z;6.9;0.00
    "#;
    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_delimiter(b';')
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
    let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);

    // Create Parameters
    let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm")];

    // Create Location
    let bbox: BBox = BBox {
        lat_min: 52.40,
        lat_max: 52.50,
        lon_min: 13.40,
        lon_max: 13.50,
        lat_res: 0.05,
        lon_res: 0.05
    };

    // Call endpoint
    let df_q = meteomatics_connector
        .query_grid_unpivoted(&start_date, &parameters, &bbox, &None)
        .await
        .unwrap();
    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn query_grid_unpivoted_time_series() {
    // directly downloaded from the API
    // https://api.meteomatics.com/1989-11-09T18:00:00.000Z--1989-11-10T18:00:00.000Z:PT12H/t_2m:C,precip_1h:mm/52.50,13.40_52.40,13.50:0.05,0.05/csv?model=mix
    let s = r#"lat;lon;validdate;t_2m:C;precip_1h:mm
    52.4;13.4;1989-11-09T18:00:00Z;6.8;0.00
    52.4;13.4;1989-11-10T06:00:00Z;1.5;0.00
    52.4;13.4;1989-11-10T18:00:00Z;5.4;0.00
    52.4;13.45;1989-11-09T18:00:00Z;6.9;0.00
    52.4;13.45;1989-11-10T06:00:00Z;1.5;0.00
    52.4;13.45;1989-11-10T18:00:00Z;5.3;0.00
    52.4;13.5;1989-11-09T18:00:00Z;6.9;0.00
    52.4;13.5;1989-11-10T06:00:00Z;1.5;0.00
    52.4;13.5;1989-11-10T18:00:00Z;5.3;0.00
    52.45;13.4;1989-11-09T18:00:00Z;6.8;0.00
    52.45;13.4;1989-11-10T06:00:00Z;1.4;0.00
    52.45;13.4;1989-11-10T18:00:00Z;5.4;0.00
    52.45;13.45;1989-11-09T18:00:00Z;6.8;0.00
    52.45;13.45;1989-11-10T06:00:00Z;1.4;0.00
    52.45;13.45;1989-11-10T18:00:00Z;5.3;0.00
    52.45;13.5;1989-11-09T18:00:00Z;6.9;0.00
    52.45;13.5;1989-11-10T06:00:00Z;1.5;0.00
    52.45;13.5;1989-11-10T18:00:00Z;5.3;0.00
    52.5;13.4;1989-11-09T18:00:00Z;6.8;0.05
    52.5;13.4;1989-11-10T06:00:00Z;1.4;0.00
    52.5;13.4;1989-11-10T18:00:00Z;5.4;0.00
    52.5;13.45;1989-11-09T18:00:00Z;6.9;0.00
    52.5;13.45;1989-11-10T06:00:00Z;1.4;0.00
    52.5;13.45;1989-11-10T18:00:00Z;5.3;0.00
    52.5;13.5;1989-11-09T18:00:00Z;6.9;0.00
    52.5;13.5;1989-11-10T06:00:00Z;1.4;0.00
    52.5;13.5;1989-11-10T18:00:00Z;5.3;0.00
    "#;
    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_delimiter(b';')
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
    // 1989-11-09 19:00:00 --> 18:00:00 UTC
    let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let end_date = start_date + Duration::days(1);
    let interval = Duration::hours(12);

    // Create Parameters
    let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm")];

    // Create Location
    let bbox: BBox = BBox {
        lat_min: 52.40,
        lat_max: 52.50,
        lon_min: 13.40,
        lon_max: 13.50,
        lat_res: 0.05,
        lon_res: 0.05
    };

    // Call endpoint
    let df_q = meteomatics_connector
        .query_grid_unpivoted_time_series(
            &start_date, &end_date, &interval, &parameters, &bbox, &None
        )
        .await
        .unwrap();
    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn query_netcdf() {
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
    // 1989-11-09 19:00:00 --> 18:00:00 UTC
    let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let end_date = start_date + Duration::days(1);
    let interval = Duration::hours(12);

    // Create Parameters
    let parameter =String::from("t_2m:C");

    // Create Location
    let bbox: BBox = BBox {
        lat_min: 52.40,
        lat_max: 52.50,
        lon_min: 13.40,
        lon_max: 13.50,
        lat_res: 0.05,
        lon_res: 0.05
    };

    // Create file name
    let file_name: String = String::from("tests/netcdf/my_netcdf.nc");

    // Call endpoint
    meteomatics_connector
        .query_netcdf(
            &start_date, &end_date, &interval, &parameter, &bbox, &file_name, &None
        )
        .await
        .unwrap();

    // Make some tests
    let file = netcdf::open(&file_name).unwrap();
    let var = &file.variable("t_2m").expect("Could not find variable 't_2m");

    // Check value: ds_rust["t_2m"].data[0,0,0]
    let xr_test: f64 = 6.81058931350708;
    let temp_f64: f64 = var.value(Some(&[0,0,0])).unwrap();
    assert_eq!(xr_test, temp_f64);

    // Check another value: ds_rust["t_2m"].data[2,2,2]
    let xr_test: f64 = 5.269172668457031;
    let temp_f64: f64 = var.value(Some(&[2,2,2])).unwrap();
    assert_eq!(xr_test, temp_f64);

    // Remove the file    
    let dir: &Path = Path::new(&file_name).parent().unwrap();
    fs::remove_file(&file_name).unwrap();
    fs::remove_dir_all(&dir).unwrap();
}