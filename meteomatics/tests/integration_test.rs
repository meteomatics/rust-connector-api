use rust_connector_api::APIClient;
use chrono::{Duration, Utc, prelude::*};
use dotenv::dotenv;
use std::env;
use rust_connector_api::location::{Point, BBox};
use polars::prelude::*;
use std::io::Cursor;
use std::fs;
use std::path::Path;

// Unit testing section
#[tokio::test]
async fn call_query_time_series_with_options() {
    let s = r#"lat;lon;validdate;t_2m:C;precip_1h:mm
    52.520551;13.461804;1989-11-09T18:00:00Z;7.8;0.00
    52.520551;13.461804;1989-11-10T06:00:00Z;3.2;0.00
    52.520551;13.461804;1989-11-10T18:00:00Z;7.6;0.00
    -52.520551;13.461804;1989-11-09T18:00:00Z;-1.7;0.00
    -52.520551;13.461804;1989-11-10T06:00:00Z;-2.2;0.00
    -52.520551;13.461804;1989-11-10T18:00:00Z;-1.4;0.00
    "#;

    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_ignore_parser_errors(true)
        .with_delimiter(b';')
        .finish()
        .unwrap();

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
    let df_q = meteomatics_connector
        .query_time_series(
            &start_date, &end_date, &interval, &parameters, &coords, &Option::from(optionals)
        )
        .await.unwrap();

    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn call_query_time_series_without_options() {
    let s = r#"lat;lon;validdate;t_2m:C;precip_1h:mm
    52.520551;13.461804;1989-11-09T18:00:00Z;6.8;0.00
    52.520551;13.461804;1989-11-10T06:00:00Z;1.4;0.00
    52.520551;13.461804;1989-11-10T18:00:00Z;5.2;0.00
    -52.520551;13.461804;1989-11-09T18:00:00Z;-1.7;0.00
    -52.520551;13.461804;1989-11-10T06:00:00Z;-2.2;0.00
    -52.520551;13.461804;1989-11-10T18:00:00Z;-1.4;0.00
    "#;

    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_ignore_parser_errors(true)
        .with_delimiter(b';')
        .finish()
        .unwrap();

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
    parameters.push(String::from("precip_1h:mm"));

    // Create Locations
    let p1: Point = Point { lat: 52.520551, lon: 13.461804};
    let p2: Point = Point { lat: -52.520551, lon: 13.461804};
    let coords: Vec<Point> = vec![p1, p2];

    // Call endpoint
    let df_q = meteomatics_connector
        .query_time_series(&start_date, &end_date, &interval, &parameters, &coords, &None)
        .await.unwrap();

    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
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
    let parameters = vec![String::from("t_2m:C")];

    // Create Locations
    let p1 = Point { lat: 52.52, lon: 13.405};
    let coords = vec![p1];

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
    let postal1 = vec![String::from("postal_CH8000"), String::from("postal_CH9000")];

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
    let bbox = BBox {
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
    let bbox = BBox {
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
    let bbox = BBox {
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
    let bbox = BBox {
        lat_min: 52.40,
        lat_max: 52.50,
        lon_min: 13.40,
        lon_max: 13.50,
        lat_res: 0.05,
        lon_res: 0.05
    };

    // Create file name
    let file_name = String::from("tests/netcdf/my_netcdf.nc");

    // Call endpoint
    meteomatics_connector
        .query_netcdf(
            &start_date, &end_date, &interval, &parameter, &bbox, &file_name, &None
        )
        .await
        .unwrap();

    // Make some tests
    let nc_file = netcdf::open(&file_name).unwrap();
    let temperature = &nc_file.variable("t_2m").expect("Could not find variable 't_2m");

    // Check value: ds_rust["t_2m"].data[0,0,0]
    let temp_val_ref: f64 = 6.81058931350708; // extracted in Python 
    let temp_val_here: f64 = temperature.value(Some(&[0,0,0])).unwrap(); // extracted from file
    assert_eq!(temp_val_ref, temp_val_here);

    // Check another value: ds_rust["t_2m"].data[2,2,2]
    let temp_val_ref: f64 = 5.269172668457031;
    let temp_val_here: f64 = temperature.value(Some(&[2,2,2])).unwrap();
    assert_eq!(temp_val_ref, temp_val_here);

    // Remove the file    
    let dir: &Path = Path::new(&file_name).parent().unwrap();
    fs::remove_file(&file_name).unwrap();
    fs::remove_dir_all(&dir).unwrap();
    // Check if the file and the directory were removed.
    assert!(!Path::new(&file_name).exists());
    assert!(!Path::new(&dir).exists());
}

#[tokio::test]
async fn query_png() {
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

    // Create Parameters
    let parameter = String::from("t_2m:C");

    // Create Location
    let bbox = BBox {
        lat_min: 45.8179716,
        lat_max: 47.8084648,
        lon_min: 5.9559113,
        lon_max: 10.4922941,
        lat_res: 0.01,
        lon_res: 0.01
    };

    // Create file name
    let file_name = String::from("tests/png/my_png.png");

    // Call endpoint
    meteomatics_connector
        .query_grid_png(
            &start_date, &parameter, &bbox, &file_name, &None
        )
        .await
        .unwrap();

    let decoder = png::Decoder::new(fs::File::open(&file_name).unwrap());
    let reader = decoder.read_info().unwrap();

    // Inspect more details of the last read frame.
    assert_eq!(454, reader.info().width);
    assert_eq!(200, reader.info().height);
    
    // Remove the file    
    let dir: &Path = Path::new(&file_name).parent().unwrap();
    fs::remove_file(&file_name).unwrap();
    fs::remove_dir_all(&dir).unwrap();
    // Check if the file and the directory were removed.
    assert!(!Path::new(&file_name).exists());
    assert!(!Path::new(&dir).exists());
}

#[tokio::test]
async fn query_grid_png_timeseries() {
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
    let parameter = String::from("t_2m:C");

    // Create Location
    let bbox = BBox {
        lat_min: 45.8179716,
        lat_max: 47.8084648,
        lon_min: 5.9559113,
        lon_max: 10.4922941,
        lat_res: 0.01,
        lon_res: 0.01
    };

    // Create file name
    let prefixpath: String = String::from("tests/png_series/test_series");

    // Call endpoint
    meteomatics_connector
        .query_grid_png_timeseries(
            &start_date, &end_date, &interval, &parameter, &bbox, &prefixpath, &None
        )
        .await
        .unwrap();
    
    // Open a single PNG
    let fmt = "%Y%m%d_%H%M%S";
    let file_name = format!("{}_{}.png", prefixpath, start_date.format(fmt));
    let decoder = png::Decoder::new(fs::File::open(&file_name).unwrap());
    let reader = decoder.read_info().unwrap();

    // Inspect more details of the last read frame.
    assert_eq!(454, reader.info().width);
    assert_eq!(200, reader.info().height);
        
    // Remove the file    
    let dir: &Path = Path::new(&file_name).parent().unwrap();
    fs::remove_file(&file_name).unwrap();
    fs::remove_dir_all(&dir).unwrap();
    // Check if the file and the directory were removed.
    assert!(!Path::new(&file_name).exists());
    assert!(!Path::new(&dir).exists());
}

#[tokio::test]
async fn query_user_features(){
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

    let ustats = meteomatics_connector.query_user_features().await.unwrap();
    assert_eq!(env::var("METEOMATICS_USER").unwrap(), ustats.stats.username);
}

#[tokio::test]
async fn query_lightning(){
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
    let start_date = Utc.ymd(2022, 5, 20).and_hms_micro(10, 0, 0, 0);
    let end_date = start_date + Duration::days(1);

    // Create Location
    let bbox: BBox = BBox {
        lat_min: 45.8179716,
        lat_max: 47.8084648,
        lon_min: 5.9559113,
        lon_max: 10.4922941,
        lat_res: 0.0,
        lon_res: 0.0
    };

    let df = meteomatics_connector.query_lightning(
        &start_date, &end_date, &bbox
    ).await.unwrap();

    println!("{:?}", df);
}

#[tokio::test]
async fn query_route_points(){
    let s = r#"lat;lon;validdate;t_2m:C;precip_1h:mm;sunshine_duration_1h:min
    47.423938;9.372858;2021-05-25T12:00:00Z;11.5;0.00;60.0
    47.499419;8.726517;2021-05-25T13:00:00Z;13.2;0.06;58.6
    47.381967;8.530662;2021-05-25T14:00:00Z;13.4;0.00;24.3
    46.949911;7.430099;2021-05-25T15:00:00Z;12.8;0.00;53.5
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
    let date1 = Utc.ymd(2021, 5, 25).and_hms_micro(12, 0, 0, 0);
    let date2 = Utc.ymd(2021, 5, 25).and_hms_micro(13, 0, 0, 0);
    let date3 = Utc.ymd(2021, 5, 25).and_hms_micro(14, 0, 0, 0);
    let date4 = Utc.ymd(2021, 5, 25).and_hms_micro(15, 0, 0, 0);
    let dates = vec![date1, date2, date3, date4];

    // Create Parameters
    let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm"), String::from("sunshine_duration_1h:min")];

    // Create Locations
    let p1: Point = Point { lat: 47.423938, lon: 9.372858};
    let p2: Point = Point { lat: 47.499419, lon: 8.726517};
    let p3: Point = Point { lat: 47.381967, lon: 8.530662};
    let p4: Point = Point { lat: 46.949911, lon: 7.430099};
    let coords = vec![p1, p2, p3, p4];

    let df_r = meteomatics_connector.route_query_points(
        &dates, &coords, &parameters
    ).await.unwrap();

    assert_eq!(df_s, df_r);
}

#[tokio::test]
async fn query_route_postal(){
    let s = r#"station_id;validdate;t_2m:C;precip_1h:mm;sunshine_duration_1h:min
    postal_CH9000;2021-05-25T12:00:00Z;11.4;0.00;60.0
    postal_CH8400;2021-05-25T13:00:00Z;13.2;0.03;56.4
    postal_CH8000;2021-05-25T14:00:00Z;13.4;0.00;21.9
    postal_CH3000;2021-05-25T15:00:00Z;12.6;0.00;20.1
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
    let date1 = Utc.ymd(2021, 5, 25).and_hms_micro(12, 0, 0, 0);
    let date2 = Utc.ymd(2021, 5, 25).and_hms_micro(13, 0, 0, 0);
    let date3 = Utc.ymd(2021, 5, 25).and_hms_micro(14, 0, 0, 0);
    let date4 = Utc.ymd(2021, 5, 25).and_hms_micro(15, 0, 0, 0);
    let dates = vec![date1, date2, date3, date4];

    // Create Parameters
    let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm"), String::from("sunshine_duration_1h:min")];

    // Create Locations
    let pcode1 = String::from("postal_CH9000");
    let pcode2 = String::from("postal_CH8400");
    let pcode3 = String::from("postal_CH8000");
    let pcode4 = String::from("postal_CH3000");
    let coords = vec![pcode1, pcode2, pcode3, pcode4];

    let df_r = meteomatics_connector.route_query_postal(
        &dates, &coords, &parameters
    ).await.unwrap();

    assert_eq!(df_s, df_r);
}