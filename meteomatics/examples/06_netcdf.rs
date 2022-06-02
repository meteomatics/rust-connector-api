//! # NetCDF Query 
//! This is a demonstration of how to download a NetCDF (Network Common Data Format) for an area or 
//! "grid" defined by a bounding-box and with a defined grid resolution (e.g. 0.1 °). 
//! To run the example either change ```u_name``` and ```u_pw```
//! directly *or* create a file called ```.env``` and put the following lines in there:
//! ```text
//! METEOMATICS_PW=your_password
//! METEOMATICS_USER=your_username
//! ```
//! Make sure to include ```.env``` in your ```.gitignore```. This is a safer variant for developers 
//! to work with API credentials as you will never accidentally commit/push your credentials.

use chrono::{Utc, Duration};
use rust_connector_api::{APIClient, BBox, TimeSeries};
use rust_connector_api::errors::ConnectorError;
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

    // Define the name of the file
    let file_name = String::from("switzerland_t_2m_C.nc");

    example_request(&api, &file_name).await.unwrap();

    // Look at the NetCDF
    let nc_file = netcdf::open(&file_name).unwrap();
    let temp2m = &nc_file.variable("t_2m").expect("Could not find variable 't_2m");
    // Access the slice at [0, 0, 0] and get dataset of size [1, 10, 10]
    let temp2m_slice = temp2m.values::<f64>(Some(&[0,0,0]), Some(&[1, 10, 10])).unwrap();

    // Print the query result
    println!("{:?}", temp2m_slice);
   
}

/// Query a time series for a single point and two parameters.
async fn example_request(api: &APIClient, file_name: &String) -> std::result::Result<(), ConnectorError>{
    // Time series definition
    let start_date = Utc::now();
    let time_series = TimeSeries {
        start: start_date,
        end: start_date + Duration::days(1),
        timedelta: Option::from(Duration::hours(1))
    };

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
    let t_2m = String::from("t_2m:C");

    // Optionals
    let model_mix = String::from("model=mix");
    let optionals = Option::from(vec![model_mix]);

    // Download the NetCDF
    let result = api.query_netcdf(
        &time_series, &t_2m, &ch, file_name, &optionals
    ).await;

    result
}
