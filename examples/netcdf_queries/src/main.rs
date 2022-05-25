use chrono::{Utc, DateTime, Duration};
use rust_connector_api::APIClient;
use rust_connector_api::location::BBox;
use rust_connector_api::errors::ConnectorError;

#[tokio::main]
async fn main(){
    // Credentials  
    let u_name: String = String::from("python-community");
    let u_pw: String = String::from("Umivipawe179");

    // Create Client
    let api: APIClient = APIClient::new(u_name,u_pw,10);

    // Define the name of the file
    let file_name = String::from("switzerland_t_2m_C.nc");

    example_request(&api, &file_name).await.unwrap();

    // Look at the NetCDF
    let file = netcdf::open(&file_name).unwrap();
    let var = &file.variable("t_2m").expect("Could not find variable 't_2m");
    // Access the slice at [0, 0, 0] and get dataset of size [1, 10, 10]
    let data = var.values::<f64>(Some(&[0,0,0]), Some(&[1, 10, 10])).unwrap();

    // Print the query result
    println!("{:?}", data);
   
}

/// Query a time series for a single point and two parameters.
async fn example_request(api: &APIClient, file_name: &String) -> std::result::Result<(), ConnectorError>{
    // Time series definition
    let start_date: DateTime<Utc> = Utc::now();
    let end_date: DateTime<Utc> = start_date + Duration::days(1);
    let interval: Duration = Duration::hours(1);

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

    // Optionals
    let opt1: String = String::from("model=mix");
    let optionals: Option<Vec<String>> = Option::from(vec![opt1]);

    // Download the NetCDF
    let result = api.query_netcdf(
        &start_date, &end_date, &interval, &param1, &ch, file_name, &optionals
    ).await;

    result
}
