//! # PNG Grid Query 
//! PNG grid queries allow you to request an image of weather and climate data for cells in a 
//! rectangular grid, where the grid is defined by a bounding box. The bounding box or [`BBox`](`rust_connector_api::BBox`) is
//! defined by the upper left (i.e. North Western) corner and lower right(i.e. South Eastern) corner). 
//! The cell size in turn is defined either in pixels (```res_lat=400``` = 400 pixel heigh cells) or
//! in degrees (e.g. ```res_lat=0.1```= 0.1° or about 7 km at the equator). 
//! 
//!# The Example
//! The example demonstrates how to request current temperature and precipitation data for Switzerland. 
//! The grid is spaced in 0.1 ° (or about 7 km cell width and cell height).
//! There are several optional parameters you can pass to the meteomatics API that will change the 
//! data you get back. In the example we specify that we would like to receive the parameters based 
//! on the mix ```model = String::from("model=mix");```. The Meteomatics Mix combines different model
//! s and sources into an intelligent blend, such that the best data source is chosen for each time 
//! and location (<https://www.meteomatics.com/en/api/request/optional-parameters/data-source/>).
//! 
//! # The account
//! You can use the provided credentials or your own if you already have them. 
//! Check out <https://www.meteomatics.com/en/request-business-wather-api-package/> to request an 
//! API package.

use chrono::{Utc};
use meteomatics::{APIClient, BBox};
use meteomatics::errors::ConnectorError;

#[tokio::main]
async fn main(){
    // Credentials
    let api: APIClient = APIClient::new("rust-community", "5GhAwL3HCpFB", 10);

    // Define the name of the file
    let file_name = String::from("switzerland_t_2m_C.png");

    example_request(&api, &file_name).await.unwrap();
   
}

/// Query a time series for a single point and two parameters.
async fn example_request(api: &APIClient, file_name: &String) -> std::result::Result<(), ConnectorError>{
    // Time series definition
    let start_date = Utc::now();

    // Location definition
    let ch: BBox = BBox {
        lat_min: 45.8,
        lat_max: 47.8,
        lon_min: 6.0,
        lon_max: 10.5,
        lat_res: 0.1,
        lon_res: 0.1,
    };

    // Parameter selection
    let t_2m = String::from("t_2m:C");

    // Optionals
    let model_mix = String::from("model=mix");
    let optionals = Option::from(vec![model_mix]);

    // Download the NetCDF
    let result = api.query_grid_png(
        &start_date, &t_2m, &ch, file_name, &optionals
    ).await;

    result
}
