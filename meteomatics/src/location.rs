//! # Location
//! This module specifies two custom structures for the query methods: a ```Point``` and a ```BBox```.
//! A ```Point``` specifies a point in geographical space using latitude and longitude coordinates (
//! e.g. St. Gallen main station -> 47.423, 9.370).
//! ```rust, no_run
//! use rust_connector_api::Point;
//! 
//! let st_gallen = Point { lat: 47.423, lon: 9.370};
//! ````
//! 
//! A ```BBox```specifies the bounding box in geographical space for a grid query. The box is defined
//! on the coordinates of the upper left (latitude max value, longitue min value) and lower right points
//! (latitude min value, longitude max value). The BBox further requires the definition of the desired
//! output resolution of the grid (latitude resolution, longitude resolution).
//! ```rust, no_run
//! use rust_connector_api::BBox;
//! 
//! let st_gallen_grid = BBox {
//!     lat_min: 47.423,
//!     lat_max: 47.424,
//!     lon_min: 9.369,
//!     lon_max: 9.370,
//!     lat_res: 0.0005,
//!     lon_res: 0.0005
//! };
//! ```

use std::fmt;

/// Define a location using its latitude and longitude coordinates. This is used in the generation of 
/// the query in ```query_time_series()```.
pub struct Point {
    pub lat: f64,
    pub lon: f64,
}

/// Define an area of interest by specifying a bounding box with coordinates at the upper left (lat_max, 
/// lon_min) and lower right locations (lat_min, lon_max). This is used in the generation of the query
/// in ```query_grid()``` and ```query_grid_time_series()```. 
pub struct BBox {
    pub lat_min: f64,
    pub lat_max: f64,
    pub lon_min: f64,
    pub lon_max: f64,
    pub lat_res: f64,
    pub lon_res: f64,
}

/// This Display Trait implements the correct way of combining latitude and longitude coordinates for
/// a Point. According to the MeteoMatics API specifications. 
// TODO: Think about the number of significant digits and rounding/imprecision issues.
impl fmt::Display for Point {
    fn fmt(&self, f:&mut fmt::Formatter) -> fmt::Result {
        write!(f, "{},{}", &self.lat.to_string(), &self.lon.to_string())
    }
}

/// This Display Trait implements the correct way of combining the bounding box coordinates. 
impl fmt::Display for BBox {
    fn fmt(&self, f:&mut fmt::Formatter) -> fmt::Result {
        write!(
            f, 
            "{},{}_{},{}:{},{}", 
            &self.lat_max.to_string(), 
            &self.lon_min.to_string(), 
            &self.lat_min.to_string(), 
            &self.lon_max.to_string(),
            &self.lat_res.to_string(),
            &self.lon_res.to_string()
        )
    }
}