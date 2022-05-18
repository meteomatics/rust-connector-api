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
// TODO: Think about the number of significant digits and rounding/imprecision issues.
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