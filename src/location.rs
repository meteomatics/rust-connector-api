use std::fmt;

pub struct Point {
    pub lat: f64,
    pub lon: f64,
}

pub struct Grid {
    lat_min: f64,
    lat_max: f64,
    lon_min: f64,
    lon_max: f64,
}

impl fmt::Display for Point {
    fn fmt(&self, f:&mut fmt::Formatter) -> fmt::Result {
        write!(f, "{},{}", &self.lat.to_string(), &self.lon.to_string())
    }
}

impl fmt::Display for Grid {
    fn fmt(&self, f:&mut fmt::Formatter) -> fmt::Result {
        write!(f, "{},{}_{},{}", &self.lon_min.to_string(), &self.lat_max.to_string(), 
        &self.lon_max.to_string(), &self.lat_min.to_string())
    }
}