[![Rust](https://github.com/bbuman/rust-connector-api/actions/workflows/rust.yml/badge.svg?branch=structure_revamp)](https://github.com/bbuman/rust-connector-api/actions/workflows/rust.yml)

# rust-connector-api
Meteomatics provides a REST-style API to retrieve historic, current, and forecast data globally. This includes model data and observational data in time series and areal formats. Areal formats are also offered through a WMS/WFS-compatible interface. Geographic and time series data can be combined in certain file formats, such as NetCDF.

Simply add ```meteomatics-api = "0.2.0"``` to the dependencies of your project. We recommend to use ```meteomatics-api``` together with  ```tokio = { version = "1", features = ["full"] }```, ```chrono = "0.4"``` and ```polars = "0.21.1"```. 

For a start we recommend to inspect and play with the examples.
- ```cargo doc --examples --no-deps --open``` will open the documentation for the examples. 
- ```cargo run --example point_query```
- ```cargo run --example grid_query```
- ```cargo run --example point_query```
