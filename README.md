[![Rust](https://github.com/bbuman/rust-connector-api/actions/workflows/rust.yml/badge.svg?branch=structure_revamp)](https://github.com/bbuman/rust-connector-api/actions/workflows/rust.yml)

# rust-connector-api
Meteomatics provides a REST-style API to retrieve historic, current, and forecast data globally. This includes model data and observational data in time series and areal formats. Areal formats are also offered through a WMS/WFS-compatible interface. Geographic and time series data can be combined in certain file formats, such as NetCDF.

Simply add the meteomatics crate to the dependencies of your project.

# For a start we recommend to inspect and play with the examples.
- Change to the meteomatics directory using ```cd meteomatics``` 
- Open and inspect the documentation for the connector 
```cargo doc --lib --no-deps --open```
- Open and inspect the documentation for the examples 
```cargo doc --examples --no-deps --open```
- ```cargo run --example point_query```
- ```cargo run --example grid_query```
- ```cargo run --example point_query```

# For developers
- create a file called ```.env``` inside the repository root and add:
```text
METEOMATICS_USER=your_username
METEOMATICS_PW=your_password
```
- Change to the meteomatics directory using ```cd meteomatics```
- Run the various tests using ```cargo test```

# Notes
- Tested on ```rustc 1.60.0``` and ```rustc 1.61.0```.
