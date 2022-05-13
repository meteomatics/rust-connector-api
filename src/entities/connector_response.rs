use csv::Reader;
use std::io::Read;
use tempfile::tempdir;
use polars::prelude::*;

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectorResponse {
    pub response_body: ResponseBody,
    pub http_status_code: String,
    pub http_status_message: String,
}

pub type ResponseHeader = Vec<String>;
pub type ResponseRecord = DataFrame;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResponseBody {
    pub response_header: ResponseHeader,
    pub response_df: ResponseRecord,
}

impl ResponseBody {
    pub fn new() -> Self {
        Self {
            response_header: Default::default(),
            response_df: Default::default(),
        }
    }

    pub fn add_header(&mut self, header: String) {
        self.response_header.push(header);
    }

    pub async fn csv_to_polars<R: Read>(
        &mut self,
        rdr: &mut Reader<R>,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {

        // Create a directory inside of `std::env::temp_dir()`.
        let dir = tempdir()?;

        let file_path = dir.path().join("http_response.csv");
        // Create a file inside of `std::env::temp_dir()`.
        let mut wtr = csv::Writer::from_path(&file_path).unwrap();

        for rec in rdr.records() {
            let record = rec?;
            wtr.write_record(&record)?;

        }
        wtr.flush()?;
        self.response_df = CsvReader::from_path(file_path)?
        .infer_schema(None)
        .has_header(true)
        .finish()?;
        Ok(())
    }
}
