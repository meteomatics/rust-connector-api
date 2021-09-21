use csv::Reader;
use std::io::Read;

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectorResponse {
    pub body: CSVBody,
    pub http_status_code: String,
    pub http_status_message: String,
}

pub type CSVHeader = Vec<String>;
pub type CSVRecord = Vec<Vec<String>>;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CSVBody {
    pub csv_headers: CSVHeader,
    pub csv_records: CSVRecord,
}

impl CSVBody {
    #![allow(unused_mut)]
    pub fn new() -> Self {
        let mut csv_headers: CSVHeader = Default::default();
        let mut csv_records: CSVRecord = Default::default();
        Self {
            csv_headers,
            csv_records,
        }
    }

    pub fn add_header(&mut self, header: String) {
        self.csv_headers.push(header);
    }

    pub async fn populate_records<R: Read>(
        &mut self,
        rdr: &mut Reader<R>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for record in rdr.records() {
            let record = record?;
            let mut row: Vec<String> = vec![];
            for (&_, value) in self.csv_headers.iter().zip(record.iter()) {
                row.push(value.to_owned());
            }
            self.csv_records.push(row);
        }
        Ok(())
    }
}

impl std::fmt::Display for CSVBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.csv_headers.to_vec().join(","))?;
        for row in self.csv_records.iter() {
            writeln!(f, "{}", row.to_vec().join(","))?;
        }
        Ok(())
    }
}
