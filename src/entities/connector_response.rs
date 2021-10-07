use csv::Reader;
use std::io::Read;

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectorResponse {
    pub body: ResponseBody,
    pub http_status_code: String,
    pub http_status_message: String,
}

pub type ResponseHeader = Vec<String>;
pub type ResponseRecord = Vec<(String, Vec<f64>)>;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResponseBody {
    pub response_headers: ResponseHeader,
    pub response_records: ResponseRecord,
}

impl ResponseBody {
    #![allow(unused_mut)]
    pub fn new() -> Self {
        let mut response_headers: ResponseHeader = Default::default();
        let mut response_records: ResponseRecord = Default::default();
        Self {
            response_headers,
            response_records,
        }
    }

    pub fn add_header(&mut self, header: String) {
        self.response_headers.push(header);
    }

    pub async fn populate_records<R: Read>(
        &mut self,
        rdr: &mut Reader<R>,
        header_num_elements: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for record in rdr.records().skip(header_num_elements) {
            let record = record?;
            let mut index: String = "".to_string();
            let mut values: Vec<f64> = vec![];
            // for (&_, value) in self.response_headers.iter().zip(record.iter()) {
            for n in 0..record.len() {
                if n == 0 {
                    index = record.get(n).unwrap().to_string();
                } else {
                    let value = record.get(n).unwrap();
                    values.push(value.parse::<f64>().unwrap());
                }
            }
            let mut row: (String, Vec<f64>) = (index, values);
            self.response_records.push(row);
        }
        Ok(())
    }
}

impl std::fmt::Display for ResponseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.response_headers.to_vec().join(","))?;
        for row in self.response_records.iter() {
            let (index, values) = row;
            let values_str: Vec<_> = values.to_vec().iter().map(ToString::to_string).collect();
            writeln!(f, "{}", index.to_owned() + ": " + &values_str.join(","))?;
        }
        Ok(())
    }
}
