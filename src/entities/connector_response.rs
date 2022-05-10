use csv::Reader;
use std::io::Read;

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectorResponse {
    pub response_body: ResponseBody,
    pub http_status_code: String,
    pub http_status_message: String,
}

pub type ResponseHeader = Vec<String>;
pub type ResponseRecord = Vec<Vec<f64>>;
pub type ResponseIndex = Vec<String>;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResponseBody {
    pub response_headers: ResponseHeader,
    pub response_records: ResponseRecord,
    pub response_indexes: ResponseIndex,
}

impl ResponseBody {
    pub fn new() -> Self {
        Self {
            response_headers: Default::default(),
            response_records: Default::default(),
            response_indexes: Default::default(),
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
            let row_index: String = index;
            let row: Vec<f64> = values;
            self.response_records.push(row);
            self.response_indexes.push(row_index);
        }
        Ok(())
    }
}

impl std::fmt::Display for ResponseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.response_headers.to_vec().join(","))?;
        for i in 0..self.response_records.len() {
            let index = &self.response_indexes[i];
            let values = &self.response_records[i];
            let values_str: Vec<_> = values.to_vec().
                iter()
                .map(ToString::to_string)
                .collect();
            writeln!(f, "{}", index.to_owned() + ": " + &values_str.join(","))?;
        }
        Ok(())
    }
}
