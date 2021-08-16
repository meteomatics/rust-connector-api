use strum_macros::Display;

#[allow(dead_code)]
#[derive(Clone, Display, Debug)]
pub enum Format {
    #[strum(serialize = "csv")]
    CSV,
}
