use std::collections::HashMap;

type ParameterMap = HashMap<String, String>;

#[derive(Clone, Debug)]
struct Parameters {
    values: Vec<ParameterMap>,
}

impl Parameters {}
