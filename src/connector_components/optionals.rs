use std::collections::HashMap;

type OptionalMap = HashMap<String, String>;

#[derive(Clone, Debug)]
struct Optionals {
    values: Vec<OptionalMap>,
}

impl Optionals {}
