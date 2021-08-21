use std::collections::HashSet;

type OptionalSet = HashSet<String, String>;

#[derive(Clone, Debug)]
struct Optionals {
    values: Vec<OptionalSet>,
}

impl Optionals {}
