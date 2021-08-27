use std::fmt::{Display, Formatter};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Opt<'a> {
    pub k: &'a str,
    pub v: &'a str,
}

pub type OptSet<'a> = Vec<Opt<'a>>;

#[derive(Builder, Clone, Debug, PartialEq)]
pub struct Optionals<'a> {
    pub opt_values: OptSet<'a>,
}

impl<'a> Display for Opt<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.k.to_owned() + "=" + &*self.v.to_owned())
    }
}

impl<'a> Display for Optionals<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.opt_values
                .iter()
                .map(|opt| opt.to_string())
                .collect::<Vec<String>>()
                .join("&")
        )
    }
}

#[cfg(test)]
mod tests {

    use crate::optionals::{Opt, OptSet, Optionals, OptionalsBuilder};
    use std::iter::FromIterator;

    #[tokio::test]
    async fn with_values() {
        println!("##### with_values:");

        let opt_values: OptSet<'_> = OptSet::from_iter([
            Opt {
                k: "source",
                v: "mix",
            },
            Opt {
                k: "calibrated",
                v: "true",
            },
        ]);

        let optionals: Optionals = OptionalsBuilder::default()
            .opt_values(opt_values)
            .build()
            .unwrap();

        println!("optionals: {}", optionals);
        assert_eq!(optionals.to_string(), "source=mix&calibrated=true");
        assert_ne!(
            optionals.opt_values,
            OptSet::from_iter([Opt {
                k: "source",
                v: "mix"
            }])
        );
    }
}
