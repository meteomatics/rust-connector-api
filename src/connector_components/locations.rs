use std::fmt::{Display, Formatter};

pub type Coordinates<'a> = Vec<&'a str>;

#[derive(Clone, Debug, PartialEq)]
pub struct Locations<'a> {
    pub coordinates: Coordinates<'a>,
}

impl<'a> Display for Locations<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.coordinates.to_vec().join(","))
    }
}

#[cfg(test)]
mod tests {

    use crate::locations::{Coordinates, Locations};

    #[tokio::test]
    async fn with_some_values() {
        println!("##### with_some_values:");

        // Single point
        let coordinates = Coordinates::from(["47.419708", "9.358478"]);
        let locations: Locations = Locations { coordinates };

        println!(">>>>>>>>>> single_point_loc: {}", locations);
        assert_eq!(locations.to_string(), "47.419708,9.358478");

        // Point list
        let locations: Locations = Locations {
            coordinates: Coordinates::from(["47.41", "9.35+47.51", "8.74+47.13", "8.22"]),
        };

        println!(">>>>>>>>>> point_list_loc: {}", locations);
        assert_eq!(locations.to_string(), "47.41,9.35+47.51,8.74+47.13,8.22");

        // Postal codes
        let locations: Locations = Locations {
            coordinates: Coordinates::from([
                "postal_CH9014",
                "postal_CH9000",
                "postal_US10001",
                "postal_GBW2",
            ]),
        };

        println!(">>>>>>>>>> postal_codes_loc: {}", locations);

        assert_eq!(
            locations.to_string(),
            "postal_CH9014,postal_CH9000,postal_US10001,postal_GBW2"
        );

        // Line
        let locations: Locations = Locations {
            coordinates: Coordinates::from(["50", "10_50", "20:100"]),
        };

        println!(">>>>>>>>>> line_loc: {}", locations);

        assert_eq!(locations.to_string(), "50,10_50,20:100");
    }
}
