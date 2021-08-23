use chrono::{DateTime, TimeZone};
use std::fmt::{Display, Formatter};

#[derive(Builder, Clone, Debug, PartialEq)]
pub struct ValidDateTime<Tz: TimeZone> {
    #[builder(setter(into))]
    pub start_date_time: DateTime<Tz>,

    #[builder(setter(strip_option), default)]
    pub period_date: Option<PeriodDate>,

    #[builder(setter(strip_option), default)]
    pub end_date_time: Option<DateTime<Tz>>,

    #[builder(setter(strip_option), default)]
    pub time_step: Option<PeriodTime>,

    #[builder(setter(strip_option), default)]
    pub time_list: Option<Vec<DateTime<Tz>>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PeriodDate {
    Years(i32),
    Months(i32),
    Days(i32),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PeriodTime {
    Hours(i32),
    Minutes(i32),
    Seconds(i32),
}

impl Display for PeriodDate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PeriodDate::Years(n) => write!(f, "P{}Y", n),
            PeriodDate::Months(n) => write!(f, "P{}M", n),
            PeriodDate::Days(n) => write!(f, "P{}D", n),
        }
    }
}

impl Display for PeriodTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PeriodTime::Hours(n) => write!(f, "PT{}H", n),
            PeriodTime::Minutes(n) => write!(f, "PT{}M", n),
            PeriodTime::Seconds(n) => write!(f, "PT{}S", n),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::connector_components::valid_date_time::ValidDateTimeBuilder;
    use crate::connector_components::valid_date_time::{PeriodDate, PeriodTime, ValidDateTime};
    use chrono::{Duration, Local, Utc};

    #[tokio::test]
    async fn create_with_default() {
        println!("##### create_with_default (UTC):");

        // Use UTC.
        let start_date_time = Utc::now();

        let utc_vdt: ValidDateTime<Utc> = ValidDateTimeBuilder::default()
            .start_date_time(start_date_time)
            .build()
            .unwrap();

        println!("utc_vdt.start_date_time: {:?}", utc_vdt.start_date_time);
        println!("utc_vdt.period_date: {:?}", utc_vdt.period_date);
        println!("utc_vdt.end_date_time: {:?}", utc_vdt.end_date_time);
        println!("utc_vdt.time_step: {:?}", utc_vdt.time_step);

        assert_eq!(
            utc_vdt,
            ValidDateTime {
                start_date_time,
                period_date: None,
                end_date_time: None,
                time_step: None,
                time_list: None
            }
        );
    }

    #[tokio::test]
    async fn create_with_optional_params() {
        println!("##### create_with_optional_params (local):");

        // Use local time zone.
        let start_date_time = Local::now();
        let period_date = PeriodDate::Days(1);
        let end_date_time = start_date_time.clone() + Duration::days(1);
        let time_step = PeriodTime::Hours(1);
        let time_list = vec![start_date_time, end_date_time];

        let local_vdt: ValidDateTime<Local> = ValidDateTimeBuilder::default()
            .start_date_time(start_date_time)
            .period_date(period_date)
            .end_date_time(end_date_time)
            .time_step(time_step)
            .time_list(time_list)
            .build()
            .unwrap();

        println!("local_vdt.start_date_time: {:?}", local_vdt.start_date_time);
        println!("local_vdt.period_date: {}", local_vdt.period_date.unwrap());
        println!(
            "local_vdt.end_date_time: {:?}",
            local_vdt.end_date_time.unwrap()
        );
        println!("local_vdt.time_step: {}", local_vdt.time_step.unwrap());

        let tl = local_vdt.time_list.unwrap();
        println!("local_vdt.time_list: {:?}", tl);

        assert_eq!(local_vdt.start_date_time, start_date_time);
        assert_eq!(local_vdt.end_date_time.unwrap(), end_date_time);

        assert_eq!(local_vdt.period_date.unwrap(), PeriodDate::Days(1));
        assert_eq!(local_vdt.time_step.unwrap(), PeriodTime::Hours(1));

        assert_eq!(local_vdt.period_date.unwrap().to_string(), "P1D");
        assert_eq!(local_vdt.time_step.unwrap().to_string(), "PT1H");

        assert_eq!(tl[0], start_date_time);
        assert_eq!(tl[1], end_date_time);
    }
}
