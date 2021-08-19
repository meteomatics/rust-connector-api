use chrono::{DateTime, Utc};
use std::fmt::{Display, Formatter};

#[derive(Builder, Clone, Debug, PartialEq)]
pub struct ValidDateTime {
    #[builder(setter(into))]
    pub start_date_time: DateTime<Utc>,

    #[builder(setter(strip_option), default)]
    pub period_date: Option<PeriodDate>,

    #[builder(setter(strip_option), default)]
    pub end_date_time: Option<DateTime<Utc>>,

    #[builder(setter(strip_option), default)]
    pub time_step: Option<PeriodTime>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
#[allow(dead_code)]
pub enum PeriodDate {
    Years(i32),
    Months(i32),
    Days(i32),
}

#[derive(Clone, Copy, Debug, PartialEq)]
#[allow(dead_code)]
pub enum PeriodTime {
    Hours(i32),
    Minutes(i32),
    Seconds(i32),
}

impl Display for PeriodDate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PeriodDate::Years(n) => {
                write!(f, "P{}Y", n)
            }
            PeriodDate::Months(n) => {
                write!(f, "P{}M", n)
            }
            PeriodDate::Days(n) => {
                write!(f, "P{}D", n)
            }
        }
    }
}

impl Display for PeriodTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PeriodTime::Hours(n) => {
                write!(f, "PT{}H", n)
            }
            PeriodTime::Minutes(n) => {
                write!(f, "PT{}M", n)
            }
            PeriodTime::Seconds(n) => {
                write!(f, "PT{}S", n)
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::connector_components::valid_date_time::ValidDateTimeBuilder;
    use crate::connector_components::valid_date_time::{PeriodDate, PeriodTime, ValidDateTime};
    use chrono::{Duration, Utc};

    #[tokio::test]
    async fn call_new() {
        let start_date_time = Utc::now();

        let vdt: ValidDateTime = ValidDateTimeBuilder::default()
            .start_date_time(start_date_time)
            .build()
            .unwrap();

        println!("call_new:");
        println!("vdt.start_date_time: {:?}", vdt.start_date_time);
        println!("vdt.period_date: {:?}", vdt.period_date);
        println!("vdt.end_date_time: {:?}", vdt.end_date_time);
        println!("vdt.time_step: {:?}", vdt.time_step);

        assert_eq!(
            vdt,
            ValidDateTime {
                start_date_time,
                period_date: None,
                end_date_time: None,
                time_step: None
            }
        );
    }

    #[tokio::test]
    async fn call_new_with_optional_params() {
        let start_date_time = Utc::now();
        let period_date = PeriodDate::Days(1);
        let end_date_time = start_date_time + Duration::days(1);
        let time_step = PeriodTime::Hours(1);

        let vdt: ValidDateTime = ValidDateTimeBuilder::default()
            .start_date_time(start_date_time)
            .period_date(period_date)
            .end_date_time(end_date_time)
            .time_step(time_step)
            .build()
            .unwrap();

        println!("call_new_with_optional_params:");
        println!("vdt.start_date_time: {:?}", vdt.start_date_time);
        println!("vdt.period_date: {}", vdt.period_date.unwrap());
        println!("vdt.end_date_time: {:?}", vdt.end_date_time);
        println!("vdt.time_step: {}", vdt.time_step.unwrap());

        assert_eq!(
            vdt,
            ValidDateTime {
                start_date_time,
                period_date: Some(period_date),
                end_date_time: Some(end_date_time),
                time_step: Some(time_step)
            }
        );

        assert_eq!(vdt.period_date.unwrap(), PeriodDate::Days(1));
        assert_eq!(vdt.time_step.unwrap(), PeriodTime::Hours(1));

        assert_eq!(vdt.period_date.unwrap().to_string(), "P1D");
        assert_eq!(vdt.time_step.unwrap().to_string(), "PT1H")
    }
}
