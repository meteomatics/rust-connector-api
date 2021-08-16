use chrono::{DateTime, Duration, Utc};

#[derive(Clone, Debug)]
struct ValidDateTime {
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    time_interval: Duration,
}

impl ValidDateTime {}
