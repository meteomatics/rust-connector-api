use chrono::{Utc, Duration, TimeZone};
use meteomatics::{APIClient, BBox, TimeSeries};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::ops::Deref;

struct SharedRequestArguments
{
    api: APIClient,
    saentis: BBox,
    t_2m: String,
    optionals: Option<Vec<String>>,
}

impl SharedRequestArguments
{
    pub fn new() -> SharedRequestArguments
    {
        let api: APIClient = APIClient::new("nice", "try", 300);

        let saentis = BBox {
            lat_max: 47.57,
            lat_min: 46.93,
            lon_min: 9.03,
            lon_max: 9.67,
            lat_res: 0.1,
            lon_res: 0.1
        };
        // Parameter selection
        let t_2m = String::from("wind_speed_10m:ms");
        // Optionals
        let model_mix = String::from("model=mm-swiss1k");
        let optionals = Option::from(vec![model_mix]);

        Self { api, saentis, t_2m, optionals  }
    }
}

// This test demonstrates a more advanced usage of the api and async
#[tokio::main]
async fn main(){
    let start_date = Utc.ymd(2018, 10, 1).and_hms(0, 0, 0);
    let end_date = Utc.ymd(2018, 10, 4).and_hms(0, 0, 0);
    let mut cur_date = start_date;

    // Arguments and API handle only constructed once and then shared between threads
    // Rw Lock can have multiple readers using this information without blocking
    let args = Arc::new(RwLock::new(SharedRequestArguments::new()));

    let mut handles = Vec::new();
    while cur_date < end_date {
        let thread_args = Arc::clone(&args);
        let time_series = TimeSeries{
            start: cur_date,
            end: cur_date + Duration::days(1),
            timedelta: Option::from(Duration::hours(6))
        };
        let api_call = async move {
            let filename = format!("download/saentis_{}.nc", cur_date.to_rfc3339());
            println!("To be downloaded: {}", filename);

            let args = thread_args.deref().read().await;
            args.api.query_netcdf(
                &time_series,
                &args.t_2m,
                &args.saentis,
                &filename,
                &args.optionals
            ).await
        };
        let job = tokio::spawn(api_call);
        cur_date = cur_date + Duration::days(1);
        handles.push(job);
    }

    // print results
    for job in handles {
        println!("{:?}", job.await);
    }
}
