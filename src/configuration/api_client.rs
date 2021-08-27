use crate::format::Format;
use crate::locations::Locations;
use crate::optionals::Optionals;
use crate::parameters::Parameters;
use crate::valid_date_time::ValidDateTime;
use reqwest::{Client, Response};
use url::{ParseError, Url};

const DEFAULT_API_BASE_URL: &str = "https://api.meteomatics.com";

#[derive(Clone, Debug)]
pub struct APIClient {
    http_client: Client,
    username: String,
    password: String,
}

impl APIClient {
    pub fn new(username: String, password: String, timeout_seconds: u64) -> Self {
        let http_client = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_seconds))
            .build()
            .unwrap();
        Self {
            http_client,
            username,
            password,
        }
    }

    pub async fn query_time_series(
        &self,
        vdt: ValidDateTime,
        parameters: Parameters<'_>,
        locations: Locations<'_>,
        optionals: Option<Optionals<'_>>,
    ) -> Result<Response, reqwest::Error> {
        let url_fragment = match optionals {
            None => {
                format!(
                    "{}--{}/{}/{}/{}",
                    vdt.start_date_time,
                    vdt.end_date_time.unwrap(),
                    parameters,
                    locations,
                    Format::CSV.to_string()
                )
            }
            Some(_) => {
                format!(
                    "{}--{}/{}/{}/{}?{}",
                    vdt.start_date_time,
                    vdt.end_date_time.unwrap(),
                    parameters,
                    locations,
                    Format::CSV.to_string(),
                    optionals.unwrap()
                )
            }
        };
        let response = self.do_http_get(url_fragment.as_str()).await?;
        Ok(response)
    }

    async fn do_http_get(&self, url_fragment: &str) -> Result<Response, reqwest::Error> {
        let full_url = build_url(url_fragment)
            .await
            .expect("URL fragment must be valid");

        println!(">>>>>>>>>> full_url: {}", full_url);

        let response = self
            .http_client
            .get(full_url)
            .basic_auth(&self.username, Some(String::from(&self.password)))
            .send()
            .await?;
        Ok(response)
    }
}

async fn build_url(url_fragment: &str) -> Result<Url, ParseError> {
    let base_url = Url::parse(DEFAULT_API_BASE_URL).expect("Base URL is known to be valid");
    let full_url = base_url.join(url_fragment)?;
    Ok(full_url)
}

#[cfg(test)]
mod tests {

    use crate::configuration::api_client::APIClient;
    use crate::connector_components::format::Format;
    use chrono::Local;

    #[tokio::test]
    async fn client_fires_get_request_to_base_url() {
        println!("##### client_fires_get_request_to_base_url:");

        // Change to correct username and password.
        let api_client = APIClient::new(
            "python-community".to_string(),
            "Umivipawe179".to_string(),
            10,
        );
        println!("api_client: {:?}", api_client);

        let now = Local::now();
        println!("now (local) {:?}", now);

        let url_fragment = &*format!(
            "{}{}{}",
            now.to_rfc3339(),
            "/t_2m:C/52.520551,13.461804/",
            Format::CSV.to_string()
        );
        println!("url_fragment: {:?}", url_fragment);

        let response = api_client.do_http_get(url_fragment).await.unwrap();
        // println!("response: {:?}", response);

        let status = format!("{}", response.status());
        println!("Status: {}", status);
        println!("Headers:\n{:#?}", response.headers());

        let body = response.text().await.unwrap();
        println!("Body:\n{}", body);

        assert_eq!(status, "200 OK");
        assert_ne!(body, "");
    }
}
