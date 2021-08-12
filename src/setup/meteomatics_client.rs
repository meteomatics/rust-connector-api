use reqwest::{Client, Response};
use url::{ParseError, Url};

const DEFAULT_API_BASE_URL: &str = "https://api.meteomatics.com";

pub struct MeteomaticsClient {
    http_client: Client,
    username: String,
    password: String,
}

#[allow(dead_code)]
impl MeteomaticsClient {
    pub fn new(username: String, password: String) -> Self {
        let http_client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap();
        Self {
            http_client,
            username,
            password,
        }
    }

    pub async fn do_http_get(&self, url_fragment: &str) -> Result<Response, reqwest::Error> {
        let full_url = build_url(url_fragment)
            .await
            .expect("URL fragment must be valid");
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

    use crate::setup::meteomatics_client::MeteomaticsClient;

    #[tokio::test]
    async fn client_fires_get_request_to_base_url() {
        // Change to correct username and password.
        let meteomatics_client =
            MeteomaticsClient::new("foo".to_string(), "bar".to_string());

        let response = meteomatics_client
            .do_http_get("get_init_date?model=ecmwf-ifs&valid_date=2021-08-12T19:00:00ZP1D:PT6H&parameters=t_2m:C,relative_humidity_2m:p")
            .await
            .unwrap();

        // println!("{:?}", response);

        let status = format!("{}", response.status());
        println!("Status: {}", status);
        println!("Headers:\n{:#?}", response.headers());

        let body = response.text().await.unwrap();
        println!("Body:\n{}", body);

        assert_eq!(status, "200 OK");
        assert_ne!(body, "");
    }
}
