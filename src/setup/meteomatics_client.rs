use reqwest::{Client, Response};

pub struct MeteomaticsClient {
    http_client: Client,
    base_url: String,
    username: String,
    password: String,
}

impl MeteomaticsClient {
    pub fn new(username: String, password: String) -> Self {
        let http_client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap();
        Self {
            http_client,
            base_url: "https://api.meteomatics.com/".to_string(),
            username,
            password,
        }
    }

    pub async fn get(&self, path_fragment: String) -> Result<Response, reqwest::Error> {
        let full_url = format!("{}{}", &self.base_url, path_fragment);
        let response = self
            .http_client
            .get(full_url)
            .basic_auth(&self.username, Some(String::from(&self.password)))
            .send()
            .await?;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {

    use crate::setup::meteomatics_client::MeteomaticsClient;

    #[tokio::test]
    async fn client_fires_get_request_to_base_url() {
        let meteomatics_client = MeteomaticsClient::new("foo".to_string(), "bar".to_string());

        let response = meteomatics_client
            .get("get_init_date?model=ecmwf-ifs&valid_date=2021-08-09T19:00:00ZP1D:PT6H&parameters=t_2m:C,relative_humidity_2m:p".to_string())
            .await
            .unwrap();

        // println!("{:?}", response);

        let body = response.text().await.unwrap();
        println!("Body:\n{}", body);

        assert_ne!(body, "");
    }
}
