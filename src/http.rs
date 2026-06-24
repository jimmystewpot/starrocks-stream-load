use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use reqwest::{Client, Method, Response};
use std::sync::atomic::{AtomicUsize, Ordering};
use url::Url;

use crate::config::StreamLoadConfig;
use crate::error::{Error, Result};

pub struct StarRocksHttpClient {
    client: Client,
    config: StreamLoadConfig,
    current_pos: AtomicUsize,
    auth_header: Option<HeaderValue>,
    parsed_urls: Vec<Url>,
}

impl StarRocksHttpClient {
    pub fn new(config: StreamLoadConfig) -> Result<Self> {
        // Disable automatic redirect follow, as we will handle 307 manually
        // to preserve headers and body repeatability.
        let client = Client::builder()
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        let auth_header = if !config.username.is_empty() {
            let auth = format!(
                "Basic {}",
                base64_encode(&format!(
                    "{}:{}",
                    config.username,
                    config.password.as_deref().unwrap_or("")
                ))
            );
            HeaderValue::from_str(&auth).ok()
        } else {
            None
        };

        let parsed_urls = config
            .load_urls
            .iter()
            .map(|raw_url| {
                let url_str = if raw_url.contains("://") {
                    raw_url.clone()
                } else {
                    format!("http://{raw_url}")
                };
                Url::parse(&url_str)
            })
            .collect::<std::result::Result<Vec<Url>, url::ParseError>>()?;

        Ok(Self {
            client,
            config,
            current_pos: AtomicUsize::new(0),
            auth_header,
            parsed_urls,
        })
    }

    #[must_use]
    pub fn config(&self) -> &StreamLoadConfig {
        &self.config
    }

    pub fn get_available_fe(&self) -> String {
        self.get_available_fe_url().to_string()
    }

    pub fn get_available_fe_url(&self) -> &Url {
        let urls = &self.parsed_urls;
        if urls.is_empty() {
            static DEFAULT_URL: std::sync::OnceLock<Url> = std::sync::OnceLock::new();
            return DEFAULT_URL.get_or_init(|| Url::parse("http://127.0.0.1:8030").unwrap());
        }

        let size = urls.len();
        let pos = self.current_pos.load(Ordering::Relaxed) % size;
        &urls[pos]
    }

    pub async fn execute_request(
        &self,
        method: Method,
        path: &str,
        mut headers: HeaderMap,
        body: Option<bytes::Bytes>,
    ) -> Result<Response> {
        if self.parsed_urls.is_empty() {
            return Err(Error::Transaction("No configured load URLs".to_string()));
        }

        if !headers.contains_key(AUTHORIZATION) {
            if let Some(ref auth) = self.auth_header {
                headers.insert(AUTHORIZATION, auth.clone());
            }
        }

        let mut last_err = None;
        let max_attempts = self.config.max_retries + 1;

        for attempt in 1..=max_attempts {
            let fe_url = self.get_available_fe_url();
            let mut url = fe_url.join(path)?;

            let mut builder = self
                .client
                .request(method.clone(), url.clone())
                .headers(headers.clone());
            if let Some(ref data) = body {
                builder = builder.body(data.clone());
            }

            match builder.send().await {
                Ok(response) => {
                    // Handle 307 Redirects manually to preserve headers and body
                    if response.status() == reqwest::StatusCode::TEMPORARY_REDIRECT {
                        let location_url = response
                            .headers()
                            .get(reqwest::header::LOCATION)
                            .and_then(|loc| loc.to_str().ok())
                            .and_then(|loc_str| {
                                Url::parse(loc_str).or_else(|_| url.join(loc_str)).ok()
                            });

                        if let Some(new_url) = location_url {
                            url = new_url;
                            let mut builder = self
                                .client
                                .request(method.clone(), url.clone())
                                .headers(headers.clone());
                            if let Some(ref data) = body {
                                builder = builder.body(data.clone());
                            }
                            match builder.send().await {
                                Ok(redirect_response) => {
                                    return Ok(redirect_response);
                                }
                                Err(err) => {
                                    last_err = Some(Error::from(err));
                                }
                            }
                        } else {
                            return Ok(response);
                        }
                    } else {
                        return Ok(response);
                    }
                }
                Err(err) => {
                    last_err = Some(Error::from(err));
                }
            }

            // Route to next FE candidate on retry
            self.current_pos.fetch_add(1, Ordering::Relaxed);

            if attempt < max_attempts {
                tracing::warn!(
                    "Request to FE {} failed (attempt {}/{}): {:?}. Retrying in {:?}",
                    fe_url,
                    attempt,
                    max_attempts,
                    last_err,
                    self.config.retry_interval
                );
                tokio::time::sleep(self.config.retry_interval).await;
            }
        }

        Err(last_err.unwrap_or_else(|| {
            Error::Transaction("HTTP request failed after max retries".to_string())
        }))
    }

    pub async fn get_request(&self, url: &str) -> Result<Response> {
        let mut builder = self.client.get(url);
        if let Some(ref auth) = self.auth_header {
            builder = builder.header(AUTHORIZATION, auth.clone());
        }
        let response = builder.send().await?;
        Ok(response)
    }
}

#[must_use]
fn base64_encode(input: &str) -> String {
    const CHARSET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut result = String::with_capacity(bytes.len().div_ceil(3) * 4);
    let mut i = 0;

    while i < bytes.len() {
        let b0 = bytes[i];
        let b1 = if i + 1 < bytes.len() {
            Some(bytes[i + 1])
        } else {
            None
        };
        let b2 = if i + 2 < bytes.len() {
            Some(bytes[i + 2])
        } else {
            None
        };

        let val =
            (u32::from(b0) << 16) | (u32::from(b1.unwrap_or(0)) << 8) | u32::from(b2.unwrap_or(0));

        let enc0 = (val >> 18) & 63;
        let enc1 = (val >> 12) & 63;
        let enc2 = if b1.is_some() {
            Some((val >> 6) & 63)
        } else {
            None
        };
        let enc3 = if b2.is_some() { Some(val & 63) } else { None };

        result.push(CHARSET[enc0 as usize] as char);
        result.push(CHARSET[enc1 as usize] as char);
        if let Some(e2) = enc2 {
            result.push(CHARSET[e2 as usize] as char);
        } else {
            result.push('=');
        }
        if let Some(e3) = enc3 {
            result.push(CHARSET[e3 as usize] as char);
        } else {
            result.push('=');
        }

        i += 3;
    }

    result
}
