use crate::client::config::ClientConfig;
use reqwest::Client;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use std::fs::File;
use std::io::Read;

pub fn build_client(
    config: &ClientConfig,
) -> Result<reqwest_middleware::ClientWithMiddleware, String> {
    let mut client_builder = reqwest::Client::builder();
    if config.use_tls {
        client_builder = client_builder
            .min_tls_version(reqwest::tls::Version::TLS_1_2)
            .https_only(true);

        if let Some(cert_path) = &config.tls_cert {
            let mut cert_buf = vec![];
            let file_open = File::open(cert_path);
            if let Err(err) = file_open {
                return Err(format!(
                    "Error message from reading TLS certificate: {:?}",
                    err
                ));
            }
            let cert_read = file_open.unwrap().read_to_end(&mut cert_buf);
            if let Err(err) = cert_read {
                return Err(format!(
                    "Error message from reading TLS certificate: {:?}",
                    err
                ));
            }
            let cert = reqwest::Certificate::from_pem(&cert_buf);
            if let Err(err) = cert {
                return Err(format!("Error message from request builder: {:?}", err));
            }
            client_builder = client_builder
                .add_root_certificate(cert.unwrap())
                .use_rustls_tls();
        }
        let client = client_builder.build();
        if let Err(err) = client {
            return Err(format!("Error message from request builder: {:?}", err));
        }
        let client = client_with_retries(&config.n_retries, client.unwrap());
        Ok(client)
    } else {
        let client = client_builder.build();
        if let Err(err) = client {
            return Err(format!("Error message from request builder: {:?}", err));
        }
        let client = client_with_retries(&config.n_retries, client.unwrap());
        Ok(client)
    }
}

pub fn client_with_retries(n_retries: &u32, client: Client) -> ClientWithMiddleware {
    let retry_policy = ExponentialBackoff::builder()
        .retry_bounds(
            std::time::Duration::from_millis(30),
            std::time::Duration::from_millis(3000),
        )
        .build_with_max_retries(*n_retries);
    let retry_middleware = RetryTransientMiddleware::new_with_policy(retry_policy);
    ClientBuilder::new(client).with(retry_middleware).build()
}
