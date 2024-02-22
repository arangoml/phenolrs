use crate::input::load_request::DatabaseConfiguration;
use reqwest::RequestBuilder;
use std::fs::File;
use std::io::Read;

pub fn build_client(
    use_tls: bool,
    tls_cert_path: &Option<String>,
) -> Result<reqwest::Client, String> {
    let builder = reqwest::Client::builder();
    if use_tls {
        let mut client_builder = builder
            .use_rustls_tls()
            .min_tls_version(reqwest::tls::Version::TLS_1_2)
            .https_only(true);

        if let Some(cert_path) = tls_cert_path {
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
            client_builder = client_builder.add_root_certificate(cert.unwrap());
        }
        let client = client_builder.build();
        if let Err(err) = client {
            return Err(format!("Error message from request builder: {:?}", err));
        }
        Ok(client.unwrap())
    } else {
        let client = builder
            //.connection_verbose(true)
            //.http2_prior_knowledge()
            .build();
        if let Err(err) = client {
            return Err(format!("Error message from request builder: {:?}", err));
        }
        Ok(client.unwrap())
    }
}

pub fn handle_auth(
    request_builder: RequestBuilder,
    db_config: &DatabaseConfiguration,
) -> RequestBuilder {
    match &db_config.jwt_token {
        Some(token) => request_builder.bearer_auth(token),
        None => handle_basic_auth(request_builder, &db_config),
    }
}

fn handle_basic_auth(
    request_builder: RequestBuilder,
    db_config: &&DatabaseConfiguration,
) -> RequestBuilder {
    match &db_config.username {
        Some(username) => request_builder.basic_auth(username, db_config.password.as_ref()),
        None => {
            // proceed without authentication
            request_builder
        }
    }
}
