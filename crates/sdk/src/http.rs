//! A small HTTP/1.1 client for the admin routes of section 11.2. One connection per request:
//! these aren't hot.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::SdkError;
use crate::client::{parse_address, tls_connector};

#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    pub fn is_ok(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

#[derive(Clone)]
pub struct HttpClient {
    host: String,
    port: u16,
    token: String,
    connector: TlsConnector,
}

impl HttpClient {
    /// `url` as `ClientConfig::url`.
    pub fn new(url: &str, ca_pem: &[u8], token: &str) -> Result<HttpClient, SdkError> {
        let (host, port) = parse_address(url)?;
        Ok(HttpClient {
            host,
            port,
            token: token.to_owned(),
            connector: tls_connector(ca_pem).map(Arc::unwrap_or_clone)?,
        })
    }

    pub async fn request(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Result<Response, SdkError> {
        let failed = |error: std::io::Error| SdkError::Connect(error.to_string());
        let socket = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(failed)?;
        let name = rustls::pki_types::ServerName::try_from(self.host.clone())
            .map_err(|error| SdkError::Config(error.to_string()))?;
        let mut tls = self.connector.connect(name, socket).await.map_err(failed)?;
        let mut head = format!(
            "{method} {path} HTTP/1.1\r\nhost: {}\r\nauthorization: Bearer {}\r\n\
             connection: close\r\ncontent-length: {}\r\n",
            self.host,
            self.token,
            body.len()
        );
        for (name, value) in headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }
        head.push_str("\r\n");
        tls.write_all(head.as_bytes()).await.map_err(failed)?;
        tls.write_all(body).await.map_err(failed)?;
        tls.flush().await.map_err(failed)?;
        let mut bytes = Vec::new();
        // A close without TLS's own close message is what some servers do, and the length says
        // whether anything is missing.
        if let Err(error) = tls.read_to_end(&mut bytes).await
            && error.kind() != std::io::ErrorKind::UnexpectedEof
        {
            return Err(failed(error));
        }
        parse_response(&bytes)
    }
}

fn parse_response(bytes: &[u8]) -> Result<Response, SdkError> {
    let bad = || SdkError::Connect("a malformed HTTP response".to_owned());
    let end = bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(bad)?;
    let head = std::str::from_utf8(&bytes[..end]).map_err(|_| bad())?;
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .and_then(|code| code.parse().ok())
        .ok_or_else(bad)?;
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    let body = bytes[end + 4..].to_vec();
    let response = Response {
        status,
        headers,
        body,
    };
    if let Some(length) = response.header("content-length")
        && length.parse::<usize>().ok() != Some(response.body.len())
    {
        return Err(bad());
    }
    Ok(response)
}
