//! The one HTTP request a webhook sink makes (section 9, D29). HTTP/1.1, one connection per
//! delivery, no redirects. The address connected to is one that was checked: a name is resolved
//! once, here, so a second answer can't point it somewhere else.

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use hmac::{Hmac, Mac};
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use sha2::Sha256;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// A response head larger than this isn't one we will read.
const MAX_RESPONSE_HEAD_BYTES: usize = 64 * 1024;

/// A parsed webhook URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookUrl {
    pub https: bool,
    /// Without brackets, for an IPv6 literal.
    pub host: String,
    pub port: u16,
    /// With its query, and at least `/`.
    pub path: String,
}

/// Only what a request line and `Host` header can carry safely, with no user info, which would
/// be a secret kept in a sink's config.
pub fn parse_url(url: &str) -> Option<WebhookUrl> {
    let (https, rest) = match url.split_once("://")? {
        ("https", rest) => (true, rest),
        ("http", rest) => (false, rest),
        _ => return None,
    };
    if !url.bytes().all(|byte| byte.is_ascii_graphic()) {
        return None;
    }
    let (authority, path) = match rest.find(['/', '?']) {
        Some(at) if rest.as_bytes()[at] == b'/' => (&rest[..at], rest[at..].to_owned()),
        Some(at) => (&rest[..at], format!("/{}", &rest[at..])),
        None => (rest, "/".to_owned()),
    };
    let path = path.split('#').next()?.to_owned();
    if authority.contains('@') {
        return None;
    }
    let default_port = if https { 443 } else { 80 };
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, after) = rest.split_once(']')?;
        host.parse::<Ipv6Addr>().ok()?;
        let port = match after.strip_prefix(':') {
            Some(port) => port.parse().ok()?,
            None if after.is_empty() => default_port,
            None => return None,
        };
        (host.to_owned(), port)
    } else {
        match authority.split_once(':') {
            Some((host, _)) if host.contains(':') => return None,
            Some((host, port)) => (host.to_owned(), port.parse().ok()?),
            None => (authority.to_owned(), default_port),
        }
    };
    if host.is_empty() || (host.contains(':') && host.parse::<Ipv6Addr>().is_err()) {
        return None;
    }
    Some(WebhookUrl {
        https,
        host,
        port,
        path,
    })
}

/// What a name resolves to, replaceable so that a test can have a public-looking name answer
/// with a private address. `lookup_host` otherwise.
pub trait SinkResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Vec<SocketAddr>>> + Send + 'a>>;
}

/// Addresses a webhook may not reach unless the node allows it: everything that isn't global
/// unicast, including the IPv4 embedded in an IPv6 form (section 9, D29).
pub fn is_private_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(v4) => is_private_v4(v4),
        IpAddr::V6(v6) => is_private_v6(v6),
    }
}

fn is_private_v4(address: Ipv4Addr) -> bool {
    let [a, b, c, _] = address.octets();
    address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_multicast()
        || address.is_broadcast()
        || a == 0
        || a >= 240
        // Shared address space, and the protocol, benchmark and documentation ranges.
        || (a == 100 && (64..128).contains(&b))
        || (a == 192 && b == 0 && (c == 0 || c == 2))
        || (a == 198 && (b == 18 || b == 19))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
}

fn is_private_v6(address: Ipv6Addr) -> bool {
    let octets = address.octets();
    let segments = address.segments();
    let embedded =
        |at: usize| Ipv4Addr::new(octets[at], octets[at + 1], octets[at + 2], octets[at + 3]);
    // ::/96 holds ::, ::1 and the old IPv4-compatible form, and none of them is global.
    if segments[..6] == [0; 6] {
        return true;
    }
    // IPv4-mapped.
    if segments[..5] == [0; 5] && segments[5] == 0xffff {
        return is_private_v4(embedded(12));
    }
    // NAT64: reaches the IPv4 inside it.
    if segments[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
        return is_private_v4(embedded(12));
    }
    // 6to4: likewise.
    if segments[0] == 0x2002 {
        return is_private_v4(embedded(2));
    }
    segments[0] & 0xfe00 == 0xfc00 // unique local
        || segments[0] & 0xffc0 == 0xfe80 // link local
        || segments[0] & 0xffc0 == 0xfec0 // site local, now deprecated
        || segments[0] & 0xff00 == 0xff00 // multicast
        || (segments[0] == 0x2001 && segments[1] == 0xdb8) // documentation
        || (segments[0] == 0x0100 && segments[1..4] == [0, 0, 0]) // discard
}

/// How one attempt ended, before the status is judged.
#[derive(Debug)]
pub enum PostError {
    /// Never worth another try.
    Blocked(String),
    /// A network failure, which may pass.
    Failed(String),
}

pub struct PostParts<'a> {
    pub url: &'a WebhookUrl,
    pub content_type: &'a str,
    /// Names and values come from this crate and hold only digits and hex.
    pub headers: &'a [(&'static str, String)],
    pub body: &'a [u8],
    pub allow_private: bool,
    pub resolver: Option<&'a Arc<dyn SinkResolver>>,
    pub tls: &'a Arc<ClientConfig>,
    pub timeout_ms: u64,
}

/// The status of the final response, or why there wasn't one. All of it is under `timeout_ms`.
pub async fn post(parts: PostParts<'_>) -> Result<u16, PostError> {
    let timeout = Duration::from_millis(parts.timeout_ms);
    match tokio::time::timeout(timeout, post_inner(&parts)).await {
        Ok(result) => result,
        Err(_) => Err(PostError::Failed("timed out".to_owned())),
    }
}

async fn resolved(parts: &PostParts<'_>) -> Result<Vec<SocketAddr>, PostError> {
    let (host, port) = (parts.url.host.as_str(), parts.url.port);
    let addresses = if let Ok(ip) = host.parse::<IpAddr>() {
        vec![SocketAddr::new(ip, port)]
    } else if let Some(resolver) = parts.resolver {
        resolver
            .resolve(host, port)
            .await
            .map_err(|error| PostError::Failed(format!("resolving {host}: {error}")))?
    } else {
        tokio::net::lookup_host((host, port))
            .await
            .map_err(|error| PostError::Failed(format!("resolving {host}: {error}")))?
            .collect()
    };
    if addresses.is_empty() {
        return Err(PostError::Failed(format!("{host} resolves to nothing")));
    }
    if !parts.allow_private
        && let Some(private) = addresses
            .iter()
            .find(|address| is_private_address(address.ip()))
    {
        return Err(PostError::Blocked(format!(
            "{host} resolves to the private address {}",
            private.ip()
        )));
    }
    Ok(addresses)
}

async fn post_inner(parts: &PostParts<'_>) -> Result<u16, PostError> {
    let addresses = resolved(parts).await?;
    let mut failure = String::new();
    let mut socket = None;
    for address in addresses {
        match TcpStream::connect(address).await {
            Ok(connected) => {
                socket = Some(connected);
                break;
            }
            Err(error) => failure = format!("connecting to {address}: {error}"),
        }
    }
    let Some(socket) = socket else {
        return Err(PostError::Failed(failure));
    };
    let _ = socket.set_nodelay(true);
    let failed = |error: std::io::Error| PostError::Failed(error.to_string());
    if parts.url.https {
        let name = ServerName::try_from(parts.url.host.clone())
            .map_err(|error| PostError::Blocked(format!("server name: {error}")))?;
        let tls = TlsConnector::from(Arc::clone(parts.tls))
            .connect(name, socket)
            .await
            .map_err(failed)?;
        exchange(tls, parts).await.map_err(failed)
    } else {
        exchange(socket, parts).await.map_err(failed)
    }
}

async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
    mut socket: S,
    parts: &PostParts<'_>,
) -> std::io::Result<u16> {
    let host = if parts.url.host.contains(':') {
        format!("[{}]", parts.url.host)
    } else {
        parts.url.host.clone()
    };
    let mut head = format!(
        "POST {} HTTP/1.1\r\nHost: {host}:{}\r\nContent-Type: {}\r\nContent-Length: {}\r\n\
         Connection: close\r\n",
        parts.url.path,
        parts.url.port,
        parts.content_type,
        parts.body.len()
    );
    for (name, value) in parts.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(parts.body).await?;
    socket.flush().await?;
    read_status(&mut socket).await
}

/// The first response that isn't `1xx`. Its body isn't read: only the status counts.
async fn read_status<S: AsyncRead + Unpin>(socket: &mut S) -> std::io::Result<u16> {
    let bad = |text: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, text.to_owned());
    let mut buffer = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        while let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            let line_end = buffer.iter().position(|byte| *byte == b'\r').unwrap_or(end);
            let line = std::str::from_utf8(&buffer[..line_end])
                .map_err(|_| bad("a status line isn't text"))?;
            let mut fields = line.split(' ');
            let version = fields.next().unwrap_or("");
            if !version.starts_with("HTTP/1.") {
                return Err(bad("not an HTTP/1 response"));
            }
            let status = fields
                .next()
                .and_then(|status| status.parse::<u16>().ok())
                .ok_or_else(|| bad("no status in the response"))?;
            if (100..200).contains(&status) {
                buffer.drain(..end + 4);
                continue;
            }
            return Ok(status);
        }
        if buffer.len() > MAX_RESPONSE_HEAD_BYTES {
            return Err(bad("the response head is too long"));
        }
        let read = socket.read(&mut chunk).await?;
        if read == 0 {
            return Err(bad("the connection closed before a response"));
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
}

/// `X-Signature`: HMAC-SHA256 of the body, in lowercase hex (D29).
pub fn signature_hex(secret: &str, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC takes a key of any length");
    mac.update(body);
    let mut out = String::with_capacity(64);
    for byte in mac.finalize().into_bytes() {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// What a status means for the delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusClass {
    Delivered,
    Retry,
    Permanent,
}

/// 2xx delivered; 408, 425, 429 and 5xx may pass. Anything else won't, and a redirect isn't
/// followed, since its target was never checked (D29).
pub fn classify_status(status: u16) -> StatusClass {
    match status {
        200..=299 => StatusClass::Delivered,
        408 | 425 | 429 | 500..=599 => StatusClass::Retry,
        _ => StatusClass::Permanent,
    }
}

/// Webhook TLS: the Mozilla roots, and any certificates the node adds. TLS 1.3 only, which is
/// all this build's `rustls` has (D199).
pub fn tls_client_config(extra_roots_pem: Option<&[u8]>) -> std::io::Result<Arc<ClientConfig>> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(mut pem) = extra_roots_pem {
        for cert in rustls_pemfile::certs(&mut pem) {
            roots.add(cert?).map_err(std::io::Error::other)?;
        }
    }
    let provider = rustls::crypto::ring::default_provider();
    let config = ClientConfig::builder_with_provider(provider.into())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(std::io::Error::other)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_parse_or_are_refused() {
        let url = parse_url("https://example.com:8443/a/b?c=d#e").unwrap();
        assert_eq!(
            url,
            WebhookUrl {
                https: true,
                host: "example.com".to_owned(),
                port: 8443,
                path: "/a/b?c=d".to_owned(),
            }
        );
        assert_eq!(parse_url("http://h").unwrap().path, "/");
        assert_eq!(parse_url("http://h").unwrap().port, 80);
        assert_eq!(parse_url("https://h?x=1").unwrap().path, "/?x=1");
        assert_eq!(parse_url("https://[::1]:9/x").unwrap().host, "::1");
        for bad in [
            "ftp://x",
            "x",
            "http://",
            "http://user:pass@h/",
            "http://h/a b",
            "http://h/\r\nX: y",
            "http://[nope]/",
            "http://[::1]x/",
            "http://h:notaport/",
        ] {
            assert!(parse_url(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn private_ranges_are_private() {
        for private in [
            "0.0.0.0",
            "10.0.0.1",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "172.31.255.255",
            "192.0.0.1",
            "192.0.2.1",
            "192.168.1.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "::127.0.0.1",
            "::ffff:10.0.0.1",
            "::ffff:127.0.0.1",
            "64:ff9b::a00:1",
            "2002:0a00:0001::1",
            "fc00::1",
            "fd12::1",
            "fe80::1",
            "fec0::1",
            "ff02::1",
            "2001:db8::1",
            "100::1",
        ] {
            let address: IpAddr = private.parse().unwrap();
            assert!(is_private_address(address), "{private}");
        }
        for public in [
            "1.1.1.1",
            "8.8.8.8",
            "100.63.255.255",
            "100.128.0.1",
            "172.15.0.1",
            "172.32.0.1",
            "192.169.0.1",
            "2606:4700:4700::1111",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "2002:0808:0808::1",
        ] {
            let address: IpAddr = public.parse().unwrap();
            assert!(!is_private_address(address), "{public}");
        }
    }

    #[test]
    fn statuses_sort_into_three() {
        assert_eq!(classify_status(200), StatusClass::Delivered);
        assert_eq!(classify_status(204), StatusClass::Delivered);
        for retry in [408, 425, 429, 500, 502, 503, 599] {
            assert_eq!(classify_status(retry), StatusClass::Retry, "{retry}");
        }
        for permanent in [301, 302, 400, 401, 403, 404, 410, 422] {
            assert_eq!(
                classify_status(permanent),
                StatusClass::Permanent,
                "{permanent}"
            );
        }
    }

    #[test]
    fn the_signature_matches_a_known_vector() {
        // RFC 4231, test case 2.
        assert_eq!(
            signature_hex("Jefe", b"what do ya want for nothing?"),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }
}
