//! The one HTTP request a webhook sink makes (section 9, D29). HTTP/1.1, no redirects. The address
//! connected to is one that was checked: a name is resolved once, here, so a second answer can't
//! point it somewhere else. A connection whose response ended cleanly is handed back, for the
//! sink's next delivery to the same URL to use again.

use std::future::Future;
use std::io::Write;
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
use tokio_rustls::client::TlsStream;

/// A response head larger than this isn't one we will read.
const MAX_RESPONSE_HEAD_BYTES: usize = 64 * 1024;
/// A response body up to this is read past, so the connection can be used again. A longer one
/// closes it.
const MAX_DRAINED_BODY_BYTES: usize = 64 * 1024;

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
    let rest = rest.split('#').next()?;
    let (authority, path) = match rest.find(['/', '?']) {
        Some(at) if rest.as_bytes()[at] == b'/' => (&rest[..at], rest[at..].to_owned()),
        Some(at) => (&rest[..at], format!("/{}", &rest[at..])),
        None => (rest, "/".to_owned()),
    };
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
        || (a == 192 && b == 88 && c == 99) // the old 6to4 relay
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
    // Only 2000::/3 is global unicast. This takes in unique local, link local, multicast,
    // discard, the IPv4-translated form, local NAT64 and SRv6, whatever is added to them.
    segments[0] & 0xe000 != 0x2000
        // IETF special purpose, with Teredo, benchmarking and ORCHID.
        || (segments[0] == 0x2001 && segments[1] < 0x200)
        // Documentation.
        || (segments[0] == 0x2001 && segments[1] == 0xdb8)
        || (segments[0] == 0x3fff && segments[1] < 0x1000)
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
    /// `Content-Encoding`, when the body is compressed.
    pub content_encoding: Option<&'a str>,
    /// Names and values come from this crate and hold only digits and hex.
    pub headers: &'a [(&'static str, String)],
    pub body: &'a [u8],
    pub allow_private: bool,
    pub resolver: Option<&'a Arc<dyn SinkResolver>>,
    pub tls: &'a Arc<ClientConfig>,
    pub timeout_ms: u64,
}

/// A connection kept from an earlier POST, to the URL it was opened for.
pub struct Conn {
    url: WebhookUrl,
    socket: Socket,
}

enum Socket {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl Conn {
    /// Nothing should arrive on an idle connection: a close, or anything else, means it can't be
    /// used again. This reads at most a byte, without waiting.
    fn is_idle(&self) -> bool {
        let tcp = match &self.socket {
            Socket::Plain(tcp) => tcp,
            Socket::Tls(tls) => tls.get_ref().0,
        };
        matches!(tcp.try_read(&mut [0u8; 1]), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    }
}

/// The status of the final response, or why there wasn't one, and the connection if it can be
/// used again. `reuse` is tried first when it was opened for the same URL and is still idle, and
/// a fresh connection after it if it fails. All of it is under `timeout_ms`.
pub async fn post(
    parts: PostParts<'_>,
    reuse: Option<Conn>,
) -> (Result<u16, PostError>, Option<Conn>) {
    let timeout = Duration::from_millis(parts.timeout_ms);
    match tokio::time::timeout(timeout, post_inner(&parts, reuse)).await {
        Ok(result) => result,
        Err(_) => (Err(PostError::Failed("timed out".to_owned())), None),
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

async fn post_inner(
    parts: &PostParts<'_>,
    reuse: Option<Conn>,
) -> (Result<u16, PostError>, Option<Conn>) {
    if let Some(conn) = reuse.filter(|conn| conn.url == *parts.url && conn.is_idle()) {
        // The other end may have closed it as the request went: at-least-once lets it go again.
        if let Ok((status, kept)) = exchange(conn.socket, parts).await {
            return (
                Ok(status),
                kept.map(|socket| Conn {
                    url: conn.url,
                    socket,
                }),
            );
        }
    }
    let socket = match connect(parts).await {
        Ok(socket) => socket,
        Err(error) => return (Err(error), None),
    };
    match exchange(socket, parts).await {
        Ok((status, kept)) => (
            Ok(status),
            kept.map(|socket| Conn {
                url: parts.url.clone(),
                socket,
            }),
        ),
        Err(error) => (Err(PostError::Failed(error.to_string())), None),
    }
}

async fn connect(parts: &PostParts<'_>) -> Result<Socket, PostError> {
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
    if !parts.url.https {
        return Ok(Socket::Plain(socket));
    }
    let name = ServerName::try_from(parts.url.host.clone())
        .map_err(|error| PostError::Blocked(format!("server name: {error}")))?;
    // The config's session cache resumes TLS with a host it has spoken to before.
    let tls = TlsConnector::from(Arc::clone(parts.tls))
        .connect(name, socket)
        .await
        .map_err(|error| PostError::Failed(error.to_string()))?;
    Ok(Socket::Tls(Box::new(tls)))
}

/// One request and its response, and the socket back if it can carry another.
async fn exchange(socket: Socket, parts: &PostParts<'_>) -> std::io::Result<(u16, Option<Socket>)> {
    match socket {
        Socket::Plain(mut tcp) => {
            let (status, reusable) = exchange_on(&mut tcp, parts).await?;
            Ok((status, reusable.then_some(Socket::Plain(tcp))))
        }
        Socket::Tls(mut tls) => {
            let (status, reusable) = exchange_on(&mut *tls, parts).await?;
            Ok((status, reusable.then_some(Socket::Tls(tls))))
        }
    }
}

async fn exchange_on<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut S,
    parts: &PostParts<'_>,
) -> std::io::Result<(u16, bool)> {
    let host = if parts.url.host.contains(':') {
        format!("[{}]", parts.url.host)
    } else {
        parts.url.host.clone()
    };
    let mut head = format!(
        "POST {} HTTP/1.1\r\nHost: {host}:{}\r\nContent-Type: {}\r\nContent-Length: {}\r\n",
        parts.url.path,
        parts.url.port,
        parts.content_type,
        parts.body.len()
    );
    if let Some(encoding) = parts.content_encoding {
        head.push_str(&format!("Content-Encoding: {encoding}\r\n"));
    }
    for (name, value) in parts.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(parts.body).await?;
    socket.flush().await?;
    read_response(socket).await
}

/// The status of the first response that isn't `1xx`, and whether the connection can carry
/// another request. Only the status counts: a short body with a length is read past and dropped,
/// and anything else leaves the connection to be closed.
async fn read_response<S: AsyncRead + Unpin>(socket: &mut S) -> std::io::Result<(u16, bool)> {
    let bad = |text: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, text.to_owned());
    let mut buffer = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        while let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            let head = std::str::from_utf8(&buffer[..end])
                .map_err(|_| bad("a response head isn't text"))?;
            let mut lines = head.split("\r\n");
            let mut fields = lines.next().unwrap_or("").split(' ');
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
            let mut reusable = version == "HTTP/1.1";
            let mut length = None;
            for line in lines {
                let Some((name, value)) = line.split_once(':') else {
                    continue;
                };
                let (name, value) = (name.trim().to_ascii_lowercase(), value.trim());
                match name.as_str() {
                    "connection" if value.eq_ignore_ascii_case("close") => reusable = false,
                    "transfer-encoding" => reusable = false,
                    "content-length" => length = value.parse::<usize>().ok(),
                    _ => {}
                }
            }
            let length = match length {
                Some(length) => length,
                None if status == 204 || status == 304 => 0,
                // The body runs to the close.
                None => return Ok((status, false)),
            };
            if !reusable || length > MAX_DRAINED_BODY_BYTES {
                return Ok((status, false));
            }
            let total = end + 4 + length;
            while buffer.len() < total {
                let read = socket.read(&mut chunk).await?;
                if read == 0 {
                    return Ok((status, false));
                }
                buffer.extend_from_slice(&chunk[..read]);
            }
            // Bytes past the response were never asked for.
            return Ok((status, buffer.len() == total));
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

/// A delivery's body: its records one after another, compressed as the sink asks.
pub fn encode_body(parts: &[bytes::Bytes], compression: Option<&str>) -> std::io::Result<Vec<u8>> {
    let length = parts.iter().map(|part| part.len()).sum::<usize>();
    match compression {
        None => Ok(parts.concat()),
        Some("zstd") => {
            let mut encoder = zstd::Encoder::new(Vec::with_capacity(length / 2), 1)?;
            for part in parts {
                encoder.write_all(part)?;
            }
            encoder.finish()
        }
        Some("gzip") => {
            let out = Vec::with_capacity(length / 2);
            let mut encoder = flate2::write::GzEncoder::new(out, flate2::Compression::fast());
            for part in parts {
                encoder.write_all(part)?;
            }
            encoder.finish()
        }
        Some(other) => Err(std::io::Error::other(format!(
            "no compression called {other}"
        ))),
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
    fn a_fragment_isnt_part_of_the_host() {
        let url = parse_url("http://example.com#frag").unwrap();
        assert_eq!((&*url.host, &*url.path), ("example.com", "/"));
        let url = parse_url("https://example.com:8443/a?b=c#frag").unwrap();
        assert_eq!(
            (&*url.host, url.port, &*url.path),
            ("example.com", 8443, "/a?b=c")
        );
        let url = parse_url("http://example.com?x=1").unwrap();
        assert_eq!(&*url.path, "/?x=1");
    }

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
