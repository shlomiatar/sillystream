//! TLS 1.3 and nothing else (section 11.1), with a certificate from PEM files.

use std::io;

use rustls::ServerConfig;

/// A server config that speaks TLS 1.3 only. No client certificates, no resumption and no
/// 0-RTT (section 15), which also saves each connection the memory they would use.
pub fn tls13_only_config(cert_pem: &[u8], key_pem: &[u8]) -> io::Result<ServerConfig> {
    let certs = rustls_pemfile::certs(&mut &*cert_pem).collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no certificate in the certificate file",
        ));
    }
    let key = rustls_pemfile::private_key(&mut &*key_pem)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "no private key in the key file",
        )
    })?;
    let provider = rustls::crypto::ring::default_provider();
    let mut config = ServerConfig::builder_with_provider(provider.into())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(io::Error::other)?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    config.send_tls13_tickets = 0;
    config.max_early_data_size = 0;
    config.session_storage = std::sync::Arc::new(rustls::server::NoServerSessionStorage {});
    Ok(config)
}
