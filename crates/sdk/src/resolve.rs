//! One DNS answer per client. Resolving for every connection puts a resolver in the path of each
//! one, and a burst of them then waits on its retries.

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::net::TcpStream;

#[derive(Clone, Default)]
pub(crate) struct Resolver {
    addresses: Arc<Mutex<Vec<SocketAddr>>>,
}

impl Resolver {
    /// Tries the addresses it has, and resolves again only if none of them answers.
    pub(crate) async fn connect(&self, host: &str, port: u16) -> io::Result<TcpStream> {
        let known = self
            .addresses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for address in known {
            if let Ok(socket) = TcpStream::connect(address).await {
                return Ok(socket);
            }
        }
        let found: Vec<SocketAddr> = tokio::net::lookup_host((host, port)).await?.collect();
        let socket = TcpStream::connect(found.as_slice()).await?;
        *self
            .addresses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = found;
        Ok(socket)
    }
}
