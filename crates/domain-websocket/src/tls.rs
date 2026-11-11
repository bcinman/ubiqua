//! The bytes under a connection, plain or under TLS for `wss://`, split into an input and an
//! output on the same socket.
//!
//! A TLS session can't be split, so both halves share it behind a lock. They hold the lock only
//! to move bytes through the session: the input waits on the socket without it, so a write never
//! waits on a read.

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

/// Which servers a client trusts on `wss://`. [`connect`](crate::connect) uses
/// [`system`](Self::system) unless given another.
#[derive(Clone)]
pub struct ClientTls(Arc<ClientConfig>);

impl ClientTls {
    /// Trusts the certificate authorities the operating system trusts, as a browser does. Loaded
    /// once.
    pub fn system() -> io::Result<Self> {
        static SYSTEM: OnceLock<Result<ClientTls, String>> = OnceLock::new();
        let system = SYSTEM.get_or_init(|| {
            let mut roots = RootCertStore::empty();
            roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
            if roots.is_empty() {
                return Err("the system trusts no certificate authorities".into());
            }
            Self::with_roots(roots).map_err(|e| e.to_string())
        });
        system.clone().map_err(|e| invalid(&e))
    }

    /// Trusts only the certificate authorities in `pem`, such as your own, for a server whose
    /// certificate no public authority signed.
    pub fn trusting(pem: &[u8]) -> io::Result<Self> {
        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(pem) {
            let cert = cert.map_err(|e| invalid(&format!("a trusted certificate: {e}")))?;
            roots.add(cert).map_err(|e| invalid(&format!("a trusted certificate: {e}")))?;
        }
        if roots.is_empty() {
            return Err(invalid("no certificates in the PEM"));
        }
        Self::with_roots(roots)
    }

    fn with_roots(roots: RootCertStore) -> io::Result<Self> {
        let config = ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(tls_error)?
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Self(Arc::new(config)))
    }
}

/// A server's certificate and key, for serving `wss://`.
#[derive(Clone)]
pub struct ServerTls(Arc<ServerConfig>);

impl ServerTls {
    /// `chain` is the server's certificate followed by any intermediates, and `key` its private
    /// key, both PEM, as a certificate authority such as Let's Encrypt issues them.
    pub fn from_pem(chain: &[u8], key: &[u8]) -> io::Result<Self> {
        let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(chain)
            .collect::<Result<_, _>>()
            .map_err(|e| invalid(&format!("the certificate chain: {e}")))?;
        if chain.is_empty() {
            return Err(invalid("no certificates in the chain"));
        }
        let key = PrivateKeyDer::from_pem_slice(key).map_err(|e| invalid(&format!("the private key: {e}")))?;
        let config = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(tls_error)?
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .map_err(tls_error)?;
        Ok(Self(Arc::new(config)))
    }
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

type Session = Arc<Mutex<rustls::Connection>>;

/// Client side: the TLS handshake with `host`, checking its certificate.
pub(crate) fn client(tls: &ClientTls, host: &str, sock: &mut TcpStream) -> io::Result<Session> {
    let name = ServerName::try_from(host.to_string()).map_err(|_| invalid(&format!("{host} isn't a host name")))?;
    let conn = rustls::ClientConnection::new(tls.0.clone(), name).map_err(tls_error)?;
    handshake(rustls::Connection::Client(conn), sock)
}

/// Server side: the TLS handshake.
pub(crate) fn server(tls: &ServerTls, sock: &mut TcpStream) -> io::Result<Session> {
    let conn = rustls::ServerConnection::new(tls.0.clone()).map_err(tls_error)?;
    handshake(rustls::Connection::Server(conn), sock)
}

fn handshake(mut conn: rustls::Connection, sock: &mut TcpStream) -> io::Result<Session> {
    while conn.is_handshaking() {
        if conn.complete_io(sock)? == (0, 0) {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
    }
    Ok(Arc::new(Mutex::new(conn)))
}

pub(crate) fn split(sock: TcpStream, tls: Option<Session>) -> io::Result<(Input, Output)> {
    let output = Output { sock: sock.try_clone()?, tls: tls.clone() };
    Ok((Input { sock, tls, ciphertext: Vec::new() }, output))
}

/// The read side.
pub(crate) struct Input {
    sock: TcpStream,
    tls: Option<Session>,
    /// Read from the socket, not yet taken by the session.
    ciphertext: Vec<u8>,
}

impl Read for Input {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let Some(tls) = &self.tls else { return self.sock.read(buf) };
        let mut chunk = [0u8; 16 << 10];
        loop {
            {
                let mut conn = lock(tls);
                match conn.reader().read(buf) {
                    Ok(n) => return Ok(n),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => return Err(e),
                }
                if !self.ciphertext.is_empty() {
                    // Only once the session's plaintext is drained, so it has room.
                    let used = conn.read_tls(&mut &self.ciphertext[..])?;
                    self.ciphertext.drain(..used);
                    conn.process_new_packets().map_err(tls_error)?;
                    send_tls(&mut conn, &mut self.sock)?;
                    continue;
                }
            }
            let read = self.sock.read(&mut chunk)?;
            if read == 0 {
                return Ok(0);
            }
            self.ciphertext.extend_from_slice(&chunk[..read]);
        }
    }
}

/// The write side.
pub(crate) struct Output {
    sock: TcpStream,
    tls: Option<Session>,
}

impl Output {
    pub(crate) fn try_clone(&self) -> io::Result<Self> {
        Ok(Self { sock: self.sock.try_clone()?, tls: self.tls.clone() })
    }

    pub(crate) fn set_nodelay(&self) -> io::Result<()> {
        self.sock.set_nodelay(true)
    }

    /// Tells the other side TLS is ending, if it's TLS. Best effort.
    pub(crate) fn close_notify(&mut self) {
        if let Some(tls) = &self.tls {
            let mut conn = lock(tls);
            conn.send_close_notify();
            let _ = send_tls(&mut conn, &mut self.sock);
        }
    }

    pub(crate) fn shutdown(&self) {
        let _ = self.sock.shutdown(Shutdown::Both);
    }
}

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.write_all(bytes)?;
        Ok(bytes.len())
    }

    fn write_all(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        let Some(tls) = &self.tls else { return self.sock.write_all(bytes) };
        let mut conn = lock(tls);
        // The session buffers a limited amount, so encrypt and send a piece at a time.
        while !bytes.is_empty() {
            send_tls(&mut conn, &mut self.sock)?;
            let taken = conn.writer().write(bytes)?;
            if taken == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            bytes = &bytes[taken..];
        }
        send_tls(&mut conn, &mut self.sock)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn send_tls(conn: &mut rustls::Connection, sock: &mut TcpStream) -> io::Result<()> {
    while conn.wants_write() {
        conn.write_tls(sock)?;
    }
    Ok(())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn invalid(why: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, why.to_string())
}

fn tls_error(e: rustls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}
