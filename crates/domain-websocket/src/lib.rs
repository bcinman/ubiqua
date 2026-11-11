//! WebSocket over `std::net`, for `domain-server` and `domain-client`: the opening handshake, and
//! text messages in frames, plain (`ws://`) or over TLS (`wss://`). No async runtime.
//!
//! A connection splits into a [`Reader`] and a [`Writer`] on the same socket, so one thread can
//! wait for messages while others send. Pings come back from [`Reader::read`]; answer them with
//! [`Writer::pong`].

mod tls;

pub use tls::{ClientTls, ServerTls};

use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use tls::{Input, Output};

/// The largest message either side accepts.
pub const MAX_MESSAGE: usize = 16 << 20;
/// The largest opening handshake a server reads.
const MAX_HEAD: usize = 16 << 10;
const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// One message from the other side.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    Text(String),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    /// The other side is closing the connection.
    Close,
}

/// The client's opening request, for a server deciding whether to accept it.
#[derive(Debug)]
pub struct Request {
    pub path: String,
    headers: HashMap<String, String>,
}

impl Request {
    /// A header's value. Names are case-insensitive.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(&name.to_ascii_lowercase()).map(String::as_str)
    }

    /// The token in `Authorization: Bearer <token>`.
    pub fn bearer(&self) -> Option<&str> {
        self.header("authorization")?.strip_prefix("Bearer ")
    }
}

/// Why a client couldn't connect.
#[derive(Debug)]
pub enum ConnectError {
    /// The server answered the handshake with this HTTP status instead of accepting it, such as
    /// 401 for a token it doesn't accept.
    Refused(u16),
    Io(io::Error),
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectError::Refused(status) => write!(f, "the server refused the connection ({status})"),
            ConnectError::Io(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for ConnectError {}

impl From<io::Error> for ConnectError {
    fn from(e: io::Error) -> Self {
        ConnectError::Io(e)
    }
}

/// Server side: reads the client's request, asks `check` whether to accept it, and answers.
/// `check` returns what to keep from the request, or the HTTP status to refuse it with.
pub fn accept<T>(stream: TcpStream, check: impl FnOnce(&Request) -> Result<T, u16>) -> io::Result<(T, Reader, Writer)> {
    let (input, output) = tls::split(stream, None)?;
    accept_on(input, output, check)
}

/// Like [`accept`], for `wss://`: the TLS handshake first, with `tls`'s certificate.
pub fn accept_tls<T>(
    mut stream: TcpStream,
    tls: &ServerTls,
    check: impl FnOnce(&Request) -> Result<T, u16>,
) -> io::Result<(T, Reader, Writer)> {
    let session = tls::server(tls, &mut stream)?;
    let (input, output) = tls::split(stream, Some(session))?;
    accept_on(input, output, check)
}

fn accept_on<T>(
    input: Input,
    output: Output,
    check: impl FnOnce(&Request) -> Result<T, u16>,
) -> io::Result<(T, Reader, Writer)> {
    let mut reader = Reader::new(input, Role::Server);
    let head = reader.read_head()?;
    let mut writer = Writer { stream: output, role: Role::Server };
    let refuse = |writer: &mut Writer, status: u16| -> io::Error {
        let reason = match status {
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            _ => "Refused",
        };
        let _ = write!(writer.stream, "HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        io::Error::new(io::ErrorKind::ConnectionRefused, format!("refused the handshake with {status}"))
    };
    let Some(request) = parse_request(&head) else { return Err(refuse(&mut writer, 400)) };
    let upgrade = request.header("upgrade").is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
    let version = request.header("sec-websocket-version") == Some("13");
    let (Some(key), true, true) = (request.header("sec-websocket-key"), upgrade, version) else {
        return Err(refuse(&mut writer, 400));
    };
    let accept = accept_key(key);
    let kept = match check(&request) {
        Ok(kept) => kept,
        Err(status) => return Err(refuse(&mut writer, status)),
    };
    write!(
        writer.stream,
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    )?;
    writer.stream.set_nodelay()?;
    Ok((kept, reader, writer))
}

/// Client side: connects to `ws://host:port/path`, or `wss://..` over TLS, trusting the
/// servers the system trusts. Sends `headers` with the request.
pub fn connect(url: &str, headers: &[(&str, &str)]) -> Result<(Reader, Writer), ConnectError> {
    connect_with(url, headers, None)
}

/// Like [`connect`], trusting the servers `tls` trusts on `wss://`.
pub fn connect_with(
    url: &str,
    headers: &[(&str, &str)],
    tls: Option<&ClientTls>,
) -> Result<(Reader, Writer), ConnectError> {
    let invalid = |why: &str| ConnectError::Io(io::Error::new(io::ErrorKind::InvalidInput, format!("{url}: {why}")));
    let (secure, rest) = match (url.strip_prefix("wss://"), url.strip_prefix("ws://")) {
        (Some(rest), _) => (true, rest),
        (_, Some(rest)) => (false, rest),
        _ => return Err(invalid("only ws:// and wss:// URLs are supported")),
    };
    let (host, path) = rest.split_once('/').map_or((rest, "/".to_string()), |(h, p)| (h, format!("/{p}")));
    let addr = if host.contains(':') { host.to_string() } else { format!("{host}:{}", if secure { 443 } else { 80 }) };
    // Tries each address the host resolves to, such as ::1 then 127.0.0.1 for localhost.
    let mut stream = TcpStream::connect(addr.as_str())?;
    stream.set_nodelay(true)?;
    let session = if secure {
        let tls = match tls {
            Some(tls) => tls.clone(),
            None => ClientTls::system()?,
        };
        let name = host.rsplit_once(':').map_or(host, |(name, _)| name);
        Some(tls::client(&tls, name, &mut stream)?)
    } else {
        None
    };
    let (input, output) = tls::split(stream, session)?;
    let key = base64(&random_bytes::<16>());
    let mut request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
    );
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    let mut writer = Writer { stream: output, role: Role::Client };
    writer.stream.write_all(request.as_bytes())?;
    let mut reader = Reader::new(input, Role::Client);
    let head = reader.read_head()?;
    let head = String::from_utf8_lossy(&head);
    let status = head.split(' ').nth(1).and_then(|s| s.parse::<u16>().ok()).unwrap_or(0);
    if status != 101 {
        return Err(ConnectError::Refused(status));
    }
    let response = parse_headers(head.lines().skip(1));
    if response.get("sec-websocket-accept").map(String::as_str) != Some(&accept_key(&key)) {
        return Err(invalid("the server's handshake doesn't match"));
    }
    Ok((reader, writer))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Server,
    Client,
}

/// Reads messages from one connection.
pub struct Reader {
    stream: Input,
    role: Role,
    buf: Vec<u8>,
}

impl Reader {
    fn new(stream: Input, role: Role) -> Self {
        Self { stream, role, buf: Vec::new() }
    }

    /// Waits for the next message. Fails when the connection breaks or the other side breaks
    /// the protocol; then the connection is done.
    pub fn read(&mut self) -> io::Result<Message> {
        let mut text: Option<Vec<u8>> = None;
        loop {
            let (fin, opcode, payload) = self.read_frame()?;
            match opcode {
                0x8 => return Ok(Message::Close),
                0x9 => return Ok(Message::Ping(payload)),
                0xA => return Ok(Message::Pong(payload)),
                0x1 if text.is_none() => text = Some(payload),
                0x0 if text.is_some() => {
                    let text = text.as_mut().expect("checked");
                    if text.len() + payload.len() > MAX_MESSAGE {
                        return Err(protocol("message too large"));
                    }
                    text.extend(payload);
                }
                0x2 => return Err(protocol("binary messages aren't supported")),
                _ => return Err(protocol("unexpected frame")),
            }
            if fin {
                let bytes = text.take().expect("a text message started");
                return String::from_utf8(bytes).map(Message::Text).map_err(|_| protocol("text isn't UTF-8"));
            }
        }
    }

    fn read_frame(&mut self) -> io::Result<(bool, u8, Vec<u8>)> {
        self.fill(2)?;
        let (b0, b1) = (self.buf[0], self.buf[1]);
        let (fin, opcode, masked) = (b0 & 0x80 != 0, b0 & 0x0F, b1 & 0x80 != 0);
        if b0 & 0x70 != 0 {
            return Err(protocol("reserved bits set"));
        }
        // Clients mask every frame and servers never do.
        if masked != (self.role == Role::Server) {
            return Err(protocol("wrong masking"));
        }
        let (len, mut at) = match b1 & 0x7F {
            126 => {
                self.fill(4)?;
                (u16::from_be_bytes([self.buf[2], self.buf[3]]) as u64, 4)
            }
            127 => {
                self.fill(10)?;
                (u64::from_be_bytes(self.buf[2..10].try_into().expect("8 bytes")), 10)
            }
            n => (n as u64, 2),
        };
        if opcode & 0x8 != 0 && (!fin || len > 125) {
            return Err(protocol("bad control frame"));
        }
        if len > MAX_MESSAGE as u64 {
            return Err(protocol("message too large"));
        }
        let mask_at = at;
        if masked {
            at += 4;
        }
        let end = at + len as usize;
        self.fill(end)?;
        let mut payload = self.buf[at..end].to_vec();
        if masked {
            let mask: [u8; 4] = self.buf[mask_at..mask_at + 4].try_into().expect("4 bytes");
            payload.iter_mut().enumerate().for_each(|(i, b)| *b ^= mask[i % 4]);
        }
        self.buf.drain(..end);
        Ok((fin, opcode, payload))
    }

    /// Reads until `buf` holds at least `n` bytes.
    fn fill(&mut self, n: usize) -> io::Result<()> {
        let mut chunk = [0u8; 8192];
        while self.buf.len() < n {
            let read = self.stream.read(&mut chunk)?;
            if read == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            self.buf.extend_from_slice(&chunk[..read]);
        }
        Ok(())
    }

    /// Reads an HTTP head, up to its blank line. Bytes after it stay in `buf`.
    fn read_head(&mut self) -> io::Result<Vec<u8>> {
        let mut chunk = [0u8; 1024];
        loop {
            if let Some(end) = self.buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = self.buf[..end].to_vec();
                self.buf.drain(..end + 4);
                return Ok(head);
            }
            if self.buf.len() > MAX_HEAD {
                return Err(protocol("handshake too large"));
            }
            let read = self.stream.read(&mut chunk)?;
            if read == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            self.buf.extend_from_slice(&chunk[..read]);
        }
    }
}

/// Sends messages on one connection.
pub struct Writer {
    stream: Output,
    role: Role,
}

impl Writer {
    pub fn text(&mut self, text: &str) -> io::Result<()> {
        self.frame(0x1, text.as_bytes())
    }

    pub fn ping(&mut self, payload: &[u8]) -> io::Result<()> {
        self.frame(0x9, &payload[..payload.len().min(125)])
    }

    pub fn pong(&mut self, payload: &[u8]) -> io::Result<()> {
        self.frame(0xA, &payload[..payload.len().min(125)])
    }

    /// Says goodbye, then closes the socket, which also ends a [`Reader::read`] waiting on it.
    pub fn close(&mut self) {
        let _ = self.frame(0x8, &[]);
        self.stream.close_notify();
        self.shutdown();
    }

    /// Closes the socket without a goodbye.
    pub fn shutdown(&self) {
        self.stream.shutdown();
    }

    /// A second writer for the same connection. Frames from two writers must not overlap, so
    /// use it from one thread at a time, or only for [`shutdown`](Self::shutdown).
    pub fn try_clone(&self) -> io::Result<Writer> {
        Ok(Writer { stream: self.stream.try_clone()?, role: self.role })
    }

    fn frame(&mut self, opcode: u8, payload: &[u8]) -> io::Result<()> {
        let mut frame = Vec::with_capacity(payload.len() + 14);
        frame.push(0x80 | opcode);
        let mask_bit = if self.role == Role::Client { 0x80 } else { 0 };
        match payload.len() {
            n @ 0..=125 => frame.push(mask_bit | n as u8),
            n @ 126..=0xFFFF => {
                frame.push(mask_bit | 126);
                frame.extend((n as u16).to_be_bytes());
            }
            n => {
                frame.push(mask_bit | 127);
                frame.extend((n as u64).to_be_bytes());
            }
        }
        if self.role == Role::Client {
            let mask = random_bytes::<4>();
            frame.extend(mask);
            frame.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
        } else {
            frame.extend_from_slice(payload);
        }
        self.stream.write_all(&frame)
    }
}

fn parse_request(head: &[u8]) -> Option<Request> {
    let head = std::str::from_utf8(head).ok()?;
    let mut lines = head.split("\r\n");
    let mut first = lines.next()?.split(' ');
    let (method, path) = (first.next()?, first.next()?);
    (method == "GET").then(|| Request { path: path.to_string(), headers: parse_headers(lines) })
}

fn parse_headers<'a>(lines: impl Iterator<Item = &'a str>) -> HashMap<String, String> {
    lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect()
}

fn accept_key(key: &str) -> String {
    let mut sha = sha1_smol::Sha1::new();
    sha.update(key.as_bytes());
    sha.update(GUID.as_bytes());
    base64(&sha.digest().bytes())
}

fn protocol(why: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, why.to_string())
}

fn base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n =
            (chunk[0] as u32) << 16 | (*chunk.get(1).unwrap_or(&0) as u32) << 8 | *chunk.get(2).unwrap_or(&0) as u32;
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(TABLE[(n >> (18 - 6 * i) & 0x3F) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// Unpredictable bytes, from the standard library's randomly keyed hasher.
fn random_bytes<const N: usize>() -> [u8; N] {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut out = [0u8; N];
    for chunk in out.chunks_mut(8) {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
        chunk.copy_from_slice(&hasher.finish().to_le_bytes()[..chunk.len()]);
    }
    out
}

/// A random 64-bit number, such as for a client's command ids.
pub fn random_u64() -> u64 {
    u64::from_le_bytes(random_bytes::<8>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn the_accept_key_matches_the_rfc_example() {
        assert_eq!(accept_key("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    const CA: &[u8] = include_bytes!("../tests/certs/ca.pem");

    fn server_tls() -> ServerTls {
        ServerTls::from_pem(
            include_bytes!("../tests/certs/server.pem"),
            include_bytes!("../tests/certs/server-key.pem"),
        )
        .unwrap()
    }

    #[test]
    fn messages_of_every_length_cross_both_ways() {
        round_trip(None);
    }

    #[test]
    fn messages_of_every_length_cross_both_ways_over_tls() {
        round_trip(Some(server_tls()));
    }

    #[test]
    fn a_client_refuses_a_server_it_doesnt_trust() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("wss://localhost:{}/", listener.local_addr().unwrap().port());
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            assert!(accept_tls(stream, &server_tls(), |_| Ok(())).is_err());
        });
        let other = ClientTls::trusting(include_bytes!("../tests/certs/server.pem")).unwrap();
        let refused = connect_with(&url, &[], Some(&other)).err().expect("not trusted");
        assert!(refused.to_string().contains("certificate"), "{refused}");
        server.join().unwrap();
    }

    /// Echoes a client's messages back, plain or over TLS.
    fn round_trip(tls: Option<ServerTls>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = match tls {
            Some(_) => format!("wss://localhost:{port}/live"),
            None => format!("ws://127.0.0.1:{port}/live"),
        };
        let secure = tls.is_some();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let check = |r: &Request| Ok((r.path.clone(), r.bearer().map(String::from)));
            let (token, mut reader, mut writer) = match &tls {
                Some(tls) => accept_tls(stream, tls, check).unwrap(),
                None => accept(stream, check).unwrap(),
            };
            assert_eq!(token, ("/live".to_string(), Some("t-1".to_string())));
            // Echo, answering pings, until the client says goodbye.
            loop {
                match reader.read().unwrap() {
                    Message::Text(text) => writer.text(&text).unwrap(),
                    Message::Ping(p) => writer.pong(&p).unwrap(),
                    Message::Close => return,
                    Message::Pong(_) => {}
                }
            }
        });
        let trusted = ClientTls::trusting(CA).unwrap();
        let tls = secure.then_some(&trusted);
        let (mut reader, mut writer) = connect_with(&url, &[("Authorization", "Bearer t-1")], tls).unwrap();
        for len in [0, 1, 125, 126, 127, 65_535, 65_536, 200_000, 3_000_000] {
            let text: String = (0..len).map(|i| char::from(b'a' + (i % 26) as u8)).collect();
            writer.text(&text).unwrap();
            assert_eq!(reader.read().unwrap(), Message::Text(text), "length {len}");
        }
        writer.ping(b"hi").unwrap();
        assert_eq!(reader.read().unwrap(), Message::Pong(b"hi".to_vec()));
        writer.close();
        server.join().unwrap();
    }

    #[test]
    fn a_refused_handshake_reports_its_status() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("ws://{}/", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            accept(stream, |r| r.bearer().map(|_| ()).ok_or(401)).err().unwrap()
        });
        assert!(matches!(connect(&url, &[]), Err(ConnectError::Refused(401))));
        server.join().unwrap();
    }
}
