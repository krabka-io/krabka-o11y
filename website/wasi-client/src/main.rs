use std::{
    error::Error,
    io::{self, Read, Write},
    process::ExitCode,
    time::Duration,
};

use reqwest::{
    Method, Url,
    blocking::Client,
    header::{CONTENT_TYPE, HeaderValue},
    redirect::Policy,
};
use serde::{Deserialize, Serialize};

const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_TENANT_BYTES: usize = 256;
const LAB_PORTS: [u16; 6] = [3100, 4040, 4041, 4318, 3200, 9090];

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Request {
    method: String,
    url: String,
    tenant: String,
    body: Option<String>,
    content_type: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Response {
    status: u16,
    content_type: String,
    body: String,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("lab HTTP request failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let input = read_bounded(io::stdin().lock())?;
    let request: Request = serde_json::from_slice(&input)?;
    let url = validate_target(&request.url)?;
    let response = perform_request(request, url)?;
    let mut output = io::stdout().lock();
    serde_json::to_writer(&mut output, &response)?;
    output.write_all(b"\n")?;
    Ok(())
}

fn read_bounded(reader: impl Read) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take(u64::try_from(MAX_BYTES)? + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_BYTES {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "8 MiB limit exceeded").into());
    }
    Ok(bytes)
}

fn validate_target(raw: &str) -> Result<Url> {
    let url = Url::parse(raw)?;
    // The URL parser also accepts numeric aliases for IPv4 addresses.
    if !raw.starts_with("http://127.0.0.1:")
        || url.scheme() != "http"
        || url.host_str() != Some("127.0.0.1")
        || !url.username().is_empty()
        || url.password().is_some()
        || !url.port().is_some_and(|port| LAB_PORTS.contains(&port))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "URL must use http://127.0.0.1 with an explicit lab service port and no credentials",
        )
        .into());
    }
    Ok(url)
}

fn perform_request(request: Request, url: Url) -> Result<Response> {
    if request.tenant.is_empty()
        || request.tenant.len() > MAX_TENANT_BYTES
        || !request
            .tenant
            .bytes()
            .all(|byte| (b' '..=b'~').contains(&byte))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tenant must contain 1 to 256 printable ASCII bytes",
        )
        .into());
    }
    let tenant = HeaderValue::from_bytes(request.tenant.as_bytes())?;
    let method = Method::from_bytes(request.method.as_bytes())?;
    let content_type = request
        .content_type
        .as_deref()
        .or(request.body.as_ref().map(|_| "application/json"))
        .map(HeaderValue::from_str)
        .transpose()?;
    let client = Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .timeout(Duration::from_secs(30))
        .build()?;
    let mut outgoing = client.request(method, url).header("X-Scope-OrgID", tenant);
    if let Some(content_type) = content_type {
        outgoing = outgoing.header(CONTENT_TYPE, content_type);
    }
    if let Some(body) = request.body {
        outgoing = outgoing.body(body);
    }
    let incoming = outgoing.send()?;
    let status = incoming.status().as_u16();
    let content_type = incoming
        .headers()
        .get(CONTENT_TYPE)
        .map(HeaderValue::to_str)
        .transpose()?
        .unwrap_or("")
        .to_owned();
    let body = String::from_utf8(read_bounded(incoming)?)?;
    Ok(Response {
        status,
        content_type,
        body,
    })
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::{net::TcpListener, thread};

    use assert2::assert;

    use super::*;

    #[test]
    fn preserves_real_http_error_responses_and_rejects_egress() {
        for url in [
            "https://127.0.0.1:3100/",
            "http://example.com:3100/",
            "http://localhost:3100/",
            "http://127.1:3100/",
            "http://2130706433:3100/",
            "http://127.0.0.1/",
            "http://127.0.0.1:8000/",
            "http://127.0.0.1:9091/",
            "http://user:password@127.0.0.1:3100/",
            "http://127.0.0.1:3100@127.0.0.1:3100/",
        ] {
            assert!(validate_target(url).is_err(), "{url}");
        }
        assert!(validate_target("http://127.0.0.1:3100/api/test").is_ok());
        assert!(read_bounded(io::repeat(0).take(u64::try_from(MAX_BYTES).unwrap() + 1)).is_err());
        let url = validate_target("http://127.0.0.1:3100/api/test").unwrap();
        assert!(
            perform_request(
                Request {
                    method: "POST".to_owned(),
                    url: url.to_string(),
                    tenant: "lab-test".to_owned(),
                    body: None,
                    content_type: Some("text/plain\r\nX-Escape: injected".to_owned()),
                },
                url,
            )
            .is_err()
        );

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 1024];
            while !request.ends_with(b"lab_latency_ms 7\n") {
                let read = socket.read(&mut buffer).unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buffer[..read]);
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("POST /api/test HTTP/1.1\r\n"));
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("x-scope-orgid: lab-test\r\n")
            );
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("content-type: text/plain\r\n")
            );
            let body = r#"{"error":"real service reply"}"#;
            write!(
                socket,
                "HTTP/1.1 307 Temporary Redirect\r\nContent-Type: application/json\r\nContent-Length: {}\r\nLocation: https://example.invalid/escape\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        let url = Url::parse(&format!("http://{address}/api/test")).unwrap();
        let response = perform_request(
            Request {
                method: "POST".to_owned(),
                url: url.to_string(),
                tenant: "lab-test".to_owned(),
                body: Some("lab_latency_ms 7\n".to_owned()),
                content_type: Some("text/plain".to_owned()),
            },
            url,
        )
        .unwrap();
        server.join().unwrap();
        assert!(response.status == 307);
        assert!(response.content_type == "application/json");
        assert!(response.body == r#"{"error":"real service reply"}"#);
    }
}
