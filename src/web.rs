//! Browser-based transfer for devices without lanxfer (phones on a hotspot).
//! Hand-rolled HTTP/1.1 over tokio — one page, raw-body uploads, streamed
//! downloads. Filesystem access is rooted in a capability directory so
//! crafted paths and symlinks cannot escape the folder being shared.

use anyhow::{Context, Result, anyhow, bail};
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Semaphore;

use crate::{storage, ui, util};

const MAX_REQUEST_LINE: usize = 8 * 1024;
const MAX_HEADER_LINE: usize = 8 * 1024;
const MAX_HEADERS: usize = 64;
const IO_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

struct Share {
    root: Arc<Dir>,
    token: Option<String>,
    max_upload_bytes: u64,
}

pub async fn run(
    bind: &str,
    dir: PathBuf,
    open: bool,
    max_upload_bytes: u64,
    max_connections: usize,
) -> Result<()> {
    if max_connections == 0 {
        bail!("max connections must be at least 1");
    }
    let dir = dir.canonicalize().context("shared directory not found")?;
    let root = Dir::open_ambient_dir(&dir, ambient_authority())
        .context("could not open shared directory")?;
    let listener = TcpListener::bind(bind)
        .await
        .with_context(|| format!("cannot listen on {bind} (another lanxfer running?)"))?;
    let port = listener.local_addr()?.port();
    let token = (!open).then(|| uuid::Uuid::new_v4().simple().to_string());
    let base = token
        .as_ref()
        .map(|token| format!("/{token}"))
        .unwrap_or_default();

    ui::section("Web transfer");
    ui::kv("folder", &dir.display().to_string());
    let urls: Vec<String> = util::local_ipv4s()
        .into_iter()
        .filter(|ip| ip != "127.0.0.1")
        .map(|ip| format!("http://{ip}:{port}{base}/"))
        .collect();
    for url in &urls {
        ui::kv("url", url);
    }
    if let Some(url) = urls.first()
        && let Ok(qr) = qrcode::QrCode::new(url.as_bytes())
    {
        // Inverted for dark terminals — block chars take the (light)
        // foreground color, so "light" modules become the bright ones.
        use qrcode::render::unicode::Dense1x2;
        let art = qr
            .render::<Dense1x2>()
            .dark_color(Dense1x2::Light)
            .light_color(Dense1x2::Dark)
            .quiet_zone(true)
            .build();
        println!("\n{art}\n");
    }
    if open {
        ui::warn("open share: anyone on this network can upload and download");
    } else {
        ui::info("the private link is required to access this share");
    }
    ui::kv("max upload", &util::format_size(max_upload_bytes));
    ui::info("Ctrl-C to stop");

    let share = Arc::new(Share {
        root: Arc::new(root),
        token,
        max_upload_bytes,
    });
    let connections = Arc::new(Semaphore::new(max_connections));
    loop {
        let (stream, _) = listener.accept().await?;
        let Ok(permit) = Arc::clone(&connections).try_acquire_owned() else {
            drop(stream);
            continue;
        };
        let share = Arc::clone(&share);
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) = handle(stream, &share).await {
                log::debug!("web request failed: {err:#}");
            }
        });
    }
}

async fn handle(stream: tokio::net::TcpStream, share: &Share) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);

    let line = read_limited_line(&mut reader, MAX_REQUEST_LINE).await?;
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();
    if parts.next().is_none() || !target.starts_with('/') {
        return respond(&mut write, "400 Bad Request", "text/plain", b"bad request").await;
    }

    let mut content_length: Option<u64> = None;
    let mut expect_continue = false;
    let mut unsupported_transfer_encoding = false;
    for _ in 0..MAX_HEADERS {
        let header = read_limited_line(&mut reader, MAX_HEADER_LINE).await?;
        if header.is_empty() {
            break;
        }
        let header = header.trim();
        if header.is_empty() {
            break;
        }
        let lower = header.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            let parsed = v
                .trim()
                .parse()
                .map_err(|_| anyhow!("invalid content-length"))?;
            if content_length.replace(parsed).is_some() {
                return respond(
                    &mut write,
                    "400 Bad Request",
                    "text/plain",
                    b"duplicate content-length",
                )
                .await;
            }
        }
        if lower.starts_with("expect:") && lower.contains("100-continue") {
            expect_continue = true;
        }
        if let Some(v) = lower.strip_prefix("transfer-encoding:")
            && v.trim() != "identity"
        {
            unsupported_transfer_encoding = true;
        }
    }

    if unsupported_transfer_encoding {
        return respond(
            &mut write,
            "501 Not Implemented",
            "text/plain",
            b"transfer encoding not supported",
        )
        .await;
    }

    let (raw_path, query) = target.split_once('?').unwrap_or((target.as_str(), ""));
    let Some(path) = authorized_path(raw_path, share.token.as_deref()) else {
        return respond(&mut write, "404 Not Found", "text/plain", b"not found").await;
    };
    let base = share
        .token
        .as_ref()
        .map(|token| format!("/{token}"))
        .unwrap_or_default();
    match (method.as_str(), path) {
        ("GET", "/") => {
            let rel = query_param(query, "path").unwrap_or_default();
            send_page(&mut write, &share.root, &rel, &base).await
        }
        ("GET", p) if p.starts_with("/f/") => send_file(&mut write, &share.root, &p[3..]).await,
        ("POST", "/up") => {
            let Some(content_length) = content_length else {
                return respond(
                    &mut write,
                    "411 Length Required",
                    "text/plain",
                    b"content-length required",
                )
                .await;
            };
            if content_length > share.max_upload_bytes {
                return respond(
                    &mut write,
                    "413 Content Too Large",
                    "text/plain",
                    b"upload exceeds configured limit",
                )
                .await;
            }
            if expect_continue {
                write.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await?;
            }
            let name = query_param(query, "name").unwrap_or_default();
            let rel = query_param(query, "path").unwrap_or_default();
            receive_upload(
                &mut reader,
                &mut write,
                &share.root,
                &name,
                &rel,
                content_length,
            )
            .await
        }
        _ => respond(&mut write, "404 Not Found", "text/plain", b"not found").await,
    }
}

async fn read_limited_line<R>(reader: &mut R, max: usize) -> Result<String>
where
    R: AsyncBufRead + Unpin,
{
    let mut bytes = Vec::new();
    let read = tokio::time::timeout(IO_IDLE_TIMEOUT, async {
        let mut limited = (&mut *reader).take((max + 1) as u64);
        limited.read_until(b'\n', &mut bytes).await
    })
    .await
    .map_err(|_| anyhow!("request timed out"))??;
    if read > max {
        bail!("request line or header exceeds {max} bytes");
    }
    String::from_utf8(bytes).context("request is not UTF-8")
}

fn authorized_path<'a>(path: &'a str, token: Option<&str>) -> Option<&'a str> {
    let Some(token) = token else {
        return Some(path);
    };
    let prefix_len = token.len() + 1;
    let prefix = path.get(..prefix_len)?;
    if !prefix.starts_with('/') || prefix.get(1..) != Some(token) {
        return None;
    }
    match path.get(prefix_len..) {
        Some("") => Some("/"),
        Some(rest) if rest.starts_with('/') => Some(rest),
        _ => None,
    }
}

/// First value for `key` in a query string, percent-decoded.
fn query_param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == key).then(|| percent_decode(v))
    })
}

/// Resolve a browse path: empty means the share root, anything else must be
/// a sanitized relative path to an existing directory under it.
fn resolve_dir(root: &Dir, rel: &str) -> Option<(Dir, String)> {
    if rel.is_empty() {
        return Some((root.try_clone().ok()?, String::new()));
    }
    let clean = storage::sanitize_relative_path(rel).ok()?;
    let dir = root.open_dir(&clean).ok()?;
    Some((dir, clean.to_string_lossy().replace('\\', "/")))
}

async fn send_page(write: &mut OwnedWriteHalf, root: &Dir, rel: &str, base: &str) -> Result<()> {
    let Some((dir, rel)) = resolve_dir(root, rel) else {
        return respond(write, "404 Not Found", "text/plain", b"not found").await;
    };
    let mut dirs: Vec<String> = Vec::new();
    let mut files: Vec<(String, u64)> = Vec::new();
    for entry in dir.entries()?.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') || name.ends_with(".part") {
            continue;
        }
        match entry.file_type() {
            Ok(t) if t.is_dir() => dirs.push(name),
            Ok(t) if t.is_file() => {
                files.push((name, entry.metadata().map(|m| m.len()).unwrap_or(0)))
            }
            _ => {}
        }
    }
    dirs.sort();
    files.sort();

    let join = |name: &str| {
        if rel.is_empty() {
            name.to_string()
        } else {
            format!("{rel}/{name}")
        }
    };
    let mut rows = String::new();
    if !rel.is_empty() {
        let parent = rel.rsplit_once('/').map(|(p, _)| p).unwrap_or("");
        let href = if parent.is_empty() {
            format!("{base}/")
        } else {
            format!("{base}/?path={}", percent_encode(parent))
        };
        rows.push_str(&format!("<li><a href=\"{href}\">⬑ ..</a></li>"));
    }
    for name in &dirs {
        rows.push_str(&format!(
            "<li><a href=\"{base}/?path={}\">📁 {}/</a></li>",
            percent_encode(&join(name)),
            escape_html(name)
        ));
    }
    for (name, size) in &files {
        rows.push_str(&format!(
            "<li><a href=\"{base}/f/{}\" download>{}</a><small>{}</small></li>",
            percent_encode(&join(name)),
            escape_html(name),
            util::format_size(*size)
        ));
    }
    if rows.is_empty() {
        rows.push_str("<li><small>empty folder</small></li>");
    }
    let cwd = if rel.is_empty() {
        String::new()
    } else {
        format!("<p><small>/{}</small></p>", escape_html(&rel))
    };
    let page = PAGE
        .replace("{CWD}", &cwd)
        .replace("{PATH}", &percent_encode(&rel))
        .replace("{BASE}", base)
        .replace("{ROWS}", &rows);
    respond(write, "200 OK", "text/html", page.as_bytes()).await
}

async fn send_file(write: &mut OwnedWriteHalf, root: &Dir, encoded: &str) -> Result<()> {
    let Ok(rel) = storage::sanitize_relative_path(&percent_decode(encoded)) else {
        return respond(write, "400 Bad Request", "text/plain", b"bad name").await;
    };
    let Ok(file) = root.open(&rel) else {
        return respond(write, "404 Not Found", "text/plain", b"not found").await;
    };
    let meta = file.metadata()?;
    if !meta.is_file() {
        return respond(write, "404 Not Found", "text/plain", b"not found").await;
    }
    let mut file = tokio::fs::File::from_std(file.into_std());
    let len = meta.len();
    let name = rel.file_name().unwrap_or_default().to_string_lossy();
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {len}\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename*=UTF-8''{}\r\nCache-Control: no-store\r\nCross-Origin-Resource-Policy: same-origin\r\nReferrer-Policy: no-referrer\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",
        percent_encode(&name)
    );
    write.write_all(header.as_bytes()).await?;
    tokio::io::copy(&mut file, write).await?;
    Ok(())
}

async fn receive_upload<R: AsyncRead + Unpin>(
    reader: &mut R,
    write: &mut OwnedWriteHalf,
    root: &Dir,
    raw_name: &str,
    rel: &str,
    len: u64,
) -> Result<()> {
    let (Some(name), Some((dir, _))) = (sanitize_name(raw_name), resolve_dir(root, rel)) else {
        return respond(write, "400 Bad Request", "text/plain", b"bad name").await;
    };
    let tmp_name = format!(".lanxfer-upload-{}.part", uuid::Uuid::new_v4().simple());
    let file = dir.open_with(&tmp_name, OpenOptions::new().write(true).create_new(true))?;
    let mut file = tokio::fs::File::from_std(file.into_std());
    let copied = copy_upload_with_idle_timeout(reader, &mut file, len).await;
    match copied {
        Ok(()) => {
            file.flush().await?;
            file.sync_all().await?;
            drop(file);
            let final_name = persist_upload(&dir, &tmp_name, &name)?;
            log::info!("web upload: {final_name} ({len} bytes)");
            respond(write, "200 OK", "text/plain", b"ok").await
        }
        Err(err) => {
            drop(file);
            let _ = dir.remove_file(&tmp_name);
            log::debug!("web upload interrupted: {err:#}");
            respond(
                write,
                "500 Internal Server Error",
                "text/plain",
                b"upload interrupted",
            )
            .await
        }
    }
}

async fn copy_upload_with_idle_timeout<R>(
    reader: &mut R,
    file: &mut tokio::fs::File,
    len: u64,
) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut remaining = len;
    let mut buf = vec![0u8; 128 * 1024];
    while remaining > 0 {
        let cap = usize::min(remaining as usize, buf.len());
        let read = tokio::time::timeout(IO_IDLE_TIMEOUT, reader.read(&mut buf[..cap]))
            .await
            .map_err(|_| anyhow!("upload timed out"))??;
        if read == 0 {
            bail!("upload ended before content-length bytes arrived");
        }
        file.write_all(&buf[..read]).await?;
        remaining -= read as u64;
    }
    Ok(())
}

fn persist_upload(dir: &Dir, tmp_name: &str, requested_name: &str) -> Result<String> {
    for i in 0u64.. {
        let candidate = numbered_name(requested_name, i);
        match dir.hard_link(tmp_name, dir, &candidate) {
            Ok(()) => {
                dir.remove_file(tmp_name)?;
                return Ok(candidate);
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err.into()),
        }
    }
    unreachable!()
}

async fn respond(write: &mut OwnedWriteHalf, status: &str, ctype: &str, body: &[u8]) -> Result<()> {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: {ctype}; charset=utf-8\r\nCache-Control: no-store\r\nContent-Security-Policy: default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'\r\nCross-Origin-Resource-Policy: same-origin\r\nPermissions-Policy: camera=(), microphone=(), geolocation=()\r\nReferrer-Policy: no-referrer\r\nX-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\nConnection: close\r\n\r\n",
        body.len()
    );
    write.write_all(header.as_bytes()).await?;
    write.write_all(body).await?;
    Ok(())
}

/// Strip any path components — uploads and downloads only touch files
/// directly inside the shared directory.
fn sanitize_name(raw: &str) -> Option<String> {
    let name = raw.rsplit(['/', '\\']).next().unwrap_or("").trim();
    if name.is_empty() || name == "." || name == ".." || name.contains('\0') {
        return None;
    }
    Some(name.to_string())
}

fn numbered_name(name: &str, index: u64) -> String {
    if index == 0 {
        return name.to_string();
    }
    let (stem, ext) = match name.rsplit_once('.') {
        Some((s, e)) if !s.is_empty() => (s, format!(".{e}")),
        _ => (name, String::new()),
    };
    format!("{stem} ({index}){ext}")
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && let (Some(h), Some(l)) = (
                bytes.get(i + 1).and_then(|b| (*b as char).to_digit(16)),
                bytes.get(i + 2).and_then(|b| (*b as char).to_digit(16)),
            )
        {
            out.push((h * 16 + l) as u8);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn percent_encode(input: &str) -> String {
    input
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn escape_html(input: &str) -> String {
    input
        .chars()
        .map(|c| match c {
            '&' => "&amp;".into(),
            '<' => "&lt;".into(),
            '>' => "&gt;".into(),
            '"' => "&quot;".into(),
            '\'' => "&#39;".into(),
            c => c.to_string(),
        })
        .collect()
}

const PAGE: &str = r#"<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>lanxfer</title>
<style>
body{font-family:system-ui;margin:0 auto;max-width:640px;padding:1rem;background:#111;color:#eee}
a{color:#7cf;text-decoration:none;word-break:break-all}
ul{padding:0}
li{display:flex;justify-content:space-between;gap:1rem;padding:.6rem 0;border-bottom:1px solid #333;list-style:none;align-items:center}
#drop{border:2px dashed #555;border-radius:8px;padding:1.5rem;text-align:center;margin:1rem 0}
progress{width:100%}
small{color:#888;white-space:nowrap}
</style></head><body>
<h1>lanxfer</h1>
{CWD}
<div id="drop">
<input type="file" id="f" multiple>
<p id="status"></p>
<progress id="bar" value="0" max="1" hidden></progress>
</div>
<ul>{ROWS}</ul>
<script>
const f=document.getElementById('f'),bar=document.getElementById('bar'),st=document.getElementById('status');
f.onchange=async()=>{
 for(const file of f.files){
  bar.hidden=false;
  st.textContent='Uploading '+file.name;
  try{
   await new Promise((ok,err)=>{
    const x=new XMLHttpRequest();
    x.open('POST','{BASE}/up?name='+encodeURIComponent(file.name)+'&path={PATH}');
    x.upload.onprogress=e=>{if(e.lengthComputable)bar.value=e.loaded/e.total};
    x.onload=()=>x.status===200?ok():err(new Error(x.responseText||x.status));
    x.onerror=()=>err(new Error('network error'));
    x.send(file);
   });
  }catch(e){alert('Upload failed: '+e.message);break;}
 }
 location.reload();
};
</script></body></html>"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_blocks_traversal() {
        assert_eq!(sanitize_name("../../etc/passwd"), Some("passwd".into()));
        assert_eq!(sanitize_name(".."), None);
        assert_eq!(sanitize_name(""), None);
        assert_eq!(sanitize_name("photo.jpg"), Some("photo.jpg".into()));
    }

    #[test]
    fn percent_roundtrip() {
        let name = "mi foto (1).jpg";
        assert_eq!(percent_decode(&percent_encode(name)), name);
        assert_eq!(percent_decode("a%2Fb"), "a/b");
        assert_eq!(percent_decode("100%"), "100%");
    }

    #[test]
    fn private_link_paths_are_exactly_scoped() {
        assert_eq!(authorized_path("/secret/", Some("secret")), Some("/"));
        assert_eq!(
            authorized_path("/secret/f/a.txt", Some("secret")),
            Some("/f/a.txt")
        );
        assert_eq!(authorized_path("/secretish/f/a.txt", Some("secret")), None);
        assert_eq!(authorized_path("/f/a.txt", Some("secret")), None);
        assert_eq!(authorized_path("/f/a.txt", None), Some("/f/a.txt"));
    }

    #[test]
    fn numbered_upload_names_preserve_extensions() {
        assert_eq!(numbered_name("photo.jpg", 0), "photo.jpg");
        assert_eq!(numbered_name("photo.jpg", 2), "photo (2).jpg");
        assert_eq!(numbered_name(".env", 1), ".env (1)");
    }

    #[cfg(unix)]
    #[test]
    fn capability_root_rejects_absolute_symlink_escape() -> Result<()> {
        use std::os::unix::fs::symlink;

        let base =
            std::env::temp_dir().join(format!("lanxfer-web-{}", uuid::Uuid::new_v4().simple()));
        let root_path = base.join("root");
        let outside_path = base.join("outside");
        std::fs::create_dir_all(&root_path)?;
        std::fs::create_dir_all(&outside_path)?;
        std::fs::write(outside_path.join("secret.txt"), b"outside")?;
        symlink(outside_path.join("secret.txt"), root_path.join("leak"))?;

        let root = Dir::open_ambient_dir(&root_path, ambient_authority())?;
        assert!(root.open("leak").is_err());

        std::fs::remove_dir_all(base)?;
        Ok(())
    }
}
