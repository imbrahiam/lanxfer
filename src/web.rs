//! Browser-based transfer for devices without lanxfer (phones on a hotspot).
//! Hand-rolled HTTP/1.1 over tokio — one page, raw-body uploads, streamed
//! downloads. ponytail: no HTTP crate; add axum only if this ever needs
//! TLS, auth, or range requests.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::net::tcp::OwnedWriteHalf;

use crate::{storage, ui, util};

pub async fn run(bind: &str, dir: PathBuf) -> Result<()> {
    let dir = dir.canonicalize().context("shared directory not found")?;
    let listener = TcpListener::bind(bind)
        .await
        .with_context(|| format!("cannot listen on {bind} (another lanxfer running?)"))?;
    let port = listener.local_addr()?.port();

    ui::section("Web transfer");
    ui::kv("folder", &dir.display().to_string());
    let urls: Vec<String> = util::local_ipv4s()
        .into_iter()
        .filter(|ip| ip != "127.0.0.1")
        .map(|ip| format!("http://{ip}:{port}/"))
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
    ui::warn("no pairing code: anyone on this network can upload and download");
    ui::info("Ctrl-C to stop");

    loop {
        let (stream, _) = listener.accept().await?;
        let dir = dir.clone();
        tokio::spawn(async move {
            if let Err(err) = handle(stream, &dir).await {
                log::debug!("web request failed: {err:#}");
            }
        });
    }
}

async fn handle(stream: tokio::net::TcpStream, dir: &Path) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);

    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();

    let mut content_length: u64 = 0;
    let mut expect_continue = false;
    for _ in 0..100 {
        let mut header = String::new();
        if reader.read_line(&mut header).await? == 0 || header.len() > 8192 {
            break;
        }
        let header = header.trim();
        if header.is_empty() {
            break;
        }
        let lower = header.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            content_length = v.trim().parse().unwrap_or(0);
        }
        if lower.starts_with("expect:") && lower.contains("100-continue") {
            expect_continue = true;
        }
    }

    let (path, query) = target.split_once('?').unwrap_or((target.as_str(), ""));
    match (method.as_str(), path) {
        ("GET", "/") => {
            let rel = query_param(query, "path").unwrap_or_default();
            send_page(&mut write, dir, &rel).await
        }
        ("GET", p) if p.starts_with("/f/") => send_file(&mut write, dir, &p[3..]).await,
        ("POST", "/up") => {
            if expect_continue {
                write.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await?;
            }
            let name = query_param(query, "name").unwrap_or_default();
            let rel = query_param(query, "path").unwrap_or_default();
            receive_upload(&mut reader, &mut write, dir, &name, &rel, content_length).await
        }
        _ => respond(&mut write, "404 Not Found", "text/plain", b"not found").await,
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
fn resolve_dir(root: &Path, rel: &str) -> Option<(PathBuf, String)> {
    if rel.is_empty() {
        return Some((root.to_path_buf(), String::new()));
    }
    let clean = storage::sanitize_relative_path(rel).ok()?;
    let path = root.join(&clean);
    path.is_dir()
        .then(|| (path, clean.to_string_lossy().replace('\\', "/")))
}

async fn send_page(write: &mut OwnedWriteHalf, root: &Path, rel: &str) -> Result<()> {
    let Some((dir, rel)) = resolve_dir(root, rel) else {
        return respond(write, "404 Not Found", "text/plain", b"not found").await;
    };
    // ponytail: blocking read_dir in async task — dir listings are tiny.
    let mut dirs: Vec<String> = Vec::new();
    let mut files: Vec<(String, u64)> = Vec::new();
    for entry in std::fs::read_dir(&dir)?.flatten() {
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
            "/".to_string()
        } else {
            format!("/?path={}", percent_encode(parent))
        };
        rows.push_str(&format!("<li><a href=\"{href}\">⬑ ..</a></li>"));
    }
    for name in &dirs {
        rows.push_str(&format!(
            "<li><a href=\"/?path={}\">📁 {}/</a></li>",
            percent_encode(&join(name)),
            escape_html(name)
        ));
    }
    for (name, size) in &files {
        rows.push_str(&format!(
            "<li><a href=\"/f/{}\" download>{}</a><small>{}</small></li>",
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
        .replace("{ROWS}", &rows);
    respond(write, "200 OK", "text/html", page.as_bytes()).await
}

async fn send_file(write: &mut OwnedWriteHalf, root: &Path, encoded: &str) -> Result<()> {
    let Ok(rel) = storage::sanitize_relative_path(&percent_decode(encoded)) else {
        return respond(write, "400 Bad Request", "text/plain", b"bad name").await;
    };
    let path = root.join(&rel);
    let Ok(mut file) = tokio::fs::File::open(&path).await else {
        return respond(write, "404 Not Found", "text/plain", b"not found").await;
    };
    let meta = file.metadata().await?;
    if !meta.is_file() {
        return respond(write, "404 Not Found", "text/plain", b"not found").await;
    }
    let len = meta.len();
    let name = rel.file_name().unwrap_or_default().to_string_lossy();
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {len}\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename*=UTF-8''{}\r\nConnection: close\r\n\r\n",
        percent_encode(&name)
    );
    write.write_all(header.as_bytes()).await?;
    tokio::io::copy(&mut file, write).await?;
    Ok(())
}

async fn receive_upload<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    write: &mut OwnedWriteHalf,
    root: &Path,
    raw_name: &str,
    rel: &str,
    len: u64,
) -> Result<()> {
    let (Some(name), Some((dir, _))) = (sanitize_name(raw_name), resolve_dir(root, rel)) else {
        return respond(write, "400 Bad Request", "text/plain", b"bad name").await;
    };
    let path = unique_path(&dir, &name);
    let tmp = path.with_file_name(format!(
        "{}.part",
        path.file_name().unwrap_or_default().to_string_lossy()
    ));
    let mut file = tokio::fs::File::create(&tmp).await?;
    let mut body = reader.take(len);
    let copied = tokio::io::copy(&mut body, &mut file).await;
    match copied {
        Ok(n) if n == len => {
            file.flush().await?;
            tokio::fs::rename(&tmp, &path).await?;
            log::info!("web upload: {} ({} bytes)", path.display(), len);
            respond(write, "200 OK", "text/plain", b"ok").await
        }
        _ => {
            let _ = tokio::fs::remove_file(&tmp).await;
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

async fn respond(write: &mut OwnedWriteHalf, status: &str, ctype: &str, body: &[u8]) -> Result<()> {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: {ctype}; charset=utf-8\r\nConnection: close\r\n\r\n",
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

fn unique_path(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join(name);
    if !path.exists() {
        return path;
    }
    let (stem, ext) = match name.rsplit_once('.') {
        Some((s, e)) if !s.is_empty() => (s, format!(".{e}")),
        _ => (name, String::new()),
    };
    for i in 1.. {
        let candidate = dir.join(format!("{stem} ({i}){ext}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    unreachable!()
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
    x.open('POST','/up?name='+encodeURIComponent(file.name)+'&path={PATH}');
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
}
