use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Result;
use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::signal;
use tonic::{transport::Server, Request, Response, Status};
use unicode_normalization::UnicodeNormalization;
use xxhash_rust::xxh3::xxh3_64;

pub mod pb {
    tonic::include_proto!("textbank");
}
use pb::{text_bank_server::{TextBank, TextBankServer}, *};

/// WAL record persisted as one JSON object per line.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct WalRecord {
    text_id: u64,
    lang: String,
    text: String, // stored as UTF-8
}

/// Very small append-only JSON-lines WAL.
struct Wal {
    file: Mutex<File>,
    path: PathBuf,
    // When true, call flush() after each append for stronger durability.
    flush_on_write: bool,
}
impl Wal {
    fn open(path: impl Into<PathBuf>, flush_on_write: bool) -> Result<Self> {
        let path = path.into();
        std::fs::create_dir_all(path.parent().unwrap_or_else(|| std::path::Path::new(".")))?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        Ok(Self { file: Mutex::new(file), path, flush_on_write })
    }

    fn append(&self, rec: &WalRecord) -> Result<()> {
        let mut f = self.file.lock();
        let line = serde_json::to_string(rec)?;
        f.write_all(line.as_bytes())?;
        f.write_all(b"\n")?;
        if self.flush_on_write {
            f.flush()?;
        }
        Ok(())
    }

    fn replay(&self) -> Result<Vec<WalRecord>> {
        let f = std::fs::OpenOptions::new().read(true).open(&self.path)?;
        let rdr = BufReader::new(f);
        let mut out = Vec::new();
        for line in rdr.lines() {
            let line = line?;
            if line.trim().is_empty() { continue; }
            let rec: WalRecord = serde_json::from_str(&line)?;
            out.push(rec);
        }
        Ok(out)
    }
}

/// Reverse index entry to disambiguate rare hash collisions.
#[derive(Clone)]
struct RevEntry {
    text_arc: Arc<str>,
    text_id: u64,
}

/// The in-memory store: two views
/// - forward: text_id -> (lang -> string)
/// - reverse: (lang, hash) -> candidates
#[derive(Clone)]
struct Store {
    forward: Arc<DashMap<u64, Arc<DashMap<String, Arc<str>>>>>,
    reverse: Arc<DashMap<(String, u64), Arc<DashMap<u64, RevEntry>>>>,
    next_id: Arc<parking_lot::Mutex<u64>>,
    wal: Arc<Wal>,
}

impl Store {
    fn new(wal: Wal) -> Self {
        Self {
            forward: Arc::new(DashMap::new()),
            reverse: Arc::new(DashMap::new()),
            next_id: Arc::new(parking_lot::Mutex::new(1)),
            wal: Arc::new(wal),
        }
    }

    /// Normalize input text (NFC). You can extend with whitespace trimming or language-aware folding.
    fn normalize_text(s: &[u8]) -> Result<String> {
        let s = std::str::from_utf8(s)?; // input is bytes in proto; treat as UTF-8
        Ok(s.nfc().collect::<String>())
    }

    /// Compute reverse key hash from lang + separator + normalized text
    fn rev_hash(lang: &str, norm: &str) -> u64 {
        // Using a non-ASCII separator to reduce accidental collisions with concat
        xxh3_64(format!("{}\u{001F}{}", lang, norm).as_bytes())
    }

    /// Intern (string, lang) -> id, idempotent. Persists via WAL.
    fn intern(&self, lang: &str, raw: &[u8]) -> Result<(u64, bool)> {
        let norm = Self::normalize_text(raw)?;
        let h = Self::rev_hash(lang, &norm);

        // Fast path: check reverse for existing exact match
        if let Some(bucket) = self.reverse.get(&(lang.to_string(), h)) {
            for entry in bucket.value().iter() {
                if let Some(e) = bucket.value().get(entry.key()) {
                    if &*e.text_arc == norm.as_str() {
                        return Ok((e.text_id, true));
                    }
                }
            }
        }

        // Miss: allocate id
        let text_id = {
            let mut guard = self.next_id.lock();
            let id = *guard;
            *guard += 1;
            id
        };

        let text_arc: Arc<str> = Arc::from(norm.clone());

        // Insert into forward view
        let lang_map = self.forward
            .entry(text_id)
            .or_insert_with(|| Arc::new(DashMap::new()))
            .clone();
        lang_map.insert(lang.to_string(), text_arc.clone());

        // Insert into reverse view
        let cand_map = self.reverse
            .entry((lang.to_string(), h))
            .or_insert_with(|| Arc::new(DashMap::new()))
            .clone();
        cand_map.insert(text_id, RevEntry { text_arc: text_arc.clone(), text_id });

        // Persist
        self.wal.append(&WalRecord {
            text_id,
            lang: lang.to_string(),
            text: norm,
        })?;

        Ok((text_id, false))
    }

    /// Get (id, lang) -> string
    fn get(&self, text_id: u64, lang: &str) -> Option<Arc<str>> {
        self.forward.get(&text_id).and_then(|lm| lm.get(lang).map(|v| v.clone()))
    }

    /// Load from WAL (rebuild both indexes) and set counter = max_id + 1.
    fn load_from_wal(&self) -> Result<()> {
        let mut max_id = 0u64;
        for rec in self.wal.replay()? {
            max_id = max_id.max(rec.text_id);
            let text_arc: Arc<str> = Arc::from(rec.text.clone());

            // forward
            let lang_map = self.forward
                .entry(rec.text_id)
                .or_insert_with(|| Arc::new(DashMap::new()))
                .clone();
            lang_map.insert(rec.lang.clone(), text_arc.clone());

            // reverse
            let h = Self::rev_hash(&rec.lang, &rec.text);
            let cand_map = self.reverse
                .entry((rec.lang.clone(), h))
                .or_insert_with(|| Arc::new(DashMap::new()))
                .clone();
            cand_map.insert(rec.text_id, RevEntry { text_arc: text_arc.clone(), text_id: rec.text_id });
        }
        *self.next_id.lock() = max_id.saturating_add(1);
        Ok(())
    }
}

/// gRPC server implementation
#[derive(Clone)]
struct Svc {
    store: Store,
}

#[tonic::async_trait]
impl TextBank for Svc {
    async fn intern(&self, req: Request<InternRequest>) -> Result<Response<InternReply>, Status> {
        let r = req.into_inner();
        if r.lang.is_empty() {
            return Err(Status::invalid_argument("lang must not be empty"));
        }
        match self.store.intern(&r.lang, &r.text) {
            Ok((id, existed)) => Ok(Response::new(InternReply { text_id: id, existed })),
            Err(e) => Err(Status::internal(format!("intern failed: {e}"))),
        }
    }

    async fn get(&self, req: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        let r = req.into_inner();
        match self.store.get(r.text_id, &r.lang) {
            Some(s) => Ok(Response::new(GetReply { found: true, text: s.as_bytes().to_vec() })),
            None => Ok(Response::new(GetReply { found: false, text: Vec::new() })),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Config
    let addr = "0.0.0.0:50051".parse().unwrap();
    let wal = Wal::open("./data/textbank.wal", /*flush_on_write=*/false)?;
    let store = Store::new(wal);
    store.load_from_wal()?;

    let svc = Svc { store }.clone();
    println!("TextBank listening on {addr}");

    Server::builder()
        .add_service(TextBankServer::new(svc))
        .serve_with_shutdown(addr, async {
            let _ = signal::ctrl_c().await;
        })
        .await?;

    Ok(())
}