use regex::Regex;
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
use pb::{
    text_bank_server::{TextBank, TextBankServer},
    *,
};

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
        Ok(Self {
            file: Mutex::new(file),
            path,
            flush_on_write,
        })
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
            if line.trim().is_empty() {
                continue;
            }
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

type ForwardLookup = DashMap<u64, Arc<DashMap<String, Arc<str>>>>;
type ReverseLookup = DashMap<(String, u64), Arc<DashMap<u64, RevEntry>>>;

/// The in-memory store: two views
/// - forward: text_id -> (lang -> string)
/// - reverse: (lang, hash) -> candidates
#[derive(Clone)]
struct Store {
    forward: Arc<ForwardLookup>,
    reverse: Arc<ReverseLookup>,
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

    /// Insert or update translation.
    /// If `text_id` is Some, update/insert (id,lang)->text.
    /// If `text_id` is None, return existing id if (lang,text) exists, else allocate.
    fn intern_with_id(&self, lang: &str, raw: &[u8], text_id: Option<u64>) -> Result<(u64, bool)> {
        let norm = Self::normalize_text(raw)?;
        let h = Self::rev_hash(lang, &norm);

        match text_id {
            Some(id) => {
                // Update path: remove old reverse entry if (id,lang) existed.
                if let Some(lm) = self.forward.get(&id) {
                    if let Some(old) = lm.get(lang) {
                        // If text is identical, short-circuit
                        if old.as_ref() == norm.as_str() {
                            return Ok((id, true));
                        }
                        self.reverse_remove(id, lang, &old);
                    }
                }

                let text_arc: Arc<str> = Arc::from(norm.clone());

                // forward
                let lang_map = self
                    .forward
                    .entry(id)
                    .or_insert_with(|| Arc::new(DashMap::new()))
                    .clone();
                lang_map.insert(lang.to_string(), text_arc.clone());

                // reverse
                let cand_map = self
                    .reverse
                    .entry((lang.to_string(), h))
                    .or_insert_with(|| Arc::new(DashMap::new()))
                    .clone();
                cand_map.insert(
                    id,
                    RevEntry {
                        text_arc: text_arc.clone(),
                        text_id: id,
                    },
                );

                // WAL
                self.wal.append(&WalRecord {
                    text_id: id,
                    lang: lang.to_string(),
                    text: norm,
                })?;
                Ok((id, true)) // existed=true because we updated/confirmed this id
            }
            None => {
                // Idempotent insert-by-(lang,text): check reverse bucket
                if let Some(bucket) = self.reverse.get(&(lang.to_string(), h)) {
                    for entry in bucket.value().iter() {
                        if &*entry.value().text_arc == norm.as_str() {
                            return Ok((entry.value().text_id, true));
                        }
                    }
                }

                // Allocate new id
                let id = {
                    let mut guard = self.next_id.lock();
                    let id = *guard;
                    *guard += 1;
                    id
                };

                let text_arc: Arc<str> = Arc::from(norm.clone());

                // forward
                let lang_map = self
                    .forward
                    .entry(id)
                    .or_insert_with(|| Arc::new(DashMap::new()))
                    .clone();
                lang_map.insert(lang.to_string(), text_arc.clone());

                // reverse
                let cand_map = self
                    .reverse
                    .entry((lang.to_string(), h))
                    .or_insert_with(|| Arc::new(DashMap::new()))
                    .clone();
                cand_map.insert(
                    id,
                    RevEntry {
                        text_arc: text_arc.clone(),
                        text_id: id,
                    },
                );

                // WAL
                self.wal.append(&WalRecord {
                    text_id: id,
                    lang: lang.to_string(),
                    text: norm,
                })?;
                Ok((id, false))
            }
        }
    }

    fn get_text(&self, text_id: u64, lang: &str) -> Option<Arc<str>> {
        self.forward
            .get(&text_id)
            .and_then(|lm| lm.get(lang).map(|v| v.clone()))
    }

    fn get_all(&self, text_id: u64) -> Vec<(String, Arc<str>)> {
        self.forward
            .get(&text_id)
            .map(|lm| {
                lm.iter()
                    .map(|kv| (kv.key().clone(), kv.value().clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Search normalized text by regex. If `lang` is Some, restrict to that language.
    /// When `lang` is None, return one hit per text_id (prefer 'en').
    fn search(&self, re: &Regex, lang: Option<&str>) -> Vec<(u64, Arc<str>)> {
        let mut out = Vec::new();

        match lang {
            Some(l) => {
                // Iterate all ids that have this language
                for item in self.forward.iter() {
                    let id = *item.key();
                    if let Some(txt) = item.value().get(l) {
                        if re.is_match(&txt) {
                            out.push((id, txt.clone()));
                        }
                    }
                }
            }
            None => {
                use std::collections::HashSet;
                let mut seen = HashSet::new();
                for item in self.forward.iter() {
                    let id = *item.key();
                    // Prefer en if present and matches
                    if let Some(txt) = item.value().get("en") {
                        if re.is_match(&txt) {
                            out.push((id, txt.clone()));
                            seen.insert(id);
                            continue;
                        }
                    }
                    // Else any first matching language
                    if !seen.contains(&id) {
                        for kv in item.value().iter() {
                            if re.is_match(kv.value()) {
                                out.push((id, kv.value().clone()));
                                break;
                            }
                        }
                    }
                }
            }
        }
        out
    }

    // Remove (id,lang) from reverse index using the OLD text (if present).
    fn reverse_remove(&self, text_id: u64, lang: &str, old_text: &str) {
        let h = Self::rev_hash(lang, old_text);
        if let Some(bucket) = self.reverse.get(&(lang.to_string(), h)) {
            bucket.value().remove(&text_id);
        }
        // If you want to GC empty buckets:
        // if let Some(mut b) = self.reverse.get_mut(&(lang.to_string(), h)) {
        //     if b.value().is_empty() { self.reverse.remove(&(lang.to_string(), h)); }
        // }
    }

    // Get preferred language: "en" if present, else any.
    fn get_any(&self, text_id: u64) -> Option<(String, Arc<str>)> {
        let lm = self.forward.get(&text_id)?;
        if let Some(v) = lm.get("en") {
            return Some(("en".to_string(), v.clone()));
        }
        // Fall back to any
        let result = lm
            .iter()
            .next()
            .map(|kv| (kv.key().clone(), kv.value().clone()));
        result
    }

    /// Load from WAL (rebuild both indexes) and set counter = max_id + 1.
    fn load_from_wal(&self) -> Result<()> {
        let mut max_id = 0u64;
        for rec in self.wal.replay()? {
            max_id = max_id.max(rec.text_id);
            let text_arc: Arc<str> = Arc::from(rec.text.clone());

            // forward
            let lang_map = self
                .forward
                .entry(rec.text_id)
                .or_insert_with(|| Arc::new(DashMap::new()))
                .clone();
            lang_map.insert(rec.lang.clone(), text_arc.clone());

            // reverse
            let h = Self::rev_hash(&rec.lang, &rec.text);
            let cand_map = self
                .reverse
                .entry((rec.lang.clone(), h))
                .or_insert_with(|| Arc::new(DashMap::new()))
                .clone();
            cand_map.insert(
                rec.text_id,
                RevEntry {
                    text_arc: text_arc.clone(),
                    text_id: rec.text_id,
                },
            );
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
        let id_opt = if r.text_id == 0 {
            None
        } else {
            Some(r.text_id)
        };
        match self.store.intern_with_id(&r.lang, &r.text, id_opt) {
            Ok((id, existed)) => Ok(Response::new(InternReply {
                text_id: id,
                existed,
            })),
            Err(e) => Err(Status::internal(format!("intern failed: {e}"))),
        }
    }

    async fn get(&self, req: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        let r = req.into_inner();
        let res = if r.lang.is_empty() {
            self.store.get_any(r.text_id).map(|(_, s)| s)
        } else {
            self.store.get_text(r.text_id, &r.lang)
        };
        match res {
            Some(s) => Ok(Response::new(GetReply {
                found: true,
                text: s.as_bytes().to_vec(),
            })),
            None => Ok(Response::new(GetReply {
                found: false,
                text: Vec::new(),
            })),
        }
    }

    async fn get_all(&self, req: Request<GetAllRequest>) -> Result<Response<GetAllReply>, Status> {
        let r = req.into_inner();
        let translations = self
            .store
            .get_all(r.text_id)
            .into_iter()
            .map(|(lang, s)| Translation {
                lang,
                text: s.as_bytes().to_vec(),
            })
            .collect();
        Ok(Response::new(GetAllReply {
            text_id: r.text_id,
            translations,
        }))
    }

    async fn search(&self, req: Request<SearchRequest>) -> Result<Response<SearchReply>, Status> {
        let r = req.into_inner();
        let pattern = r.pattern;
        let lang_opt = if r.lang.is_empty() {
            None
        } else {
            Some(r.lang)
        };
        let re = regex::Regex::new(&pattern)
            .map_err(|e| Status::invalid_argument(format!("bad regex: {e}")))?;

        let hits = self
            .store
            .search(&re, lang_opt.as_deref())
            .into_iter()
            .map(|(id, s)| SearchHit {
                text_id: id,
                text: s.as_bytes().to_vec(),
            })
            .collect();

        Ok(Response::new(SearchReply { hits }))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Config
    let addr = "0.0.0.0:50051".parse().unwrap();
    let wal = Wal::open("./data/textbank.wal", /*flush_on_write=*/ false)?;
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
