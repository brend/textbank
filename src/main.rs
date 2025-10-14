//! # TextBank
//!
//! A high-performance, multilingual text storage and retrieval service built with Rust and gRPC.
//!
//! TextBank provides efficient storage, retrieval, and search capabilities for multilingual text content,
//! with features including:
//! - Text normalization (Unicode NFC)
//! - Hash-based deduplication
//! - Language-aware storage and retrieval
//! - Regex-based search across languages
//! - Write-ahead logging for durability
//! - High-performance concurrent access

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

/// Generated protobuf definitions for the TextBank service.
pub mod pb {
    tonic::include_proto!("textbank");
}
use pb::{
    text_bank_server::{TextBank, TextBankServer},
    *,
};

/// Write-ahead log record persisted as one JSON object per line.
///
/// Each record represents a single text insertion or update operation,
/// containing the text ID, language code, and normalized UTF-8 text content.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct WalRecord {
    /// Unique identifier for the text entry
    text_id: u64,
    /// Language code (e.g., "en", "fr", "de")
    lang: String,
    /// Normalized UTF-8 text content
    text: String,
}

/// Very small append-only JSON-lines write-ahead log for durability.
///
/// The WAL ensures data persistence by logging all text operations to disk
/// before they're applied to the in-memory store. This provides crash recovery
/// and data durability guarantees.
struct Wal {
    /// File handle for the WAL, protected by a mutex for concurrent access
    file: Mutex<File>,
    /// Path to the WAL file on disk
    path: PathBuf,
    /// When true, calls flush() after each append for stronger durability at the cost of performance
    flush_on_write: bool,
}

impl Wal {
    /// Opens or creates a new write-ahead log at the specified path.
    ///
    /// # Arguments
    /// * `path` - The file path where the WAL should be stored
    /// * `flush_on_write` - Whether to flush after each write for stronger durability
    ///
    /// # Returns
    /// * `Result<Self>` - The WAL instance or an error if file operations fail
    ///
    /// # Examples
    /// ```
    /// let wal = Wal::open("./data/textbank.wal", false)?;
    /// ```
    fn open(path: impl Into<PathBuf>, flush_on_write: bool) -> Result<Self> {
        let path = path.into();
        // Create parent directories if they don't exist
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

    /// Appends a single record to the write-ahead log.
    ///
    /// The record is serialized as JSON and written to a new line in the WAL file.
    /// If `flush_on_write` is enabled, the data is immediately flushed to disk.
    ///
    /// # Arguments
    /// * `rec` - The WAL record to append
    ///
    /// # Returns
    /// * `Result<()>` - Success or an error if the write operation fails
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

    /// Replays all records from the WAL file for crash recovery.
    ///
    /// Reads the entire WAL file line by line, deserializing each JSON record.
    /// Empty lines are skipped to handle potential file corruption gracefully.
    ///
    /// # Returns
    /// * `Result<Vec<WalRecord>>` - All records from the WAL or an error if replay fails
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
///
/// When multiple texts hash to the same value, this entry helps distinguish
/// between them by storing the actual text content and associated ID.
#[derive(Clone)]
struct RevEntry {
    /// The actual text content as a reference-counted string
    text_arc: Arc<str>,
    /// The unique text identifier
    text_id: u64,
}

/// Type alias for the forward lookup index: text_id -> (language -> text_content)
type ForwardLookup = DashMap<u64, Arc<DashMap<String, Arc<str>>>>;

/// Type alias for the reverse lookup index: (language, hash) -> \[candidate_entries\]
type ReverseLookup = DashMap<(String, u64), Arc<DashMap<u64, RevEntry>>>;

/// The in-memory store providing two complementary views of the data.
///
/// This store maintains both forward and reverse indexes for efficient lookups:
/// - **Forward index**: Maps text IDs to their language-specific content
/// - **Reverse index**: Maps (language, content_hash) pairs to candidate text IDs
///
/// The dual-index design enables:
/// - Fast retrieval by ID and language
/// - Efficient deduplication using content hashes
/// - Quick existence checks for new content
/// - Concurrent access through lock-free data structures
#[derive(Clone)]
struct Store {
    /// Forward lookup: text_id -> (lang -> string)
    forward: Arc<ForwardLookup>,
    /// Reverse lookup: (lang, hash) -> candidates for collision resolution
    reverse: Arc<ReverseLookup>,
    /// Counter for generating unique text IDs
    next_id: Arc<parking_lot::Mutex<u64>>,
    /// Write-ahead log for persistence
    wal: Arc<Wal>,
}

impl Store {
    /// Creates a new store instance with the given write-ahead log.
    ///
    /// # Arguments
    /// * `wal` - The write-ahead log instance for persistence
    ///
    /// # Returns
    /// * `Self` - A new store instance with empty indexes
    fn new(wal: Wal) -> Self {
        Self {
            forward: Arc::new(DashMap::new()),
            reverse: Arc::new(DashMap::new()),
            next_id: Arc::new(parking_lot::Mutex::new(1)),
            wal: Arc::new(wal),
        }
    }

    /// Normalizes input text using Unicode NFC normalization.
    ///
    /// This ensures consistent text representation regardless of how the original
    /// text was encoded (e.g., composed vs. decomposed Unicode characters).
    /// Additional normalization like whitespace trimming could be added here.
    ///
    /// # Arguments
    /// * `s` - Raw bytes representing UTF-8 encoded text
    ///
    /// # Returns
    /// * `Result<String>` - Normalized text string or UTF-8 conversion error
    fn normalize_text(s: &[u8]) -> Result<String> {
        let s = std::str::from_utf8(s)?; // Treat input bytes as UTF-8
        Ok(s.nfc().collect::<String>())
    }

    /// Computes a hash key for the reverse index from language and normalized text.
    ///
    /// Uses a non-ASCII separator (U+001F) to reduce the chance of accidental
    /// collisions when concatenating language and text for hashing.
    ///
    /// # Arguments
    /// * `lang` - Language code
    /// * `norm` - Normalized text content
    ///
    /// # Returns
    /// * `u64` - XXH3 hash of the combined language and text
    fn rev_hash(lang: &str, norm: &str) -> u64 {
        // Using a non-ASCII separator to reduce accidental collisions with concat
        xxh3_64(format!("{}\u{001F}{}", lang, norm).as_bytes())
    }

    /// Inserts or updates a text translation with optional ID specification.
    ///
    /// This method handles two distinct use cases:
    /// 1. **Update mode** (`text_id` is Some): Updates or inserts text for a specific ID
    /// 2. **Insert mode** (`text_id` is None): Idempotent insert that returns existing ID if text already exists
    ///
    /// # Arguments
    /// * `lang` - Language code for the text
    /// * `raw` - Raw bytes representing the text content
    /// * `text_id` - Optional text ID for update mode
    ///
    /// # Returns
    /// * `Result<(u64, bool)>` - Tuple of (text_id, existed_flag) or error
    ///   - `text_id`: The ID of the stored text
    ///   - `existed_flag`: True if text already existed or was updated
    fn intern_with_id(&self, lang: &str, raw: &[u8], text_id: Option<u64>) -> Result<(u64, bool)> {
        let norm = Self::normalize_text(raw)?;
        let h = Self::rev_hash(lang, &norm);

        match text_id {
            Some(id) => {
                // Update path: remove old reverse entry if (id,lang) existed
                if let Some(lm) = self.forward.get(&id) {
                    if let Some(old) = lm.get(lang) {
                        // Short-circuit if text is identical
                        if old.as_ref() == norm.as_str() {
                            return Ok((id, true));
                        }
                        self.reverse_remove(id, lang, &old);
                    }
                }

                let text_arc: Arc<str> = Arc::from(norm.clone());

                // Update forward index
                let lang_map = self
                    .forward
                    .entry(id)
                    .or_insert_with(|| Arc::new(DashMap::new()))
                    .clone();
                lang_map.insert(lang.to_string(), text_arc.clone());

                // Update reverse index
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

                // Log to WAL
                self.wal.append(&WalRecord {
                    text_id: id,
                    lang: lang.to_string(),
                    text: norm,
                })?;
                Ok((id, true)) // existed=true because we updated/confirmed this id
            }
            None => {
                // Idempotent insert-by-(lang,text): check if content already exists
                if let Some(bucket) = self.reverse.get(&(lang.to_string(), h)) {
                    for entry in bucket.value().iter() {
                        if &*entry.value().text_arc == norm.as_str() {
                            return Ok((entry.value().text_id, true));
                        }
                    }
                }

                // Allocate new ID
                let id = {
                    let mut guard = self.next_id.lock();
                    let id = *guard;
                    *guard += 1;
                    id
                };

                let text_arc: Arc<str> = Arc::from(norm.clone());

                // Update forward index
                let lang_map = self
                    .forward
                    .entry(id)
                    .or_insert_with(|| Arc::new(DashMap::new()))
                    .clone();
                lang_map.insert(lang.to_string(), text_arc.clone());

                // Update reverse index
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

                // Log to WAL
                self.wal.append(&WalRecord {
                    text_id: id,
                    lang: lang.to_string(),
                    text: norm,
                })?;
                Ok((id, false))
            }
        }
    }

    /// Retrieves text content for a specific text ID and language.
    ///
    /// # Arguments
    /// * `text_id` - The unique text identifier
    /// * `lang` - The language code to retrieve
    ///
    /// # Returns
    /// * `Option<Arc<str>>` - The text content if found, None otherwise
    fn get_text(&self, text_id: u64, lang: &str) -> Option<Arc<str>> {
        self.forward
            .get(&text_id)
            .and_then(|lm| lm.get(lang).map(|v| v.clone()))
    }

    /// Retrieves all language translations for a specific text ID.
    ///
    /// # Arguments
    /// * `text_id` - The unique text identifier
    ///
    /// # Returns
    /// * `Vec<(String, Arc<str>)>` - Vector of (language_code, text_content) pairs
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

    /// Searches normalized text content using regular expressions.
    ///
    /// Supports both language-specific and cross-language search modes:
    /// - When `lang` is specified, searches only within that language
    /// - When `lang` is None, searches across all languages, preferring English results
    ///
    /// # Arguments
    /// * `re` - Compiled regular expression to match against
    /// * `lang` - Optional language filter
    ///
    /// # Returns
    /// * `Vec<(u64, Arc<str>)>` - Vector of (text_id, matching_text) pairs
    fn search(&self, re: &Regex, lang: Option<&str>) -> Vec<(u64, Arc<str>)> {
        let mut out = Vec::new();

        match lang {
            Some(l) => {
                // Language-specific search: iterate all IDs that have this language
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
                // Cross-language search with English preference
                use std::collections::HashSet;
                let mut seen = HashSet::new();

                for item in self.forward.iter() {
                    let id = *item.key();

                    // Prefer English if present and matches
                    if let Some(txt) = item.value().get("en") {
                        if re.is_match(&txt) {
                            out.push((id, txt.clone()));
                            seen.insert(id);
                            continue;
                        }
                    }

                    // Otherwise, use first matching language
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

    /// Removes a text ID from the reverse index using the old text content.
    ///
    /// This is used during updates to clean up stale reverse index entries
    /// when text content changes for an existing ID.
    ///
    /// # Arguments
    /// * `text_id` - The text ID to remove from reverse index
    /// * `lang` - The language of the text being removed
    /// * `old_text` - The previous text content (needed to compute the hash)
    fn reverse_remove(&self, text_id: u64, lang: &str, old_text: &str) {
        let h = Self::rev_hash(lang, old_text);
        if let Some(bucket) = self.reverse.get(&(lang.to_string(), h)) {
            bucket.value().remove(&text_id);
        }
        // Optional: GC empty buckets to save memory
        // if let Some(mut b) = self.reverse.get_mut(&(lang.to_string(), h)) {
        //     if b.value().is_empty() {
        //         self.reverse.remove(&(lang.to_string(), h));
        //     }
        // }
    }

    /// Retrieves text in a preferred language, falling back to any available language.
    ///
    /// Attempts to return English content first, then falls back to any available
    /// language if English is not available.
    ///
    /// # Arguments
    /// * `text_id` - The unique text identifier
    ///
    /// # Returns
    /// * `Option<(String, Arc<str>)>` - Tuple of (language_code, text_content) if found
    fn get_any(&self, text_id: u64) -> Option<(String, Arc<str>)> {
        let lm = self.forward.get(&text_id)?;

        // Prefer English if available
        if let Some(v) = lm.get("en") {
            return Some(("en".to_string(), v.clone()));
        }

        // Fall back to any available language
        let result = lm
            .iter()
            .next()
            .map(|kv| (kv.key().clone(), kv.value().clone()));
        result
    }

    /// Loads data from the write-ahead log to rebuild both indexes.
    ///
    /// This method is used for crash recovery - it replays all operations from
    /// the WAL to reconstruct the in-memory state. It also sets the next ID
    /// counter to be one greater than the maximum ID found in the WAL.
    ///
    /// # Returns
    /// * `Result<()>` - Success or error if WAL replay fails
    fn load_from_wal(&self) -> Result<()> {
        let mut max_id = 0u64;

        for rec in self.wal.replay()? {
            max_id = max_id.max(rec.text_id);
            let text_arc: Arc<str> = Arc::from(rec.text.clone());

            // Rebuild forward index
            let lang_map = self
                .forward
                .entry(rec.text_id)
                .or_insert_with(|| Arc::new(DashMap::new()))
                .clone();
            lang_map.insert(rec.lang.clone(), text_arc.clone());

            // Rebuild reverse index
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

        // Set next ID to be one greater than the maximum found
        *self.next_id.lock() = max_id.saturating_add(1);
        Ok(())
    }
}

/// gRPC service implementation for the TextBank API.
///
/// This struct implements the `TextBank` trait generated from the protobuf
/// definition, providing all the public API endpoints for text storage,
/// retrieval, and search operations.
#[derive(Clone)]
struct Svc {
    /// The underlying data store
    store: Store,
}

#[tonic::async_trait]
impl TextBank for Svc {
    /// Inserts or updates a text translation.
    ///
    /// This endpoint supports two modes of operation:
    /// - **Insert mode**: When `text_id` is 0 or unset, either allocates a new ID or returns the existing ID if the (language, text) pair already exists
    /// - **Update mode**: When `text_id` is specified, updates or inserts the text for that specific ID
    ///
    /// # Arguments
    /// * `req` - The gRPC request containing text, language, and optional text ID
    ///
    /// # Returns
    /// * `Result<Response<InternReply>, Status>` - Reply with text ID and existence flag
    ///
    /// # Errors
    /// * `Status::invalid_argument` - When language is empty
    /// * `Status::internal` - When internal operations fail
    async fn intern(&self, req: Request<InternRequest>) -> Result<Response<InternReply>, Status> {
        let r = req.into_inner();

        // Validate required parameters
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

    /// Retrieves a text translation by ID and optional language.
    ///
    /// When language is specified, returns the text in that specific language.
    /// When language is empty, returns text in a preferred language (English first, then any).
    ///
    /// # Arguments
    /// * `req` - The gRPC request containing text ID and optional language
    ///
    /// # Returns
    /// * `Result<Response<GetReply>, Status>` - Reply with found flag and text content
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

    /// Retrieves all language translations for a specific text ID.
    ///
    /// Returns all available language versions of the text content associated
    /// with the given ID.
    ///
    /// # Arguments
    /// * `req` - The gRPC request containing the text ID
    ///
    /// # Returns
    /// * `Result<Response<GetAllReply>, Status>` - Reply with text ID and all translations
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

    /// Searches text content using regular expressions.
    ///
    /// Supports both language-specific and cross-language search:
    /// - When `lang` is specified, searches only within that language
    /// - When `lang` is empty, searches across all languages (preferring English results)
    ///
    /// The regex is applied to normalized (Unicode NFC) text content.
    ///
    /// # Arguments
    /// * `req` - The gRPC request containing regex pattern and optional language filter
    ///
    /// # Returns
    /// * `Result<Response<SearchReply>, Status>` - Reply with matching text entries
    ///
    /// # Errors
    /// * `Status::invalid_argument` - When the regex pattern is invalid
    async fn search(&self, req: Request<SearchRequest>) -> Result<Response<SearchReply>, Status> {
        let r = req.into_inner();
        let pattern = r.pattern;
        let lang_opt = if r.lang.is_empty() {
            None
        } else {
            Some(r.lang)
        };

        // Compile the regex pattern
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

/// Main entry point for the TextBank service.
///
/// Initializes the write-ahead log, creates the in-memory store, loads existing
/// data from the WAL, and starts the gRPC server. The server runs until interrupted
/// by a CTRL-C signal.
#[tokio::main]
async fn main() -> Result<()> {
    // Server configuration
    let addr = "0.0.0.0:50051".parse().unwrap();

    // Initialize WAL and store
    let wal = Wal::open("./data/textbank.wal", /*flush_on_write=*/ false)?;
    let store = Store::new(wal);

    // Load existing data from WAL for crash recovery
    store.load_from_wal()?;

    let svc = Svc { store }.clone();
    println!("TextBank listening on {addr}");

    // Start gRPC server with graceful shutdown on CTRL-C
    Server::builder()
        .add_service(TextBankServer::new(svc))
        .serve_with_shutdown(addr, async {
            let _ = signal::ctrl_c().await;
        })
        .await?;

    Ok(())
}
