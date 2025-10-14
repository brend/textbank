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
use tonic::{Request, Response, Status};
use unicode_normalization::UnicodeNormalization;
use xxhash_rust::xxh3::xxh3_64;

/// Generated protobuf definitions for the TextBank service.
pub mod pb {
    tonic::include_proto!("textbank");
}
pub use pb::{
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

/// Write-ahead log for persisting text operations to disk.
///
/// The WAL ensures durability by logging all text insertions and updates
/// as JSON records, one per line. On startup, the WAL is replayed to
/// reconstruct the in-memory data structures.
#[derive(Debug)]
struct Wal {
    /// File handle for writing new records
    file: Mutex<File>,
    /// Path to the WAL file for error reporting
    path: PathBuf,
    /// Whether to flush writes immediately to disk
    flush_on_write: bool,
}

impl Wal {
    /// Opens or creates a WAL file at the specified path.
    ///
    /// # Arguments
    /// * `path` - Path to the WAL file
    /// * `flush_on_write` - Whether to flush writes immediately to disk
    ///
    /// # Returns
    /// * `Result<Self>` - New WAL instance or error
    ///
    /// # Errors
    /// * Returns error if the file cannot be opened or created
    fn open<P: Into<PathBuf>>(path: P, flush_on_write: bool) -> Result<Self> {
        let path = path.into();

        // Create parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        Ok(Wal {
            file: Mutex::new(file),
            path,
            flush_on_write,
        })
    }

    /// Appends a record to the WAL.
    ///
    /// # Arguments
    /// * `record` - The record to append
    ///
    /// # Returns
    /// * `Result<()>` - Success or error
    fn append(&self, record: &WalRecord) -> Result<()> {
        let mut file = self.file.lock();
        serde_json::to_writer(&mut *file, record)?;
        writeln!(file)?;
        if self.flush_on_write {
            file.flush()?;
        }
        Ok(())
    }

    /// Replays the WAL, calling the provided closure for each record.
    ///
    /// # Arguments
    /// * `f` - Closure to call for each record
    ///
    /// # Returns
    /// * `Result<()>` - Success or error
    fn replay<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(WalRecord) -> Result<()>,
    {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            let record: WalRecord = serde_json::from_str(&line)?;
            f(record)?;
        }
        Ok(())
    }
}

/// Reverse lookup entry for content-based deduplication.
///
/// Maps from (language, content hash) to text ID, enabling efficient
/// detection of duplicate content within the same language.
#[derive(Debug, Clone)]
struct RevEntry {
    /// Arc reference to the original text content
    text_arc: Arc<String>,
    /// Text ID associated with this content
    text_id: u64,
}

/// Forward lookup map: text ID -> language -> text content
type ForwardLookup = DashMap<u64, DashMap<String, Arc<String>>>;

/// Reverse lookup map: (language, content hash) -> reverse entry
type ReverseLookup = DashMap<(String, u64), RevEntry>;

/// High-performance concurrent text storage with deduplication.
///
/// The Store provides thread-safe operations for storing and retrieving
/// multilingual text content. It uses hash-based deduplication to avoid
/// storing identical content multiple times within the same language.
///
/// # Thread Safety
/// All operations are thread-safe and can be called concurrently from
/// multiple tasks or threads without external synchronization.
#[derive(Debug)]
pub struct Store {
    /// Forward lookup: ID -> language -> text
    forward: ForwardLookup,
    /// Reverse lookup: (lang, hash) -> text info
    reverse: ReverseLookup,
    /// Next available text ID
    next_id: Mutex<u64>,
    /// Write-ahead log for durability
    wal: Wal,
}

impl Store {
    /// Creates a new Store with WAL persistence.
    ///
    /// # Arguments
    /// * `wal_path` - Path to the write-ahead log file
    /// * `flush_on_write` - Whether to flush WAL writes immediately
    ///
    /// # Returns
    /// * `Result<Self>` - New Store instance or error
    pub fn new<P: Into<PathBuf>>(wal_path: P, flush_on_write: bool) -> Result<Self> {
        let wal = Wal::open(wal_path, flush_on_write)?;
        let store = Store {
            forward: DashMap::new(),
            reverse: DashMap::new(),
            next_id: Mutex::new(1),
            wal,
        };

        // Load existing data from WAL
        store.load_from_wal()?;

        Ok(store)
    }

    /// Normalizes text using Unicode NFC normalization.
    ///
    /// # Arguments
    /// * `text` - Input text to normalize
    ///
    /// # Returns
    /// * `String` - Normalized text
    fn normalize_text(text: &str) -> String {
        text.nfc().collect()
    }

    /// Computes hash for reverse lookup.
    ///
    /// Uses XXH3 for fast, high-quality hashing of normalized text content.
    ///
    /// # Arguments
    /// * `text` - Text to hash
    ///
    /// # Returns
    /// * `u64` - Hash value
    fn rev_hash(text: &str) -> u64 {
        xxh3_64(text.as_bytes())
    }

    /// Interns text with optional ID specification.
    ///
    /// This is the core text storage method supporting both insert and update modes:
    /// - Insert mode: When `id_opt` is None, performs content-based deduplication
    /// - Update mode: When `id_opt` is Some, updates or inserts for specific ID
    ///
    /// # Arguments
    /// * `lang` - Language code for the text
    /// * `text` - Raw text content (will be normalized)
    /// * `id_opt` - Optional text ID for update mode
    ///
    /// # Returns
    /// * `Result<(u64, bool)>` - Tuple of (text_id, existed) or error
    ///
    /// # Thread Safety
    /// This method is thread-safe and handles concurrent access properly.
    pub fn intern_with_id(
        &self,
        lang: &str,
        text: &[u8],
        id_opt: Option<u64>,
    ) -> Result<(u64, bool)> {
        // Convert bytes to string and normalize
        let text_str = String::from_utf8_lossy(text);
        let normalized = Self::normalize_text(&text_str);
        let text_arc = Arc::new(normalized.clone());

        match id_opt {
            Some(id) => {
                // Update mode: use specified ID
                let lang_map = self.forward.entry(id).or_default();
                let existed = lang_map.contains_key(lang);

                // Insert/update in forward map
                lang_map.insert(lang.to_string(), text_arc.clone());

                // Update reverse map
                let hash = Self::rev_hash(&normalized);
                let rev_entry = RevEntry {
                    text_arc: text_arc.clone(),
                    text_id: id,
                };
                self.reverse.insert((lang.to_string(), hash), rev_entry);

                // Update next_id if necessary
                {
                    let mut next_id = self.next_id.lock();
                    if id >= *next_id {
                        *next_id = id + 1;
                    }
                }

                // Log to WAL
                let record = WalRecord {
                    text_id: id,
                    lang: lang.to_string(),
                    text: normalized,
                };
                self.wal.append(&record)?;

                Ok((id, existed))
            }
            None => {
                // Insert mode: check for existing content
                let hash = Self::rev_hash(&normalized);
                let key = (lang.to_string(), hash);

                if let Some(existing) = self.reverse.get(&key) {
                    // Content already exists
                    Ok((existing.text_id, true))
                } else {
                    // Allocate new ID
                    let id = {
                        let mut next_id = self.next_id.lock();
                        let id = *next_id;
                        *next_id += 1;
                        id
                    };

                    // Insert in both maps
                    let lang_map = self.forward.entry(id).or_default();
                    lang_map.insert(lang.to_string(), text_arc.clone());

                    let rev_entry = RevEntry {
                        text_arc: text_arc.clone(),
                        text_id: id,
                    };
                    self.reverse.insert(key, rev_entry);

                    // Log to WAL
                    let record = WalRecord {
                        text_id: id,
                        lang: lang.to_string(),
                        text: normalized,
                    };
                    self.wal.append(&record)?;

                    Ok((id, false))
                }
            }
        }
    }

    /// Retrieves text by ID and optional language.
    ///
    /// # Arguments
    /// * `id` - Text ID to retrieve
    /// * `lang_opt` - Optional language filter
    ///
    /// # Returns
    /// * `Option<Arc<String>>` - Text content if found
    pub fn get_text(&self, id: u64, lang_opt: Option<&str>) -> Option<Arc<String>> {
        let lang_map = self.forward.get(&id)?;

        match lang_opt {
            Some(lang) => lang_map.get(lang).map(|v| v.value().clone()),
            None => {
                // Return English if available, otherwise any language
                lang_map
                    .get("en")
                    .map(|v| v.value().clone())
                    .or_else(|| lang_map.iter().next().map(|kv| kv.value().clone()))
            }
        }
    }

    /// Retrieves all translations for a text ID.
    ///
    /// # Arguments
    /// * `id` - Text ID to retrieve all translations for
    ///
    /// # Returns
    /// * `Vec<(String, Arc<String>)>` - Vector of (language, text) pairs
    pub fn get_all(&self, id: u64) -> Vec<(String, Arc<String>)> {
        if let Some(lang_map) = self.forward.get(&id) {
            lang_map
                .iter()
                .map(|kv| (kv.key().clone(), kv.value().clone()))
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Searches text content using regular expressions.
    ///
    /// # Arguments
    /// * `pattern` - Regex pattern to search for
    /// * `lang_opt` - Optional language filter
    ///
    /// # Returns
    /// * `Result<Vec<(u64, Arc<String>)>>` - Vector of (text_id, text) pairs or error
    pub fn search(&self, pattern: &str, lang_opt: Option<&str>) -> Result<Vec<(u64, Arc<String>)>> {
        let regex = Regex::new(pattern)?;
        let mut results = Vec::new();

        for lang_map_ref in self.forward.iter() {
            let id = *lang_map_ref.key();
            let lang_map = lang_map_ref.value();

            match lang_opt {
                Some(target_lang) => {
                    // Search specific language
                    if let Some(text) = lang_map.get(target_lang) {
                        if regex.is_match(&text) {
                            results.push((id, text.value().clone()));
                        }
                    }
                }
                None => {
                    // Search all languages, prefer English
                    let text = lang_map
                        .get("en")
                        .map(|v| v.value().clone())
                        .or_else(|| lang_map.iter().next().map(|kv| kv.value().clone()));

                    if let Some(text) = text {
                        if regex.is_match(&text) {
                            results.push((id, text));
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Removes an entry from the reverse lookup map.
    ///
    /// Used internally during WAL replay to handle updates correctly.
    fn reverse_remove(&self, lang: &str, old_text: &str) -> Option<u64> {
        let hash = Self::rev_hash(old_text);
        let key = (lang.to_string(), hash);
        self.reverse.remove(&key).map(|(_, v)| v.text_id)
    }

    /// Gets any available text for an ID, preferring English.
    ///
    /// # Arguments
    /// * `id` - Text ID
    ///
    /// # Returns
    /// * `Option<Arc<String>>` - Text content if found
    pub fn get_any(&self, id: u64) -> Option<Arc<String>> {
        let lang_map = self.forward.get(&id)?;

        // Prefer English, otherwise return any available
        lang_map
            .get("en")
            .map(|v| v.value().clone())
            .or_else(|| lang_map.iter().next().map(|kv| kv.value().clone()))
    }

    /// Loads data from the write-ahead log on startup.
    ///
    /// # Returns
    /// * `Result<()>` - Success or error
    fn load_from_wal(&self) -> Result<()> {
        let mut max_id = 0u64;

        self.wal.replay(|record| {
            let text_arc = Arc::new(record.text.clone());

            // Update max_id
            if record.text_id > max_id {
                max_id = record.text_id;
            }

            // Check if we already have text for this (ID, language) pair
            let lang_map = self.forward.entry(record.text_id).or_default();
            if let Some(old_text) = lang_map.get(&record.lang) {
                // Remove old reverse entry
                self.reverse_remove(&record.lang, &old_text);
            }

            // Insert/update forward map
            lang_map.insert(record.lang.clone(), text_arc.clone());

            // Insert/update reverse map
            let hash = Self::rev_hash(&record.text);
            let rev_entry = RevEntry {
                text_arc,
                text_id: record.text_id,
            };
            self.reverse.insert((record.lang, hash), rev_entry);

            Ok(())
        })?;

        // Update next_id
        *self.next_id.lock() = max_id + 1;

        Ok(())
    }
}

/// gRPC service implementation wrapping the Store.
#[derive(Debug)]
pub struct Svc {
    /// Reference to the text store
    pub store: Arc<Store>,
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
            self.store.get_text(r.text_id, None)
        } else {
            self.store.get_text(r.text_id, Some(&r.lang))
        };

        match res {
            Some(text) => Ok(Response::new(GetReply {
                found: true,
                text: text.as_bytes().to_vec(),
            })),
            None => Ok(Response::new(GetReply {
                found: false,
                text: Vec::new(),
            })),
        }
    }

    /// Retrieves all available language translations for a specific text ID.
    ///
    /// # Arguments
    /// * `req` - The gRPC request containing the text ID
    ///
    /// # Returns
    /// * `Result<Response<GetAllReply>, Status>` - Reply with all translations
    async fn get_all(&self, req: Request<GetAllRequest>) -> Result<Response<GetAllReply>, Status> {
        let r = req.into_inner();
        let translations = self.store.get_all(r.text_id);

        let translations = translations
            .into_iter()
            .map(|(lang, text)| Translation {
                lang,
                text: text.as_bytes().to_vec(),
            })
            .collect();

        Ok(Response::new(GetAllReply {
            text_id: r.text_id,
            translations,
        }))
    }

    /// Searches text content using regular expressions with optional language filtering.
    ///
    /// # Arguments
    /// * `req` - The gRPC request containing search pattern and optional language
    ///
    /// # Returns
    /// * `Result<Response<SearchReply>, Status>` - Reply with search results
    async fn search(&self, req: Request<SearchRequest>) -> Result<Response<SearchReply>, Status> {
        let r = req.into_inner();

        let lang_opt = if r.lang.is_empty() {
            None
        } else {
            Some(r.lang.as_str())
        };

        match self.store.search(&r.pattern, lang_opt) {
            Ok(results) => {
                let hits = results
                    .into_iter()
                    .map(|(text_id, text)| SearchHit {
                        text_id,
                        text: text.as_bytes().to_vec(),
                    })
                    .collect();

                Ok(Response::new(SearchReply { hits }))
            }
            Err(e) => Err(Status::invalid_argument(format!("invalid regex: {e}"))),
        }
    }
}
