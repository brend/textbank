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
    io::{BufRead, BufReader, ErrorKind, Write},
    path::{Path, PathBuf},
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

/// Durability mode for write-ahead-log commits.
///
/// The selected mode determines what happens after each append before a write is
/// acknowledged:
/// - `None`: no flush or fsync
/// - `Flush`: `flush()` only
/// - `FsyncData`: `flush()` + `sync_data()`
/// - `FsyncAll`: `flush()` + `sync_all()`
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WalDurability {
    None,
    Flush,
    #[default]
    FsyncData,
    FsyncAll,
}

impl WalDurability {
    /// Parses a durability mode from an environment variable.
    ///
    /// If the variable is unset, returns the default production mode.
    pub fn from_env(var_name: &str) -> Result<Self> {
        match std::env::var(var_name) {
            Ok(raw) => raw.parse(),
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }
}

impl std::str::FromStr for WalDurability {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<Self> {
        let normalized = input.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "none" | "off" => Ok(Self::None),
            "flush" => Ok(Self::Flush),
            "fsync_data" | "fsync-data" | "data" => Ok(Self::FsyncData),
            "fsync_all" | "fsync-all" | "all" => Ok(Self::FsyncAll),
            _ => anyhow::bail!(
                "invalid WAL durability mode '{input}'. expected one of: none, flush, fsync_data, fsync_all"
            ),
        }
    }
}

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
    /// Durability behavior for acknowledged writes
    durability: WalDurability,
}

impl Wal {
    /// Opens or creates a WAL file at the specified path.
    ///
    /// # Arguments
    /// * `path` - Path to the WAL file
    /// * `durability` - Durability behavior for acknowledged writes
    ///
    /// # Returns
    /// * `Result<Self>` - New WAL instance or error
    ///
    /// # Errors
    /// * Returns error if the file cannot be opened or created
    fn open<P: Into<PathBuf>>(path: P, durability: WalDurability) -> Result<Self> {
        let path = path.into();

        // Create parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Try to create the WAL atomically so we can reliably detect first creation.
        let (file, created_new) = match std::fs::OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)
        {
            Ok(file) => (file, true),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)?;
                (file, false)
            }
            Err(e) => return Err(e.into()),
        };

        if created_new {
            Self::sync_parent_dir(path.parent())?;
        }

        Ok(Wal {
            file: Mutex::new(file),
            path,
            durability,
        })
    }

    #[cfg(unix)]
    fn sync_parent_dir(parent: Option<&Path>) -> Result<()> {
        if let Some(parent) = parent {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    }

    #[cfg(not(unix))]
    fn sync_parent_dir(_parent: Option<&Path>) -> Result<()> {
        Ok(())
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
        match self.durability {
            WalDurability::None => {}
            WalDurability::Flush => {
                file.flush()?;
            }
            WalDurability::FsyncData => {
                file.flush()?;
                file.sync_data()?;
            }
            WalDurability::FsyncAll => {
                file.flush()?;
                file.sync_all()?;
            }
        }
        Ok(())
    }

    /// Replays the WAL, calling the provided closure for each record.
    ///
    /// A truncated final line (for example, from a crash during append) is
    /// treated as a partial write and ignored. Malformed records elsewhere
    /// are treated as corruption and returned as errors.
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
        let mut reader = BufReader::new(file);
        let mut line = String::new();

        loop {
            line.clear();
            let bytes_read = reader.read_line(&mut line)?;
            if bytes_read == 0 {
                break;
            }

            let had_newline = line.ends_with('\n');
            if had_newline {
                line.pop(); // Remove '\n'
                if line.ends_with('\r') {
                    line.pop(); // Handle Windows-style line endings
                }
            }

            if line.is_empty() {
                continue;
            }

            match serde_json::from_str::<WalRecord>(&line) {
                Ok(record) => f(record)?,
                Err(e) => {
                    let at_eof = reader.fill_buf()?.is_empty();
                    let truncated_tail = at_eof && !had_newline;
                    if truncated_tail {
                        break;
                    }
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }
}

/// Reverse lookup entry for content-based deduplication.
///
/// Stored in per-hash candidate lists so collisions can be resolved by
/// comparing full normalized text content.
#[derive(Debug, Clone)]
struct RevEntry {
    /// Arc reference to the original text content
    text_arc: Arc<String>,
    /// Text ID associated with this content
    text_id: u64,
}

/// Forward lookup map: text ID -> language -> text content
type ForwardLookup = DashMap<u64, DashMap<String, Arc<String>>>;

/// Reverse lookup map: (language, content hash) -> candidate entries
type ReverseLookup = DashMap<(String, u64), Vec<RevEntry>>;

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
    /// Serializes write operations so WAL append and index updates stay ordered.
    write_lock: Mutex<()>,
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
    /// * `durability` - WAL durability behavior for acknowledged writes
    ///
    /// # Returns
    /// * `Result<Self>` - New Store instance or error
    pub fn new<P: Into<PathBuf>>(wal_path: P, durability: WalDurability) -> Result<Self> {
        let wal = Wal::open(wal_path, durability)?;
        let store = Store {
            forward: DashMap::new(),
            reverse: DashMap::new(),
            write_lock: Mutex::new(()),
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

    /// Finds a reverse-index match by full normalized text inside a hash bucket.
    fn reverse_find_id(&self, lang: &str, hash: u64, normalized: &str) -> Option<u64> {
        let key = (lang.to_string(), hash);
        self.reverse.get(&key).and_then(|entries| {
            entries
                .iter()
                .rev()
                .find(|entry| entry.text_arc.as_str() == normalized)
                .map(|entry| entry.text_id)
        })
    }

    /// Inserts or updates a reverse-index mapping for (lang, hash, normalized_text).
    ///
    /// If the same normalized text already exists in the bucket, it is replaced so
    /// dedup resolution stays deterministic for that content.
    fn reverse_upsert(&self, lang: &str, hash: u64, text_arc: Arc<String>, text_id: u64) {
        let key = (lang.to_string(), hash);
        if let Some(mut entries) = self.reverse.get_mut(&key) {
            if let Some(pos) = entries
                .iter()
                .position(|entry| entry.text_arc.as_str() == text_arc.as_str())
            {
                entries.remove(pos);
            }
            entries.push(RevEntry { text_arc, text_id });
        } else {
            self.reverse
                .insert(key, vec![RevEntry { text_arc, text_id }]);
        }
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
        let _write_guard = self.write_lock.lock();

        match id_opt {
            Some(id) => {
                // Update mode: use specified ID
                let old_text = self
                    .forward
                    .get(&id)
                    .and_then(|lang_map| lang_map.get(lang).map(|entry| entry.value().clone()));
                let existed = old_text.is_some();

                // Persist first. If append fails, no in-memory mutation has happened.
                let record = WalRecord {
                    text_id: id,
                    lang: lang.to_string(),
                    text: normalized,
                };
                self.wal.append(&record)?;

                // Remove stale reverse mapping for the previous value of (id, lang).
                // This prevents future insert-mode dedup from returning an ID that no
                // longer contains the deduplicated text.
                if let Some(old_text) = old_text {
                    let _ = self.reverse_remove_for_id(lang, &old_text, id);
                }

                // Insert/update in forward map
                let lang_map = self.forward.entry(id).or_default();
                lang_map.insert(lang.to_string(), text_arc.clone());

                // Update reverse map
                let hash = Self::rev_hash(&record.text);
                self.reverse_upsert(&record.lang, hash, text_arc, id);

                // Update next_id if necessary
                {
                    let mut next_id = self.next_id.lock();
                    if id >= *next_id {
                        *next_id = id + 1;
                    }
                }

                Ok((id, existed))
            }
            None => {
                // Insert mode: check for existing content
                let hash = Self::rev_hash(&normalized);
                if let Some(existing_id) = self.reverse_find_id(lang, hash, &normalized) {
                    // Content already exists
                    Ok((existing_id, true))
                } else {
                    // Reserve current candidate ID but only commit allocator
                    // advancement after WAL append succeeds.
                    let id = *self.next_id.lock();

                    // Persist first. If append fails, no in-memory mutation has happened.
                    let record = WalRecord {
                        text_id: id,
                        lang: lang.to_string(),
                        text: normalized,
                    };
                    self.wal.append(&record)?;

                    // Commit ID allocation only after successful WAL append.
                    {
                        let mut next_id = self.next_id.lock();
                        if *next_id <= id {
                            *next_id = id + 1;
                        }
                    }

                    // Insert in both maps
                    let lang_map = self.forward.entry(id).or_default();
                    lang_map.insert(record.lang.clone(), text_arc.clone());

                    self.reverse_upsert(lang, hash, text_arc, id);

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
                    // Cross-language search: match against any translation.
                    // Result payload prefers English when available.
                    let first_matching = lang_map.iter().find_map(|entry| {
                        if regex.is_match(entry.value()) {
                            Some(entry.value().clone())
                        } else {
                            None
                        }
                    });

                    if let Some(matching_text) = first_matching {
                        let preferred = lang_map
                            .get("en")
                            .map(|entry| entry.value().clone())
                            .unwrap_or(matching_text);
                        results.push((id, preferred));
                    }
                }
            }
        }

        Ok(results)
    }

    /// Removes an entry from the reverse lookup map.
    ///
    /// Used internally during WAL replay to handle updates correctly.
    fn reverse_remove_for_id(&self, lang: &str, old_text: &str, expected_id: u64) -> bool {
        let hash = Self::rev_hash(old_text);
        let key = (lang.to_string(), hash);
        if let Some(mut entries) = self.reverse.get_mut(&key) {
            if let Some(pos) = entries.iter().position(|entry| {
                entry.text_id == expected_id && entry.text_arc.as_str() == old_text
            }) {
                entries.remove(pos);
                let remove_bucket = entries.is_empty();
                drop(entries);
                if remove_bucket {
                    self.reverse.remove(&key);
                }
                true
            } else {
                false
            }
        } else {
            false
        }
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
                let _ = self.reverse_remove_for_id(&record.lang, &old_text, record.text_id);
            }

            // Insert/update forward map
            lang_map.insert(record.lang.clone(), text_arc.clone());

            // Insert/update reverse map
            let hash = Self::rev_hash(&record.text);
            self.reverse_upsert(&record.lang, hash, text_arc, record.text_id);

            Ok(())
        })?;

        // Update next_id
        *self.next_id.lock() = max_id + 1;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{GetRequest, InternRequest, SearchRequest, Store, Svc, TextBank, WalDurability};
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tonic::{Code, Request};

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

    fn make_wal_path() -> std::path::PathBuf {
        let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let wal_path = std::env::temp_dir().join(format!(
            "textbank_store_test_{}_{}.jsonl",
            std::process::id(),
            id
        ));
        let _ = std::fs::remove_file(&wal_path);
        wal_path
    }

    fn make_store_with_durability(durability: WalDurability) -> Store {
        Store::new(make_wal_path(), durability).expect("failed to create test store")
    }

    fn make_store() -> Store {
        make_store_with_durability(WalDurability::Flush)
    }

    fn replace_wal_file_with_read_only_handle(store: &Store) {
        let path = store.wal.path.clone();
        let read_only = File::open(path).expect("failed to open wal as read-only");
        *store.wal.file.lock() = read_only;
    }

    fn restore_wal_append_handle(store: &Store) {
        let path = store.wal.path.clone();
        let append_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("failed to reopen wal in append mode");
        *store.wal.file.lock() = append_file;
    }

    #[test]
    fn wal_durability_default_is_fsync_data() {
        assert_eq!(WalDurability::default(), WalDurability::FsyncData);
    }

    #[test]
    fn wal_durability_parses_supported_values() {
        assert_eq!(
            "none".parse::<WalDurability>().unwrap(),
            WalDurability::None
        );
        assert_eq!(
            "flush".parse::<WalDurability>().unwrap(),
            WalDurability::Flush
        );
        assert_eq!(
            "fsync_data".parse::<WalDurability>().unwrap(),
            WalDurability::FsyncData
        );
        assert_eq!(
            "fsync_all".parse::<WalDurability>().unwrap(),
            WalDurability::FsyncAll
        );
    }

    #[test]
    fn wal_durability_rejects_invalid_values() {
        let result = "eventual".parse::<WalDurability>();
        assert!(result.is_err(), "invalid durability mode must be rejected");
    }

    #[test]
    fn intern_succeeds_with_fsync_durability_modes() {
        for durability in [WalDurability::FsyncData, WalDurability::FsyncAll] {
            let store = make_store_with_durability(durability);
            let (id, existed) = store
                .intern_with_id("en", b"durable-write", None)
                .expect("insert should succeed for fsync modes");
            assert!(!existed);
            let value = store
                .get_text(id, Some("en"))
                .expect("stored value should be retrievable");
            assert_eq!(value.as_str(), "durable-write");
        }
    }

    #[test]
    fn insert_mode_is_idempotent_for_same_language_and_text() {
        let store = make_store();

        let (id1, existed1) = store
            .intern_with_id("en", b"hello", None)
            .expect("first insert should succeed");
        let (id2, existed2) = store
            .intern_with_id("en", b"hello", None)
            .expect("second insert should succeed");

        assert_eq!(id1, id2, "same (lang,text) should dedup to same id");
        assert!(!existed1, "first insert should report new content");
        assert!(existed2, "second insert should report existing content");
    }

    #[test]
    fn update_removes_stale_reverse_mapping_for_previous_value() {
        let store = make_store();

        let (id, existed) = store
            .intern_with_id("en", b"A", None)
            .expect("insert A should succeed");
        assert!(!existed);

        let (updated_id, updated_existed) = store
            .intern_with_id("en", b"B", Some(id))
            .expect("update to B should succeed");
        assert_eq!(updated_id, id);
        assert!(
            updated_existed,
            "updating existing lang on id should set existed=true"
        );

        let (new_id_for_a, existed_again) = store
            .intern_with_id("en", b"A", None)
            .expect("reinserting old content should succeed");

        assert!(
            !existed_again,
            "old content should not dedup to an id that no longer contains it"
        );
        assert_ne!(
            new_id_for_a, id,
            "reinserting old content must allocate a new id after update"
        );
    }

    #[test]
    fn latest_update_wins_for_insert_mode_dedup_mapping() {
        let store = make_store();

        let (id_a, _) = store
            .intern_with_id("en", b"A", None)
            .expect("insert A should succeed");
        let (id_b, _) = store
            .intern_with_id("en", b"B", None)
            .expect("insert B should succeed");
        assert_ne!(id_a, id_b);

        let (returned_id, existed) = store
            .intern_with_id("en", b"A", Some(id_b))
            .expect("update id_b to A should succeed");
        assert_eq!(returned_id, id_b);
        assert!(existed);

        let (dedup_id, dedup_existed) = store
            .intern_with_id("en", b"A", None)
            .expect("insert-mode dedup should succeed");
        assert!(dedup_existed);
        assert_eq!(
            dedup_id, id_b,
            "insert-mode dedup should deterministically resolve to the latest mapping"
        );
    }

    #[test]
    fn insert_does_not_mutate_in_memory_when_wal_append_fails() {
        let store = make_store();
        replace_wal_file_with_read_only_handle(&store);

        let err = store.intern_with_id("en", b"should-fail", None);
        assert!(err.is_err(), "insert must fail when WAL append fails");

        assert!(
            store.forward.is_empty(),
            "forward index must remain unchanged"
        );
        assert!(
            store.reverse.is_empty(),
            "reverse index must remain unchanged"
        );
        assert_eq!(
            *store.next_id.lock(),
            1,
            "id allocator must not advance on failed writes"
        );
    }

    #[test]
    fn update_does_not_mutate_in_memory_when_wal_append_fails() {
        let store = make_store();

        let (id, _) = store
            .intern_with_id("en", b"original", None)
            .expect("seed insert should succeed");

        replace_wal_file_with_read_only_handle(&store);

        let err = store.intern_with_id("en", b"new-value", Some(id));
        assert!(err.is_err(), "update must fail when WAL append fails");

        let current = store
            .get_text(id, Some("en"))
            .expect("existing value must still be present");
        assert_eq!(current.as_str(), "original", "failed update must not apply");

        restore_wal_append_handle(&store);

        let (dedup_id, existed) = store
            .intern_with_id("en", b"original", None)
            .expect("dedup after failed update should succeed");
        assert!(existed);
        assert_eq!(dedup_id, id, "dedup mapping must remain unchanged");
    }

    #[test]
    fn wal_replay_ignores_truncated_tail_record() {
        let wal_path = make_wal_path();
        let mut file = File::create(&wal_path).expect("failed to create wal file");

        writeln!(file, r#"{{"text_id":1,"lang":"en","text":"ok"}}"#)
            .expect("failed to write valid wal record");
        write!(file, r#"{{"text_id":2,"lang":"en","text":"partial"#)
            .expect("failed to write truncated wal tail");
        drop(file);

        let store = Store::new(&wal_path, WalDurability::Flush)
            .expect("store should load with truncated wal tail");
        let text = store
            .get_text(1, Some("en"))
            .expect("valid records before tail must be replayed");

        assert_eq!(text.as_str(), "ok");
        assert!(
            store.get_text(2, Some("en")).is_none(),
            "truncated tail record must not be applied"
        );
        assert_eq!(
            *store.next_id.lock(),
            2,
            "next_id should be derived from last valid record"
        );
    }

    #[test]
    fn wal_replay_rejects_corrupted_non_tail_record() {
        let wal_path = make_wal_path();
        let mut file = File::create(&wal_path).expect("failed to create wal file");

        writeln!(file, r#"{{"text_id":1,"lang":"en","text":"ok"}}"#)
            .expect("failed to write first wal record");
        writeln!(file, r#"{{"text_id":2"#).expect("failed to write corrupted wal record");
        writeln!(file, r#"{{"text_id":3,"lang":"en","text":"later"}}"#)
            .expect("failed to write trailing wal record");
        drop(file);

        let result = Store::new(&wal_path, WalDurability::Flush);
        assert!(
            result.is_err(),
            "corrupted non-tail record should fail startup to avoid silent data loss"
        );
    }

    #[test]
    fn hash_bucket_candidates_are_matched_by_full_text() {
        let store = make_store();
        let forced_hash = 42_u64;

        store.reverse_upsert("en", forced_hash, Arc::new("alpha".to_string()), 10);
        store.reverse_upsert("en", forced_hash, Arc::new("beta".to_string()), 20);

        assert_eq!(
            store.reverse_find_id("en", forced_hash, "alpha"),
            Some(10),
            "alpha should resolve to its own id in a shared hash bucket"
        );
        assert_eq!(
            store.reverse_find_id("en", forced_hash, "beta"),
            Some(20),
            "beta should resolve to its own id in a shared hash bucket"
        );
        assert_eq!(
            store.reverse_find_id("en", forced_hash, "gamma"),
            None,
            "non-matching text in same hash bucket must not dedup"
        );
    }

    #[test]
    fn insert_mode_ignores_hash_bucket_entries_with_different_text() {
        let store = make_store();
        let hash = Store::rev_hash("alpha");

        // Simulate a collision bucket already containing a different normalized text.
        store.reverse_upsert("en", hash, Arc::new("beta".to_string()), 999);

        let (id, existed) = store
            .intern_with_id("en", b"alpha", None)
            .expect("insert should succeed");

        assert!(
            !existed,
            "different text in same hash bucket must not dedup"
        );
        assert_ne!(id, 999, "must not return id from non-matching candidate");
    }

    #[test]
    fn dedup_is_scoped_to_language() {
        let store = make_store();

        let (id_en, existed_en) = store
            .intern_with_id("en", b"hello", None)
            .expect("english insert should succeed");
        let (id_fr, existed_fr) = store
            .intern_with_id("fr", b"hello", None)
            .expect("french insert should succeed");

        assert!(!existed_en);
        assert!(!existed_fr);
        assert_ne!(
            id_en, id_fr,
            "same text in different languages should not dedup to same id"
        );
    }

    #[test]
    fn wal_replay_preserves_latest_update_and_reverse_index_consistency() {
        let wal_path = make_wal_path();
        let store =
            Store::new(&wal_path, WalDurability::Flush).expect("failed to create initial store");

        let (id, _) = store
            .intern_with_id("en", b"first", None)
            .expect("first insert should succeed");
        store
            .intern_with_id("en", b"second", Some(id))
            .expect("update should succeed");
        drop(store);

        let reloaded =
            Store::new(&wal_path, WalDurability::Flush).expect("reloading from wal should succeed");

        let current = reloaded
            .get_text(id, Some("en"))
            .expect("updated value should be present after replay");
        assert_eq!(current.as_str(), "second");

        let (old_id, old_existed) = reloaded
            .intern_with_id("en", b"first", None)
            .expect("reinserting old content should succeed");
        assert!(
            !old_existed,
            "old content should not dedup to id after replaying a later update"
        );
        assert_ne!(
            old_id, id,
            "replay must remove stale reverse mapping for superseded value"
        );
    }

    #[test]
    fn invalid_utf8_is_lossy_but_deterministic_for_dedup() {
        let store = make_store();
        let invalid = [b'f', b'o', 0x80, b'o'];

        let (id1, existed1) = store
            .intern_with_id("en", &invalid, None)
            .expect("invalid utf8 insert should succeed");
        let (id2, existed2) = store
            .intern_with_id("en", &invalid, None)
            .expect("same invalid utf8 should dedup");

        assert!(!existed1);
        assert!(existed2);
        assert_eq!(id1, id2, "lossy normalization should be stable for dedup");

        let stored = store
            .get_text(id1, Some("en"))
            .expect("stored text should be retrievable");
        assert_eq!(stored.as_str(), "fo\u{fffd}o");

        let (id3, existed3) = store
            .intern_with_id("en", "fo\u{fffd}o".as_bytes(), None)
            .expect("literal replacement-char form should dedup to same content");
        assert!(existed3);
        assert_eq!(id3, id1);
    }

    #[test]
    fn explicit_update_id_advances_allocator_for_future_inserts() {
        let store = make_store();

        let explicit_id = 42;
        let (id, existed) = store
            .intern_with_id("en", b"seed", Some(explicit_id))
            .expect("explicit-id update should succeed");
        assert_eq!(id, explicit_id);
        assert!(
            !existed,
            "new language slot on explicit id should report existed=false"
        );

        let (next_id, next_existed) = store
            .intern_with_id("en", b"fresh", None)
            .expect("next insert should succeed");
        assert!(!next_existed);
        assert_eq!(
            next_id,
            explicit_id + 1,
            "allocator should continue after highest explicit id"
        );
    }

    #[test]
    fn search_without_language_matches_any_translation_and_prefers_english_payload() {
        let store = make_store();

        let (id, _) = store
            .intern_with_id("en", b"hello world", None)
            .expect("english insert should succeed");
        store
            .intern_with_id("fr", b"bonjour le monde", Some(id))
            .expect("french insert on same id should succeed");

        let hits = store
            .search("bonjour", None)
            .expect("cross-language search should succeed");

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, id);
        assert_eq!(
            hits[0].1.as_str(),
            "hello world",
            "cross-language results should prefer english payload when available"
        );
    }

    #[test]
    fn search_without_language_returns_matching_payload_when_english_missing() {
        let store = make_store();

        let (id, _) = store
            .intern_with_id("fr", b"bonjour le monde", None)
            .expect("french insert should succeed");

        let hits = store
            .search("bonjour", None)
            .expect("cross-language search should succeed");

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, id);
        assert_eq!(
            hits[0].1.as_str(),
            "bonjour le monde",
            "matching payload should be returned when english translation is absent"
        );
    }

    #[tokio::test]
    async fn grpc_intern_rejects_empty_language() {
        let svc = Svc {
            store: Arc::new(make_store()),
        };
        let request = Request::new(InternRequest {
            text: b"hello".to_vec(),
            lang: String::new(),
            text_id: 0,
        });

        let result = svc.intern(request).await;
        let status = result.expect_err("empty lang should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn grpc_search_reports_invalid_regex() {
        let svc = Svc {
            store: Arc::new(make_store()),
        };
        let request = Request::new(SearchRequest {
            pattern: "[".to_string(),
            lang: String::new(),
        });

        let result = svc.search(request).await;
        let status = result.expect_err("invalid regex should be rejected");
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn grpc_get_prefers_english_when_language_unspecified() {
        let store = make_store();
        let (id, _) = store
            .intern_with_id("en", b"hello", None)
            .expect("english insert should succeed");
        store
            .intern_with_id("fr", b"bonjour", Some(id))
            .expect("french update should succeed");

        let svc = Svc {
            store: Arc::new(store),
        };
        let response = svc
            .get(Request::new(GetRequest {
                text_id: id,
                lang: String::new(),
            }))
            .await
            .expect("get without lang should succeed")
            .into_inner();

        assert!(response.found);
        assert_eq!(response.text, b"hello".to_vec());
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
