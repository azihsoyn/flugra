use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::migration_parser;

/// An execution unit: either a leaf directory or a single SQL file.
#[derive(Debug, Clone)]
pub struct Unit {
    /// Relative path from root (e.g. "users/add-email" or "0001_create_users.sql")
    pub id: String,
    /// Absolute path to the unit directory (or parent directory for file-per-unit)
    pub path: PathBuf,
    /// SQL files in filename order
    pub sql_files: Vec<PathBuf>,
}

/// Discovery mode determines how units are detected.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DiscoveryMode {
    /// Original mode: leaf directories = units
    Directory,
    /// Flat mode: each SQL file = one unit (for existing migration projects)
    FilePerUnit,
    /// Auto-detect: use FilePerUnit if root is a flat dir with SQL files
    Auto,
}

impl Unit {
    /// Read and concatenate all SQL content in order.
    /// If `extract_up` is true, extract only the "Up" section from migration files.
    pub fn read_sql_with_options(&self, extract_up: bool) -> Result<String> {
        let mut content = String::new();
        for file in &self.sql_files {
            let raw = std::fs::read_to_string(file)
                .with_context(|| format!("Failed to read {}", file.display()))?;
            if extract_up {
                content.push_str(&migration_parser::extract_up_section(&raw));
            } else {
                content.push_str(&raw);
            }
            content.push('\n');
        }
        Ok(content)
    }

    /// Read and concatenate all SQL content in order (no Up extraction).
    pub fn read_sql(&self) -> Result<String> {
        self.read_sql_with_options(false)
    }

    /// Compute a SHA-256 checksum of SQL content.
    pub fn checksum_with_options(&self, extract_up: bool) -> Result<String> {
        use sha2::{Digest, Sha256};
        let content = self.read_sql_with_options(extract_up)?;
        let hash = Sha256::digest(content.as_bytes());
        Ok(hex::encode(hash))
    }

    /// Compute a SHA-256 checksum of all SQL content.
    pub fn checksum(&self) -> Result<String> {
        self.checksum_with_options(false)
    }
}

/// Discover all execution units under `root`.
pub fn discover(root: &Path) -> Result<BTreeMap<String, Unit>> {
    discover_with_mode(root, DiscoveryMode::Auto)
}

/// Discover units with a specific mode.
pub fn discover_with_mode(root: &Path, mode: DiscoveryMode) -> Result<BTreeMap<String, Unit>> {
    let root = root
        .canonicalize()
        .with_context(|| format!("Cannot resolve root path: {}", root.display()))?;

    match mode {
        DiscoveryMode::FilePerUnit => discover_file_per_unit(&root),
        DiscoveryMode::Directory => discover_directory_mode(&root),
        DiscoveryMode::Auto => {
            // Auto-detect: if root directly contains SQL files and no subdirs with SQL,
            // use file-per-unit mode
            if is_flat_sql_dir(&root)? {
                discover_file_per_unit(&root)
            } else {
                discover_directory_mode(&root)
            }
        }
    }
}

/// Check if a directory is a flat directory with SQL files (no subdirs containing SQL).
fn is_flat_sql_dir(dir: &Path) -> Result<bool> {
    let mut has_sql = false;
    let mut has_subdir_sql = false;

    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("Cannot read directory: {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().map_or(false, |ext| ext == "sql") {
            has_sql = true;
        } else if path.is_dir() {
            if dir_contains_sql_recursive(&path)? {
                has_subdir_sql = true;
            }
        }
    }

    Ok(has_sql && !has_subdir_sql)
}

fn dir_contains_sql_recursive(dir: &Path) -> Result<bool> {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|_| std::fs::read_dir("/dev/null").unwrap()) {
        if let Ok(entry) = entry {
            let path = entry.path();
            if path.is_file() && path.extension().map_or(false, |ext| ext == "sql") {
                return Ok(true);
            }
            if path.is_dir() && dir_contains_sql_recursive(&path)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// File-per-unit discovery: each .sql file in the root becomes its own unit.
fn discover_file_per_unit(root: &Path) -> Result<BTreeMap<String, Unit>> {
    let mut units = BTreeMap::new();
    let mut sql_files: Vec<PathBuf> = Vec::new();

    for entry in std::fs::read_dir(root)
        .with_context(|| format!("Cannot read directory: {}", root.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().map_or(false, |ext| ext == "sql") {
            sql_files.push(path);
        }
    }

    sql_files.sort();

    for file in sql_files {
        let filename = file
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        units.insert(
            filename.clone(),
            Unit {
                id: filename,
                path: root.to_path_buf(),
                sql_files: vec![file],
            },
        );
    }

    Ok(units)
}

/// Original directory-based discovery.
fn discover_directory_mode(root: &Path) -> Result<BTreeMap<String, Unit>> {
    let mut units = BTreeMap::new();
    discover_recursive(root, root, &mut units)?;
    Ok(units)
}

fn discover_recursive(
    root: &Path,
    dir: &Path,
    units: &mut BTreeMap<String, Unit>,
) -> Result<bool> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .with_context(|| format!("Cannot read directory: {}", dir.display()))?
        .filter_map(|e| e.ok())
        .collect();
    entries.sort_by_key(|e| e.file_name());

    let mut sql_files: Vec<PathBuf> = Vec::new();
    let mut subdirs: Vec<PathBuf> = Vec::new();

    for entry in &entries {
        let path = entry.path();
        if path.is_dir() {
            subdirs.push(path);
        } else if path.extension().map_or(false, |ext| ext == "sql") {
            sql_files.push(path);
        }
    }

    // Recurse into subdirectories
    let mut child_has_sql = false;
    for subdir in &subdirs {
        if discover_recursive(root, subdir, units)? {
            child_has_sql = true;
        }
    }

    // This directory is a unit if it has SQL files and no child has SQL
    let has_sql = !sql_files.is_empty();
    if has_sql && !child_has_sql {
        sql_files.sort();
        let id = dir
            .strip_prefix(root)
            .unwrap_or(dir)
            .to_string_lossy()
            .replace('\\', "/");

        let id = if id.is_empty() {
            ".".to_string()
        } else {
            id
        };

        units.insert(
            id.clone(),
            Unit {
                id,
                path: dir.to_path_buf(),
                sql_files,
            },
        );
    }

    Ok(has_sql || child_has_sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn setup_test_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();

        // users/add-email/ with two SQL files
        let unit1 = dir.path().join("users/add-email");
        fs::create_dir_all(&unit1).unwrap();
        fs::write(unit1.join("001_add_column.sql"), "ALTER TABLE users ADD COLUMN email TEXT;").unwrap();
        fs::write(unit1.join("002_backfill.sql"), "UPDATE users SET email = '';").unwrap();

        // orders/create/
        let unit2 = dir.path().join("orders/create");
        fs::create_dir_all(&unit2).unwrap();
        fs::write(unit2.join("001_create.sql"), "CREATE TABLE orders (id SERIAL PRIMARY KEY);").unwrap();

        dir
    }

    #[test]
    fn test_discover_finds_leaf_units() {
        let dir = setup_test_dir();
        let units = discover(dir.path()).unwrap();

        assert_eq!(units.len(), 2);
        assert!(units.contains_key("users/add-email"));
        assert!(units.contains_key("orders/create"));
    }

    #[test]
    fn test_sql_files_sorted() {
        let dir = setup_test_dir();
        let units = discover(dir.path()).unwrap();
        let unit = &units["users/add-email"];

        assert_eq!(unit.sql_files.len(), 2);
        assert!(unit.sql_files[0].ends_with("001_add_column.sql"));
        assert!(unit.sql_files[1].ends_with("002_backfill.sql"));
    }

    #[test]
    fn test_non_leaf_excluded() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("migrations");
        fs::create_dir_all(&parent).unwrap();
        fs::write(parent.join("setup.sql"), "SELECT 1;").unwrap();

        let child = parent.join("v1");
        fs::create_dir_all(&child).unwrap();
        fs::write(child.join("001.sql"), "CREATE TABLE t1 (id INT);").unwrap();

        let units = discover(dir.path()).unwrap();
        assert_eq!(units.len(), 1);
        assert!(units.contains_key("migrations/v1"));
    }

    #[test]
    fn test_checksum_deterministic() {
        let dir = setup_test_dir();
        let units = discover(dir.path()).unwrap();
        let unit = &units["users/add-email"];

        let c1 = unit.checksum().unwrap();
        let c2 = unit.checksum().unwrap();
        assert_eq!(c1, c2);
    }

    #[test]
    fn test_flat_dir_auto_detects_file_per_unit() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("001_create_users.sql"), "CREATE TABLE users (id SERIAL);").unwrap();
        fs::write(dir.path().join("002_create_orders.sql"), "CREATE TABLE orders (id SERIAL);").unwrap();

        let units = discover(dir.path()).unwrap();
        assert_eq!(units.len(), 2);
        assert!(units.contains_key("001_create_users.sql"));
        assert!(units.contains_key("002_create_orders.sql"));
    }

    #[test]
    fn test_flat_dir_preserves_order() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("003_c.sql"), "SELECT 3;").unwrap();
        fs::write(dir.path().join("001_a.sql"), "SELECT 1;").unwrap();
        fs::write(dir.path().join("002_b.sql"), "SELECT 2;").unwrap();

        let units = discover(dir.path()).unwrap();
        let keys: Vec<_> = units.keys().collect();
        assert_eq!(keys, vec!["001_a.sql", "002_b.sql", "003_c.sql"]);
    }

    #[test]
    fn test_file_per_unit_with_up_section() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("001.sql"),
            "-- +migrate Up\nCREATE TABLE t1 (id INT);\n-- +migrate Down\nDROP TABLE t1;\n",
        ).unwrap();

        let units = discover(dir.path()).unwrap();
        let unit = &units["001.sql"];
        let sql = unit.read_sql_with_options(true).unwrap();
        assert!(sql.contains("CREATE TABLE"));
        assert!(!sql.contains("DROP TABLE"));
    }
}
