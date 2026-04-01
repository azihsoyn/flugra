use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

use crate::discovery::Unit;
use crate::planner::UnitDependency;

const LOCK_VERSION: u32 = 1;
const LOCK_FILENAME: &str = "flugra.lock";

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct LockFile {
    pub version: u32,
    pub units: BTreeMap<String, LockUnit>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct LockUnit {
    pub checksum: String,
    pub depends_on: Vec<String>,
}

impl LockFile {
    /// Build a lock file from discovered units and resolved dependencies.
    pub fn from_units(
        units: &BTreeMap<String, Unit>,
        deps: &BTreeMap<String, UnitDependency>,
    ) -> Result<Self> {
        Self::from_units_with_options(units, deps, false)
    }

    /// Build a lock file, optionally extracting Up sections for checksums.
    pub fn from_units_with_options(
        units: &BTreeMap<String, Unit>,
        deps: &BTreeMap<String, UnitDependency>,
        extract_up: bool,
    ) -> Result<Self> {
        let mut lock_units = BTreeMap::new();

        for (id, unit) in units {
            let checksum = unit.checksum_with_options(extract_up)?;
            let depends_on = deps
                .get(id)
                .map(|d| d.depends_on_units.clone())
                .unwrap_or_default();

            lock_units.insert(
                id.clone(),
                LockUnit {
                    checksum,
                    depends_on,
                },
            );
        }

        Ok(LockFile {
            version: LOCK_VERSION,
            units: lock_units,
        })
    }

    /// Write lock file to the given root directory.
    pub fn write(&self, root: &Path) -> Result<()> {
        let path = root.join(LOCK_FILENAME);
        let content = serde_yaml::to_string(self).context("Failed to serialize lock file")?;
        std::fs::write(&path, content)
            .with_context(|| format!("Failed to write {}", path.display()))?;
        Ok(())
    }

    /// Read lock file from the given root directory.
    pub fn read(root: &Path) -> Result<Self> {
        let path = root.join(LOCK_FILENAME);
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read {}. Run 'flugra lock' first.", path.display()))?;
        let lock: LockFile =
            serde_yaml::from_str(&content).context("Failed to parse lock file")?;

        if lock.version != LOCK_VERSION {
            bail!(
                "Unsupported lock file version: {} (expected {})",
                lock.version,
                LOCK_VERSION
            );
        }

        Ok(lock)
    }

    /// Validate that the lock file matches the current filesystem state.
    pub fn validate(&self, units: &BTreeMap<String, Unit>) -> Result<()> {
        self.validate_with_options(units, false)
    }

    /// Validate with optional Up section extraction.
    pub fn validate_with_options(&self, units: &BTreeMap<String, Unit>, extract_up: bool) -> Result<()> {
        // Check all lock units exist on filesystem
        for id in self.units.keys() {
            if !units.contains_key(id) {
                bail!("Unit '{}' in lock file not found on filesystem", id);
            }
        }

        // Check all filesystem units are in lock file
        for id in units.keys() {
            if !self.units.contains_key(id) {
                bail!(
                    "Unit '{}' on filesystem not in lock file. Run 'flugra lock' to update.",
                    id
                );
            }
        }

        // Check checksums match
        for (id, lock_unit) in &self.units {
            let unit = &units[id];
            let current_checksum = unit.checksum_with_options(extract_up)?;
            if current_checksum != lock_unit.checksum {
                bail!(
                    "Checksum mismatch for unit '{}'. Expected {}, got {}. Run 'flugra lock' to update.",
                    id,
                    lock_unit.checksum,
                    current_checksum
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery;
    use crate::planner;
    use std::fs;

    #[test]
    fn test_lock_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("migrations");
        fs::create_dir_all(root.join("users/create")).unwrap();
        fs::write(
            root.join("users/create/001.sql"),
            "CREATE TABLE users (id SERIAL);",
        )
        .unwrap();

        let units = discovery::discover(&root).unwrap();
        let deps = planner::resolve_dependencies(&units).unwrap();
        let lock = LockFile::from_units(&units, &deps).unwrap();

        lock.write(dir.path()).unwrap();
        let loaded = LockFile::read(dir.path()).unwrap();

        assert_eq!(lock, loaded);
    }

    #[test]
    fn test_validate_checksum_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("migrations");
        fs::create_dir_all(root.join("users/create")).unwrap();
        fs::write(
            root.join("users/create/001.sql"),
            "CREATE TABLE users (id SERIAL);",
        )
        .unwrap();

        let units = discovery::discover(&root).unwrap();
        let deps = planner::resolve_dependencies(&units).unwrap();
        let lock = LockFile::from_units(&units, &deps).unwrap();
        lock.write(dir.path()).unwrap();

        // Modify SQL
        fs::write(
            root.join("users/create/001.sql"),
            "CREATE TABLE users (id SERIAL, name TEXT);",
        )
        .unwrap();

        let units2 = discovery::discover(&root).unwrap();
        let loaded = LockFile::read(dir.path()).unwrap();
        assert!(loaded.validate(&units2).is_err());
    }

    #[test]
    fn test_validate_missing_unit() {
        let dir = tempfile::tempdir().unwrap();
        let lock = LockFile {
            version: 1,
            units: {
                let mut m = BTreeMap::new();
                m.insert(
                    "nonexistent".to_string(),
                    LockUnit {
                        checksum: "abc".to_string(),
                        depends_on: vec![],
                    },
                );
                m
            },
        };

        let units = BTreeMap::new();
        assert!(lock.validate(&units).is_err());
    }
}
