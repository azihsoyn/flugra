use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

const HOOKS_FILENAME: &str = "flugra.hooks.yaml";

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct HooksConfig {
    /// Commands to run before applying migrations
    #[serde(default)]
    pub pre_apply: Vec<HookEntry>,

    /// Commands to run after applying migrations
    #[serde(default)]
    pub post_apply: Vec<HookEntry>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HookEntry {
    /// Shell command to execute
    pub command: String,

    /// Human-readable description
    #[serde(default)]
    pub description: Option<String>,

    /// Working directory (defaults to the hooks file's directory)
    #[serde(default)]
    pub workdir: Option<String>,
}

impl HooksConfig {
    /// Load hooks config from root directory. Returns default (empty) if file doesn't exist.
    pub fn load(root: &Path) -> Result<Self> {
        let path = root.join(HOOKS_FILENAME);
        if !path.exists() {
            return Ok(Self::default());
        }

        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read {}", path.display()))?;
        let config: HooksConfig =
            serde_yaml::from_str(&content).with_context(|| format!("Failed to parse {}", path.display()))?;
        Ok(config)
    }

    pub fn has_hooks(&self) -> bool {
        !self.pre_apply.is_empty() || !self.post_apply.is_empty()
    }
}

/// Run a list of hooks, passing DATABASE_URL as an environment variable.
pub fn run_hooks(hooks: &[HookEntry], phase: &str, database_url: &str, default_workdir: &Path) -> Result<()> {
    if hooks.is_empty() {
        return Ok(());
    }

    println!("\nRunning {} hook(s) ({})...\n", hooks.len(), phase);

    for (i, hook) in hooks.iter().enumerate() {
        let desc = hook.description.as_deref().unwrap_or(&hook.command);
        print!("  [{}/{}] {} ... ", i + 1, hooks.len(), desc);

        let workdir = hook
            .workdir
            .as_ref()
            .map(|w| Path::new(w).to_path_buf())
            .unwrap_or_else(|| default_workdir.to_path_buf());

        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(&hook.command)
            .env("DATABASE_URL", database_url)
            .current_dir(&workdir)
            .output()
            .with_context(|| format!("Failed to execute hook: {}", hook.command))?;

        if output.status.success() {
            println!("OK");
        } else {
            println!("FAILED (exit code: {})", output.status.code().unwrap_or(-1));
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stderr.is_empty() {
                // Show last few lines of stderr
                let lines: Vec<&str> = stderr.lines().collect();
                let start = if lines.len() > 5 { lines.len() - 5 } else { 0 };
                for line in &lines[start..] {
                    println!("    {}", line);
                }
            }
            anyhow::bail!("Hook failed: {}", hook.command);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = HooksConfig::load(dir.path()).unwrap();
        assert!(config.pre_apply.is_empty());
        assert!(config.post_apply.is_empty());
        assert!(!config.has_hooks());
    }

    #[test]
    fn test_load_hooks_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("flugra.hooks.yaml"),
            "pre_apply:\n  - command: echo hello\n    description: Test hook\npost_apply: []\n",
        )
        .unwrap();

        let config = HooksConfig::load(dir.path()).unwrap();
        assert_eq!(config.pre_apply.len(), 1);
        assert_eq!(config.pre_apply[0].command, "echo hello");
        assert_eq!(config.pre_apply[0].description.as_deref(), Some("Test hook"));
        assert!(config.has_hooks());
    }

    #[test]
    fn test_run_hook_success() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = vec![HookEntry {
            command: "echo ok".to_string(),
            description: Some("echo test".to_string()),
            workdir: None,
        }];
        run_hooks(&hooks, "pre_apply", "postgres://localhost/test", dir.path()).unwrap();
    }

    #[test]
    fn test_run_hook_failure() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = vec![HookEntry {
            command: "exit 1".to_string(),
            description: None,
            workdir: None,
        }];
        assert!(run_hooks(&hooks, "pre_apply", "postgres://localhost/test", dir.path()).is_err());
    }

    #[test]
    fn test_hook_receives_database_url() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("url.txt");
        let hooks = vec![HookEntry {
            command: format!("echo $DATABASE_URL > {}", marker.display()),
            description: None,
            workdir: None,
        }];
        run_hooks(&hooks, "pre_apply", "postgres://mydb", dir.path()).unwrap();
        let content = std::fs::read_to_string(&marker).unwrap();
        assert!(content.trim().contains("postgres://mydb"));
    }
}
