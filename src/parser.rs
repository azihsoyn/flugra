use std::collections::BTreeSet;

/// Result of analyzing SQL content for table-level dependencies.
#[derive(Debug, Clone, Default)]
pub struct SqlAnalysis {
    /// Tables created by this SQL (e.g., CREATE TABLE)
    pub creates: BTreeSet<String>,
    /// Tables referenced by this SQL (e.g., INSERT INTO, ALTER TABLE, SELECT FROM, etc.)
    pub references: BTreeSet<String>,
}

impl SqlAnalysis {
    /// Tables that this unit depends on: references minus creates.
    /// (If a unit creates and references the same table, it doesn't depend on itself.)
    pub fn dependencies(&self) -> BTreeSet<String> {
        self.references.difference(&self.creates).cloned().collect()
    }
}

/// Lightweight SQL analysis using heuristics.
///
/// Extracts table names from common DDL/DML patterns:
/// - CREATE TABLE name → creates
/// - ALTER TABLE name → references
/// - INSERT INTO name → references
/// - UPDATE name → references
/// - DELETE FROM name → references
/// - FROM name (in SELECT) → references
/// - JOIN name → references
/// - DROP TABLE name → references
pub fn analyze(sql: &str) -> SqlAnalysis {
    let mut result = SqlAnalysis::default();
    let upper = sql.to_uppercase();
    let tokens = tokenize(&upper);

    let mut i = 0;
    while i < tokens.len() {
        match tokens[i].as_str() {
            "CREATE" => {
                // CREATE TABLE [IF NOT EXISTS] name
                if let Some(name) = match_create_table(&tokens, i) {
                    result.creates.insert(normalize_table_name(&sql_tokens(sql)[find_token_index(&tokens, i, &name)]));
                }
            }
            "ALTER" | "DROP" => {
                // ALTER TABLE name / DROP TABLE name
                if i + 2 < tokens.len() && tokens[i + 1] == "TABLE" {
                    let mut j = i + 2;
                    if j + 2 < tokens.len() && tokens[j] == "IF" {
                        j += 3; // skip IF [NOT] EXISTS
                    }
                    if j < tokens.len() && is_identifier(&tokens[j]) {
                        result.references.insert(normalize_table_name(&sql_tokens(sql)[j]));
                    }
                }
            }
            "INSERT" => {
                // INSERT INTO name
                if i + 2 < tokens.len() && tokens[i + 1] == "INTO" && is_identifier(&tokens[i + 2]) {
                    result.references.insert(normalize_table_name(&sql_tokens(sql)[i + 2]));
                }
            }
            "UPDATE" => {
                if i + 1 < tokens.len() && is_identifier(&tokens[i + 1]) {
                    result.references.insert(normalize_table_name(&sql_tokens(sql)[i + 1]));
                }
            }
            "DELETE" => {
                // DELETE FROM name
                if i + 2 < tokens.len() && tokens[i + 1] == "FROM" && is_identifier(&tokens[i + 2]) {
                    result.references.insert(normalize_table_name(&sql_tokens(sql)[i + 2]));
                }
            }
            "FROM" | "JOIN" => {
                if i + 1 < tokens.len() && is_identifier(&tokens[i + 1]) {
                    result.references.insert(normalize_table_name(&sql_tokens(sql)[i + 1]));
                }
            }
            "REFERENCES" => {
                // REFERENCES name (foreign key)
                if i + 1 < tokens.len() && is_identifier(&tokens[i + 1]) {
                    result.references.insert(normalize_table_name(&sql_tokens(sql)[i + 1]));
                }
            }
            _ => {}
        }
        i += 1;
    }

    result
}

fn match_create_table(tokens: &[String], i: usize) -> Option<String> {
    if i + 2 >= tokens.len() || tokens[i + 1] != "TABLE" {
        return None;
    }
    let mut j = i + 2;
    // Skip IF NOT EXISTS
    if j + 2 < tokens.len() && tokens[j] == "IF" && tokens[j + 1] == "NOT" && tokens[j + 2] == "EXISTS" {
        j += 3;
    }
    if j < tokens.len() && is_identifier(&tokens[j]) {
        Some(tokens[j].clone())
    } else {
        None
    }
}

fn find_token_index(tokens: &[String], start: usize, target: &str) -> usize {
    for i in start..tokens.len() {
        if tokens[i] == target {
            return i;
        }
    }
    start
}

/// Tokenize SQL into whitespace-separated tokens (uppercase).
fn tokenize(sql: &str) -> Vec<String> {
    // Remove single-line comments
    let no_comments: String = sql
        .lines()
        .map(|line| {
            if let Some(pos) = line.find("--") {
                &line[..pos]
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join(" ");

    no_comments
        .replace('(', " ( ")
        .replace(')', " ) ")
        .replace(',', " , ")
        .replace(';', " ; ")
        .split_whitespace()
        .map(|s| s.to_string())
        .collect()
}

/// Tokenize preserving original case.
fn sql_tokens(sql: &str) -> Vec<String> {
    let no_comments: String = sql
        .lines()
        .map(|line| {
            if let Some(pos) = line.find("--") {
                &line[..pos]
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join(" ");

    no_comments
        .replace('(', " ( ")
        .replace(')', " ) ")
        .replace(',', " , ")
        .replace(';', " ; ")
        .split_whitespace()
        .map(|s| s.to_string())
        .collect()
}

fn is_identifier(token: &str) -> bool {
    let reserved = [
        "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "ON", "SET",
        "VALUES", "INTO", "TABLE", "INDEX", "IF", "EXISTS", "PRIMARY", "KEY",
        "FOREIGN", "REFERENCES", "CASCADE", "NULL", "DEFAULT", "CONSTRAINT",
        "UNIQUE", "CHECK", "AS", "WITH", "RETURNING", "ORDER", "BY", "GROUP",
        "HAVING", "LIMIT", "OFFSET", "UNION", "ALL", "DISTINCT", "JOIN",
        "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "NATURAL", "USING",
        "CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "DELETE", "TRUNCATE",
        "(", ")", ",", ";",
    ];
    !token.is_empty() && !reserved.contains(&token.to_uppercase().as_str())
}

fn normalize_table_name(name: &str) -> String {
    name.trim_matches('"').to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table() {
        let sql = "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);";
        let analysis = analyze(sql);
        assert!(analysis.creates.contains("users"));
        assert!(analysis.dependencies().is_empty());
    }

    #[test]
    fn test_insert_into() {
        let sql = "INSERT INTO users (name) VALUES ('alice');";
        let analysis = analyze(sql);
        assert!(analysis.references.contains("users"));
        assert!(analysis.dependencies().contains("users"));
    }

    #[test]
    fn test_alter_table() {
        let sql = "ALTER TABLE users ADD COLUMN email TEXT;";
        let analysis = analyze(sql);
        assert!(analysis.references.contains("users"));
    }

    #[test]
    fn test_create_and_insert_same_table() {
        let sql = "CREATE TABLE users (id SERIAL);\nINSERT INTO users (id) VALUES (1);";
        let analysis = analyze(sql);
        assert!(analysis.creates.contains("users"));
        assert!(analysis.references.contains("users"));
        // No external dependency since we create the table ourselves
        assert!(analysis.dependencies().is_empty());
    }

    #[test]
    fn test_foreign_key_reference() {
        let sql = "CREATE TABLE orders (id SERIAL, user_id INT REFERENCES users);";
        let analysis = analyze(sql);
        assert!(analysis.creates.contains("orders"));
        assert!(analysis.references.contains("users"));
        assert!(analysis.dependencies().contains("users"));
    }

    #[test]
    fn test_join() {
        let sql = "SELECT * FROM orders JOIN users ON orders.user_id = users.id;";
        let analysis = analyze(sql);
        assert!(analysis.references.contains("orders"));
        assert!(analysis.references.contains("users"));
    }

    #[test]
    fn test_create_table_if_not_exists() {
        let sql = "CREATE TABLE IF NOT EXISTS users (id SERIAL);";
        let analysis = analyze(sql);
        assert!(analysis.creates.contains("users"));
    }

    #[test]
    fn test_comments_ignored() {
        let sql = "-- This creates a table\nCREATE TABLE users (id SERIAL);";
        let analysis = analyze(sql);
        assert!(analysis.creates.contains("users"));
    }
}
