/// Extract the "Up" section from a migration file.
///
/// Supports two formats:
/// 1. `-- +migrate Up` / `-- +migrate Down`
/// 2. `-- Up Migration` / `-- Down Migration`
///
/// If no markers are found, returns the entire content.
pub fn extract_up_section(content: &str) -> String {
    // Try `-- +migrate Up/Down` format
    if let Some(result) = extract_between_markers(
        content,
        &["-- +migrate Up", "-- +migrate up"],
        &["-- +migrate Down", "-- +migrate down"],
    ) {
        return result;
    }

    // Try `-- Up Migration` / `-- Down Migration` format
    if let Some(result) = extract_between_markers(
        content,
        &["-- Up Migration", "-- up migration"],
        &["-- Down Migration", "-- down migration"],
    ) {
        return result;
    }

    // Try with dashes
    if let Some(result) = extract_section_with_headers(content) {
        return result;
    }

    // No markers found, return entire content
    content.to_string()
}

/// Extract content between up marker and down marker.
fn extract_between_markers(content: &str, up_markers: &[&str], down_markers: &[&str]) -> Option<String> {
    let lower = content.to_lowercase();

    // Find the up marker position
    let up_pos = up_markers.iter().filter_map(|m| {
        lower.find(&m.to_lowercase())
    }).min()?;

    // Find the line end after the up marker
    let start = content[up_pos..].find('\n').map(|p| up_pos + p + 1).unwrap_or(content.len());

    // Find the down marker position
    let end = down_markers.iter().filter_map(|m| {
        lower[start..].find(&m.to_lowercase()).map(|p| start + p)
    }).min().unwrap_or(content.len());

    Some(content[start..end].trim_end().to_string())
}

/// Handle format with separator lines:
/// ```text
/// -- ----------------------------------------------------------------------
/// -- Up Migration
/// -- ----------------------------------------------------------------------
/// ... SQL ...
/// -- ----------------------------------------------------------------------
/// -- Down Migration
/// -- ----------------------------------------------------------------------
/// ```
fn extract_section_with_headers(content: &str) -> Option<String> {
    let lines: Vec<&str> = content.lines().collect();

    let mut up_start = None;
    let mut down_start = None;

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim().to_lowercase();
        if trimmed.contains("up migration") && !trimmed.starts_with("--") || trimmed == "-- up migration" {
            // Skip any following separator lines
            up_start = Some(skip_separator_lines(&lines, i + 1));
        }
        if (trimmed.contains("down migration") && !trimmed.starts_with("--")) || trimmed == "-- down migration" {
            // Go back to include separator lines before "Down Migration"
            down_start = Some(find_section_boundary(&lines, i));
        }
    }

    if let Some(start) = up_start {
        let end = down_start.unwrap_or(lines.len());
        let section: Vec<&str> = lines[start..end].to_vec();
        Some(section.join("\n").trim_end().to_string())
    } else {
        None
    }
}

fn skip_separator_lines(lines: &[&str], from: usize) -> usize {
    let mut i = from;
    while i < lines.len() {
        let trimmed = lines[i].trim();
        if trimmed.starts_with("-- ---") || trimmed.is_empty() {
            i += 1;
        } else {
            break;
        }
    }
    i
}

fn find_section_boundary(lines: &[&str], header_line: usize) -> usize {
    // Look backwards from header_line to find separator lines
    let mut i = header_line;
    while i > 0 {
        let trimmed = lines[i - 1].trim();
        if trimmed.starts_with("-- ---") || trimmed.is_empty() {
            i -= 1;
        } else {
            break;
        }
    }
    i
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_migrate_format() {
        let content = "-- +migrate Up\nCREATE TABLE users (id INT);\n\n-- +migrate Down\nDROP TABLE users;\n";
        let up = extract_up_section(content);
        assert_eq!(up, "CREATE TABLE users (id INT);");
    }

    #[test]
    fn test_node_pg_migrate_format() {
        let content = "\
-- ----------------------------------------------------------------------
-- Up Migration
-- ----------------------------------------------------------------------

SET LOCAL app.admin = 'true';
CREATE TABLE users (id INT);

-- ----------------------------------------------------------------------
-- Down Migration
-- ----------------------------------------------------------------------

DROP TABLE users;";
        let up = extract_up_section(content);
        assert!(up.contains("CREATE TABLE users"));
        assert!(up.contains("SET LOCAL"));
        assert!(!up.contains("DROP TABLE"));
    }

    #[test]
    fn test_no_markers_returns_full_content() {
        let content = "CREATE TABLE users (id INT);\n";
        let up = extract_up_section(content);
        assert_eq!(up, content);
    }

    #[test]
    fn test_up_only_no_down() {
        let content = "-- +migrate Up\nCREATE TABLE users (id INT);\n";
        let up = extract_up_section(content);
        assert_eq!(up, "CREATE TABLE users (id INT);");
    }

    #[test]
    fn test_complex_up_section() {
        let content = "\
-- +migrate Up
CREATE TABLE IF NOT EXISTS import_state
(
    table_name CHARACTER VARYING(100) NOT NULL,
    CONSTRAINT import_state_pkey PRIMARY KEY (table_name)
);

CREATE TABLE IF NOT EXISTS orders
(
    id SERIAL PRIMARY KEY
);

-- +migrate Down
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS import_state;
";
        let up = extract_up_section(content);
        assert!(up.contains("CREATE TABLE IF NOT EXISTS import_state"));
        assert!(up.contains("CREATE TABLE IF NOT EXISTS orders"));
        assert!(!up.contains("DROP TABLE"));
    }
}
