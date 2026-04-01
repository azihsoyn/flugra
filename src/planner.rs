use anyhow::{bail, Result};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::discovery::Unit;
use crate::parser;

/// Dependency information for a unit.
#[derive(Debug, Clone)]
pub struct UnitDependency {
    pub unit_id: String,
    /// Tables this unit creates
    pub creates: BTreeSet<String>,
    /// Tables this unit depends on (external)
    pub depends_on_tables: BTreeSet<String>,
    /// Unit IDs this unit depends on
    pub depends_on_units: Vec<String>,
}

/// Build a dependency graph from discovered units.
pub fn resolve_dependencies(units: &BTreeMap<String, Unit>) -> Result<BTreeMap<String, UnitDependency>> {
    resolve_dependencies_with_options(units, false)
}

/// Build a dependency graph, optionally extracting Up sections.
pub fn resolve_dependencies_with_options(units: &BTreeMap<String, Unit>, extract_up: bool) -> Result<BTreeMap<String, UnitDependency>> {
    // First pass: analyze each unit's SQL
    let mut analyses: BTreeMap<String, parser::SqlAnalysis> = BTreeMap::new();
    for (id, unit) in units {
        let sql = unit.read_sql_with_options(extract_up)?;
        analyses.insert(id.clone(), parser::analyze(&sql));
    }

    // Build table→unit creator map
    // For sequential migrations (file-per-unit), later files may re-create
    // tables (DROP + CREATE pattern), so we allow overwriting and use the
    // latest creator.
    let mut table_creators: HashMap<String, String> = HashMap::new();
    for (id, analysis) in &analyses {
        for table in &analysis.creates {
            table_creators.insert(table.clone(), id.clone());
        }
    }

    // Second pass: resolve table deps to unit deps
    let mut result: BTreeMap<String, UnitDependency> = BTreeMap::new();
    for (id, analysis) in &analyses {
        let deps = analysis.dependencies();
        let mut depends_on_units: BTreeSet<String> = BTreeSet::new();

        for table in &deps {
            if let Some(creator) = table_creators.get(table) {
                if creator != id {
                    depends_on_units.insert(creator.clone());
                }
            }
            // If no creator found, it's an external table (pre-existing) — no dependency tracked
        }

        result.insert(
            id.clone(),
            UnitDependency {
                unit_id: id.clone(),
                creates: analysis.creates.clone(),
                depends_on_tables: deps,
                depends_on_units: depends_on_units.into_iter().collect(),
            },
        );
    }

    Ok(result)
}

/// Compute a deterministic execution order via topological sort.
///
/// Ties are broken by lexical order of unit IDs.
pub fn execution_order(deps: &BTreeMap<String, UnitDependency>) -> Result<Vec<String>> {
    let mut in_degree: BTreeMap<String, usize> = BTreeMap::new();
    let mut dependents: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for (id, dep) in deps {
        in_degree.entry(id.clone()).or_insert(0);
        for d in &dep.depends_on_units {
            dependents
                .entry(d.clone())
                .or_default()
                .insert(id.clone());
            *in_degree.entry(id.clone()).or_insert(0) += 1;
        }
    }

    let mut order = Vec::new();
    let mut ready: BTreeSet<String> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(id, _)| id.clone())
        .collect();

    while let Some(id) = ready.iter().next().cloned() {
        ready.remove(&id);
        order.push(id.clone());

        if let Some(children) = dependents.get(&id) {
            for child in children {
                if let Some(deg) = in_degree.get_mut(child) {
                    *deg -= 1;
                    if *deg == 0 {
                        ready.insert(child.clone());
                    }
                }
            }
        }
    }

    if order.len() != deps.len() {
        // Find cycle
        let remaining: Vec<_> = in_degree
            .iter()
            .filter(|(_, &deg)| deg > 0)
            .map(|(id, _)| id.as_str())
            .collect();
        bail!("Circular dependency detected among: {}", remaining.join(", "));
    }

    Ok(order)
}

/// Validate that the dependency graph has no cycles.
pub fn validate_no_cycles(deps: &BTreeMap<String, UnitDependency>) -> Result<()> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut stack: HashSet<String> = HashSet::new();

    for id in deps.keys() {
        if !visited.contains(id) {
            detect_cycle(id, deps, &mut visited, &mut stack)?;
        }
    }
    Ok(())
}

fn detect_cycle(
    id: &str,
    deps: &BTreeMap<String, UnitDependency>,
    visited: &mut HashSet<String>,
    stack: &mut HashSet<String>,
) -> Result<()> {
    visited.insert(id.to_string());
    stack.insert(id.to_string());

    if let Some(dep) = deps.get(id) {
        for next in &dep.depends_on_units {
            if !visited.contains(next.as_str()) {
                detect_cycle(next, deps, visited, stack)?;
            } else if stack.contains(next.as_str()) {
                bail!("Circular dependency detected: {} -> {}", id, next);
            }
        }
    }

    stack.remove(id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn make_unit(dir: &std::path::Path, id: &str, sql: &str) -> Unit {
        let unit_path = dir.join(id);
        fs::create_dir_all(&unit_path).unwrap();
        fs::write(unit_path.join("001.sql"), sql).unwrap();
        Unit {
            id: id.to_string(),
            path: unit_path.clone(),
            sql_files: vec![unit_path.join("001.sql")],
        }
    }

    #[test]
    fn test_dependency_resolution() {
        let dir = tempfile::tempdir().unwrap();
        let mut units = BTreeMap::new();

        let u1 = make_unit(dir.path(), "users/create", "CREATE TABLE users (id SERIAL);");
        let u2 = make_unit(dir.path(), "orders/create", "CREATE TABLE orders (id SERIAL, user_id INT REFERENCES users);");

        units.insert(u1.id.clone(), u1);
        units.insert(u2.id.clone(), u2);

        let deps = resolve_dependencies(&units).unwrap();
        assert!(deps["orders/create"].depends_on_units.contains(&"users/create".to_string()));
        assert!(deps["users/create"].depends_on_units.is_empty());
    }

    #[test]
    fn test_execution_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut units = BTreeMap::new();

        let u1 = make_unit(dir.path(), "a/users", "CREATE TABLE users (id SERIAL);");
        let u2 = make_unit(dir.path(), "b/orders", "CREATE TABLE orders (user_id INT REFERENCES users);");
        let u3 = make_unit(dir.path(), "c/items", "CREATE TABLE items (order_id INT REFERENCES orders);");

        units.insert(u1.id.clone(), u1);
        units.insert(u2.id.clone(), u2);
        units.insert(u3.id.clone(), u3);

        let deps = resolve_dependencies(&units).unwrap();
        let order = execution_order(&deps).unwrap();

        let pos_users = order.iter().position(|x| x == "a/users").unwrap();
        let pos_orders = order.iter().position(|x| x == "b/orders").unwrap();
        let pos_items = order.iter().position(|x| x == "c/items").unwrap();

        assert!(pos_users < pos_orders);
        assert!(pos_orders < pos_items);
    }

    #[test]
    fn test_lexical_fallback_for_independent_units() {
        let dir = tempfile::tempdir().unwrap();
        let mut units = BTreeMap::new();

        let u1 = make_unit(dir.path(), "b/beta", "CREATE TABLE beta (id SERIAL);");
        let u2 = make_unit(dir.path(), "a/alpha", "CREATE TABLE alpha (id SERIAL);");

        units.insert(u1.id.clone(), u1);
        units.insert(u2.id.clone(), u2);

        let deps = resolve_dependencies(&units).unwrap();
        let order = execution_order(&deps).unwrap();

        // Lexical order: a/alpha before b/beta
        assert_eq!(order[0], "a/alpha");
        assert_eq!(order[1], "b/beta");
    }

    #[test]
    fn test_circular_dependency_detected() {
        // Manually construct circular deps
        let mut deps = BTreeMap::new();
        deps.insert(
            "a".to_string(),
            UnitDependency {
                unit_id: "a".to_string(),
                creates: BTreeSet::new(),
                depends_on_tables: BTreeSet::new(),
                depends_on_units: vec!["b".to_string()],
            },
        );
        deps.insert(
            "b".to_string(),
            UnitDependency {
                unit_id: "b".to_string(),
                creates: BTreeSet::new(),
                depends_on_tables: BTreeSet::new(),
                depends_on_units: vec!["a".to_string()],
            },
        );

        assert!(validate_no_cycles(&deps).is_err());
        assert!(execution_order(&deps).is_err());
    }
}
