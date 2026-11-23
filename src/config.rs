//! Per-node-group autoscaling limits, read from a JSON file.
//!
//! ```json
//! {
//!   "default":   { "min": 1, "max": 3 },
//!   "overrides": { "workers": { "min": 2 }, "batch": { "enabled": false } }
//! }
//! ```
//!
//! Every setting is optional. A group takes each setting from its override,
//! then from `default`, then from the built-in default: min 1, max the
//! cluster's per-group cap, enabled.

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

use crate::BoxError;

/// Smallest min a node group can have. UKS doesn't allow empty node groups.
pub(crate) const MIN_NODES_PER_GROUP: i32 = 1;

/// The config file, as written.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NodeGroupsConfig {
    #[serde(default)]
    default: GroupSettings,
    #[serde(default)]
    overrides: HashMap<String, GroupSettings>,
}

#[derive(Debug, Default, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupSettings {
    min: Option<i32>,
    max: Option<i32>,
    enabled: Option<bool>,
}

impl GroupSettings {
    /// Fills each unset setting from `fallback`.
    fn or(self, fallback: GroupSettings) -> GroupSettings {
        GroupSettings {
            min: self.min.or(fallback.min),
            max: self.max.or(fallback.max),
            enabled: self.enabled.or(fallback.enabled),
        }
    }
}

/// A managed node group's size limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Limits {
    pub(crate) min: i32,
    pub(crate) max: i32,
}

/// Checked limits for every node group. `None` means autoscaling is disabled
/// for the group.
#[derive(Debug)]
pub(crate) struct NodeGroupLimits {
    default: Option<Limits>,
    overrides: HashMap<String, Option<Limits>>,
}

impl NodeGroupsConfig {
    pub(crate) fn from_file(path: &Path) -> Result<Self, BoxError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("reading node group config {}: {e}", path.display()))?;
        let config: Self = serde_json::from_str(&text)
            .map_err(|e| format!("node group config {}: {e}", path.display()))?;

        // Runs every check except the cap, which needs the cluster's plan
        // from the API, so most mistakes fail before any API call.
        config.resolve(i32::MAX)?;

        Ok(config)
    }

    /// Fills in unset settings and checks every group's limits. `cap` is the
    /// most nodes one group can have on this cluster.
    pub(crate) fn resolve(&self, cap: i32) -> Result<NodeGroupLimits, BoxError> {
        let default = check("default", self.default, cap)?;

        let mut overrides = HashMap::new();
        for (name, settings) in &self.overrides {
            let limits = check(&format!("overrides.{name}"), settings.or(self.default), cap)?;
            overrides.insert(name.clone(), limits);
        }

        Ok(NodeGroupLimits { default, overrides })
    }
}

/// Applies the built-in defaults to `settings` and checks the result.
fn check(label: &str, settings: GroupSettings, cap: i32) -> Result<Option<Limits>, BoxError> {
    let min = settings.min.unwrap_or(MIN_NODES_PER_GROUP);
    let max = settings.max.unwrap_or(cap);

    if min < MIN_NODES_PER_GROUP {
        return Err(format!(
            "node group config {label}: min {min} is below {MIN_NODES_PER_GROUP}; UKS doesn't allow empty node groups"
        )
        .into());
    }
    if max < min {
        return Err(format!("node group config {label}: max {max} is below min {min}").into());
    }
    if max > cap {
        return Err(format!(
            "node group config {label}: max {max} is above {cap}, the most nodes a group can have on this cluster"
        )
        .into());
    }

    Ok(settings
        .enabled
        .unwrap_or(true)
        .then_some(Limits { min, max }))
}

impl NodeGroupLimits {
    /// The group's limits, or `None` if autoscaling is disabled for it.
    pub(crate) fn for_group(&self, name: &str) -> Option<Limits> {
        self.overrides.get(name).copied().unwrap_or(self.default)
    }

    /// Names of overrides that match none of `group_names`, sorted.
    pub(crate) fn unknown_overrides(&self, group_names: &[&str]) -> Vec<&str> {
        let mut unknown: Vec<&str> = self
            .overrides
            .keys()
            .map(String::as_str)
            .filter(|name| !group_names.contains(name))
            .collect();
        unknown.sort_unstable();
        unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAP: i32 = 16;

    fn resolve(json: &str) -> Result<NodeGroupLimits, BoxError> {
        serde_json::from_str::<NodeGroupsConfig>(json)?.resolve(CAP)
    }

    fn limits(min: i32, max: i32) -> Option<Limits> {
        Some(Limits { min, max })
    }

    #[test]
    fn empty_config_uses_built_in_defaults() {
        let l = resolve("{}").unwrap();
        assert_eq!(l.for_group("any"), limits(1, CAP));
    }

    #[test]
    fn no_config_file_matches_empty_config() {
        let l = NodeGroupsConfig::default().resolve(CAP).unwrap();
        assert_eq!(l.for_group("any"), limits(1, CAP));
    }

    #[test]
    fn default_applies_to_every_group() {
        let l = resolve(r#"{"default": {"min": 2, "max": 3}}"#).unwrap();
        assert_eq!(l.for_group("a"), limits(2, 3));
        assert_eq!(l.for_group("b"), limits(2, 3));
    }

    #[test]
    fn override_takes_unset_settings_from_default() {
        let l = resolve(r#"{"default": {"min": 2, "max": 3}, "overrides": {"big": {"max": 8}}}"#)
            .unwrap();
        assert_eq!(l.for_group("big"), limits(2, 8));
        assert_eq!(l.for_group("other"), limits(2, 3));
    }

    #[test]
    fn override_can_disable_a_group() {
        let l = resolve(r#"{"overrides": {"batch": {"enabled": false}}}"#).unwrap();
        assert_eq!(l.for_group("batch"), None);
        assert_eq!(l.for_group("other"), limits(1, CAP));
    }

    #[test]
    fn default_disabled_makes_overrides_opt_in() {
        let l = resolve(
            r#"{"default": {"enabled": false}, "overrides": {"web": {"enabled": true, "max": 4}}}"#,
        )
        .unwrap();
        assert_eq!(l.for_group("web"), limits(1, 4));
        assert_eq!(l.for_group("other"), None);
    }

    #[test]
    fn null_settings_count_as_unset() {
        let l = resolve(r#"{"default": {"min": 2, "max": null}}"#).unwrap();
        assert_eq!(l.for_group("any"), limits(2, CAP));
    }

    #[test]
    fn rejects_min_below_one() {
        let err = resolve(r#"{"default": {"min": 0}}"#).unwrap_err();
        assert!(
            err.to_string().contains("default: min 0 is below 1"),
            "{err}"
        );
    }

    #[test]
    fn rejects_max_below_min() {
        let err = resolve(r#"{"default": {"min": 3, "max": 2}}"#).unwrap_err();
        assert!(err.to_string().contains("max 2 is below min 3"), "{err}");
    }

    #[test]
    fn rejects_max_above_cap() {
        let err = resolve(r#"{"default": {"max": 17}}"#).unwrap_err();
        assert!(err.to_string().contains("max 17 is above 16"), "{err}");
    }

    #[test]
    fn rejects_invalid_override_by_name() {
        let err = resolve(r#"{"overrides": {"web": {"min": 0}}}"#).unwrap_err();
        assert!(err.to_string().contains("overrides.web: min 0"), "{err}");
    }

    #[test]
    fn rejects_override_that_conflicts_with_default() {
        let err =
            resolve(r#"{"default": {"max": 3}, "overrides": {"web": {"min": 5}}}"#).unwrap_err();
        assert!(
            err.to_string()
                .contains("overrides.web: max 3 is below min 5"),
            "{err}"
        );
    }

    #[test]
    fn checks_disabled_groups_too() {
        assert!(resolve(r#"{"overrides": {"batch": {"enabled": false, "min": 0}}}"#).is_err());
    }

    #[test]
    fn rejects_unknown_fields() {
        let err = resolve(r#"{"default": {"maxx": 3}}"#).unwrap_err();
        assert!(err.to_string().contains("unknown field `maxx`"), "{err}");
        assert!(resolve(r#"{"defaults": {}}"#).is_err());
    }

    #[test]
    fn lists_overrides_that_match_no_group() {
        let l = resolve(r#"{"overrides": {"web": {}, "old": {}, "batch": {}}}"#).unwrap();
        assert_eq!(l.unknown_overrides(&["web"]), vec!["batch", "old"]);
    }
}
