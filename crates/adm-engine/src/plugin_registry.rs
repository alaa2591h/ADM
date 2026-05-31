//! Dynamic plugin registry and management system.
//!
//! Allows loading and managing custom download manager plugins at runtime:
//! - Custom extractors
//! - Custom network clients
//! - Custom file handlers
//! - Custom event processors
//! - Custom UI components

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginManifest {
    pub name: String,
    pub version: String,
    pub author: String,
    pub description: String,
    pub plugin_type: PluginType,
    pub entry_point: String,
    pub dependencies: Vec<String>,
    pub permissions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PluginType {
    Extractor,
    NetworkClient,
    FileHandler,
    EventProcessor,
    UiComponent,
    Custom,
}

impl PluginType {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Extractor => "extractor",
            Self::NetworkClient => "network_client",
            Self::FileHandler => "file_handler",
            Self::EventProcessor => "event_processor",
            Self::UiComponent => "ui_component",
            Self::Custom => "custom",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadedPlugin {
    pub manifest: PluginManifest,
    pub loaded_at: i64,
    pub enabled: bool,
    pub status: PluginStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PluginStatus {
    Active,
    Inactive,
    Error(String),
    Disabled,
}

pub struct PluginRegistry {
    plugins: HashMap<String, LoadedPlugin>,
    plugin_dir: PathBuf,
}

impl PluginRegistry {
    #[must_use]
    pub fn new(plugin_dir: PathBuf) -> Self {
        Self {
            plugins: HashMap::new(),
            plugin_dir,
        }
    }

    #[must_use]
    pub fn plugin_dir(&self) -> &Path {
        &self.plugin_dir
    }

    /// Load plugin from manifest file.
    pub async fn load_plugin(&mut self, manifest_path: &Path) -> Result<()> {
        let manifest_content = tokio::fs::read_to_string(manifest_path)
            .await
            .context("Failed to read manifest file")?;

        let manifest: PluginManifest = serde_json::from_str(&manifest_content)?;

        // Validate manifest
        self.validate_manifest(&manifest)?;

        let plugin = LoadedPlugin {
            manifest: manifest.clone(),
            loaded_at: chrono::Utc::now().timestamp(),
            enabled: true,
            status: PluginStatus::Active,
        };

        self.plugins.insert(manifest.name.clone(), plugin);

        tracing::info!(
            plugin_name = %manifest.name,
            plugin_version = %manifest.version,
            "Plugin loaded successfully"
        );

        Ok(())
    }

    /// Unload a plugin by name.
    pub fn unload_plugin(&mut self, name: &str) -> Result<()> {
        if self.plugins.remove(name).is_none() {
            return Err(anyhow::anyhow!("Plugin not found: {name}"));
        }

        tracing::info!(plugin_name = name, "Plugin unloaded");
        Ok(())
    }

    /// Enable a plugin.
    pub fn enable_plugin(&mut self, name: &str) -> Result<()> {
        if let Some(plugin) = self.plugins.get_mut(name) {
            plugin.enabled = true;
            plugin.status = PluginStatus::Active;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Plugin not found: {name}"))
        }
    }

    /// Disable a plugin.
    pub fn disable_plugin(&mut self, name: &str) -> Result<()> {
        if let Some(plugin) = self.plugins.get_mut(name) {
            plugin.enabled = false;
            plugin.status = PluginStatus::Disabled;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Plugin not found: {name}"))
        }
    }

    /// Get all loaded plugins.
    #[must_use]
    pub fn list_plugins(&self) -> Vec<LoadedPlugin> {
        self.plugins.values().cloned().collect()
    }

    /// Get active plugins of a specific type.
    #[must_use]
    pub fn get_plugins_by_type(&self, plugin_type: PluginType) -> Vec<LoadedPlugin> {
        self.plugins
            .values()
            .filter(|p| p.enabled && p.manifest.plugin_type == plugin_type)
            .cloned()
            .collect()
    }

    /// Get plugin by name.
    #[must_use]
    pub fn get_plugin(&self, name: &str) -> Option<LoadedPlugin> {
        self.plugins.get(name).cloned()
    }

    fn validate_manifest(&self, manifest: &PluginManifest) -> Result<()> {
        if manifest.name.is_empty() {
            return Err(anyhow::anyhow!("Plugin name cannot be empty"));
        }

        if manifest.version.is_empty() {
            return Err(anyhow::anyhow!("Plugin version cannot be empty"));
        }

        if manifest.entry_point.is_empty() {
            return Err(anyhow::anyhow!("Plugin entry_point cannot be empty"));
        }

        // Validate semantic version
        if !self.is_valid_version(&manifest.version) {
            return Err(anyhow::anyhow!(
                "Invalid semantic version: {}",
                manifest.version
            ));
        }

        Ok(())
    }

    fn is_valid_version(&self, version: &str) -> bool {
        let parts: Vec<&str> = version.split('.').collect();
        parts.len() == 3 && parts.iter().all(|p| p.parse::<u32>().is_ok())
    }
}

/// Load all plugins from plugin directory.
pub async fn load_all_plugins(registry: &mut PluginRegistry, plugin_dir: &Path) -> Result<usize> {
    let mut loaded_count = 0;

    if !plugin_dir.exists() {
        tracing::warn!(plugin_dir = ?plugin_dir, "Plugin directory does not exist");
        return Ok(0);
    }

    let mut entries = tokio::fs::read_dir(plugin_dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();

        if path.is_dir() {
            let manifest_path = path.join("manifest.json");
            if manifest_path.exists() {
                match registry.load_plugin(&manifest_path).await {
                    Ok(()) => loaded_count += 1,
                    Err(e) => {
                        tracing::warn!(path = ?manifest_path, error = %e, "Failed to load plugin");
                    }
                }
            }
        }
    }

    Ok(loaded_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_type_strings() {
        assert_eq!(PluginType::Extractor.as_str(), "extractor");
        assert_eq!(PluginType::NetworkClient.as_str(), "network_client");
    }

    #[test]
    fn test_version_validation() {
        let registry = PluginRegistry::new(PathBuf::from("/tmp"));

        assert!(registry.is_valid_version("1.0.0"));
        assert!(registry.is_valid_version("2.1.5"));
        assert!(!registry.is_valid_version("1.0"));
        assert!(!registry.is_valid_version("1.0.0.0"));
    }

    #[test]
    fn test_plugin_registry_operations() {
        let mut registry = PluginRegistry::new(PathBuf::from("/tmp"));

        let manifest = PluginManifest {
            name: "test-plugin".to_string(),
            version: "1.0.0".to_string(),
            author: "Test Author".to_string(),
            description: "Test plugin".to_string(),
            plugin_type: PluginType::Extractor,
            entry_point: "lib.rs".to_string(),
            dependencies: vec![],
            permissions: vec!["network".to_string()],
        };

        let plugin = LoadedPlugin {
            manifest,
            loaded_at: 0,
            enabled: true,
            status: PluginStatus::Active,
        };

        registry.plugins.insert("test-plugin".to_string(), plugin);

        assert_eq!(registry.list_plugins().len(), 1);
        assert!(registry.get_plugin("test-plugin").is_some());
    }

    #[test]
    fn test_plugin_enable_disable() {
        let mut registry = PluginRegistry::new(PathBuf::from("/tmp"));

        let manifest = PluginManifest {
            name: "test".to_string(),
            version: "1.0.0".to_string(),
            author: "Test".to_string(),
            description: "Test".to_string(),
            plugin_type: PluginType::Custom,
            entry_point: "lib.rs".to_string(),
            dependencies: vec![],
            permissions: vec![],
        };

        let plugin = LoadedPlugin {
            manifest,
            loaded_at: 0,
            enabled: true,
            status: PluginStatus::Active,
        };

        registry.plugins.insert("test".to_string(), plugin);

        registry.disable_plugin("test").unwrap();
        assert!(!registry.get_plugin("test").unwrap().enabled);

        registry.enable_plugin("test").unwrap();
        assert!(registry.get_plugin("test").unwrap().enabled);
    }
}
