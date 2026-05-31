// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — adapters/runtime_adapter.rs                                   ║
// ║  Base runtime adapter interface and utilities                            ║
// ║  This module provides common utilities for adapter implementation        ║
// ╚══════════════════════════════════════════════════════════════════════════╝

use crate::contracts::{RuntimeCommand, RuntimeEvent};

/// AdapterRegistry — holds adapter factories for different backends
/// This allows dynamic selection of which adapter to use
pub struct AdapterRegistry {
    adapters: std::collections::HashMap<String, String>,
}

impl AdapterRegistry {
    pub fn new() -> Self {
        AdapterRegistry {
            adapters: std::collections::HashMap::new(),
        }
    }

    pub fn register(&mut self, name: &str, factory_type: &str) {
        self.adapters.insert(name.to_string(), factory_type.to_string());
    }

    pub fn get(&self, name: &str) -> Option<String> {
        self.adapters.get(name).cloned()
    }

    pub fn list_adapters(&self) -> Vec<String> {
        self.adapters.keys().cloned().collect()
    }
}

/// CommandDispatcher — helper for routing commands to appropriate handlers
pub struct CommandDispatcher;

impl CommandDispatcher {
    /// Check if a command needs immediate response vs async processing
    pub fn is_blocking(command: &RuntimeCommand) -> bool {
        matches!(
            command,
            RuntimeCommand::RequestStateSync | RuntimeCommand::RequestStatistics
        )
    }

    /// Get the name of a command
    pub fn name(command: &RuntimeCommand) -> &str {
        command.name()
    }
}

/// EventAggregator — helper for combining multiple events
pub struct EventAggregator {
    events: Vec<RuntimeEvent>,
}

impl EventAggregator {
    pub fn new() -> Self {
        EventAggregator {
            events: Vec::new(),
        }
    }

    pub fn push(&mut self, event: RuntimeEvent) {
        self.events.push(event);
    }

    pub fn drain(self) -> Vec<RuntimeEvent> {
        self.events
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }
}
