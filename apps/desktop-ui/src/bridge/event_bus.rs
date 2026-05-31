// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — bridge/event_bus.rs                                           ║
// ║  Single-threaded event queue between UI callbacks and the sim loop.      ║
// ╚══════════════════════════════════════════════════════════════════════════╝

use std::collections::VecDeque;
use crate::contracts::RuntimeCommand;

/// A single-threaded FIFO queue of RuntimeCommand values.
/// UI callbacks push here; the runtime timer drains and processes.
pub struct EventBus {
    queue: VecDeque<RuntimeCommand>,
}

impl EventBus {
    pub fn new() -> Self {
        EventBus { queue: VecDeque::with_capacity(32) }
    }

    /// Enqueue a command.
    pub fn push(&mut self, command: RuntimeCommand) {
        self.queue.push_back(command);
    }

    /// Drain all pending commands in FIFO order.
    pub fn drain(&mut self) -> Vec<RuntimeCommand> {
        self.queue.drain(..).collect()
    }

    /// True when no commands are pending.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}
