use serde::{Deserialize, Serialize};

/// Strongly typed state machine representing the lifecycle of a download task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DownloadState {
    Queued,
    Preparing,
    Downloading,
    Paused,
    Processing,
    Completed,
    Failed,
    Cancelled,
}

impl DownloadState {
    /// Checks if a transition from `self` to `target` is valid.
    pub fn can_transition_to(&self, target: &Self) -> bool {
        match (self, target) {
            // Self-transitions are always allowed
            (a, b) if a == b => true,

            // Queued can start preparing
            (Self::Queued, Self::Preparing) => true,
            // Queued can be paused or cancelled
            (Self::Queued, Self::Paused) => true,
            (Self::Queued, Self::Cancelled) => true,

            // Preparing can start downloading, pause, or fail
            (Self::Preparing, Self::Downloading) => true,
            (Self::Preparing, Self::Paused) => true,
            (Self::Preparing, Self::Failed) => true,
            (Self::Preparing, Self::Cancelled) => true,

            // Downloading can be paused, process (e.g. file joins/muxing), fail, or complete
            (Self::Downloading, Self::Paused) => true,
            (Self::Downloading, Self::Processing) => true,
            (Self::Downloading, Self::Completed) => true,
            (Self::Downloading, Self::Failed) => true,
            (Self::Downloading, Self::Cancelled) => true,

            // Paused can resume (go back to Queued or Downloading) or be cancelled
            (Self::Paused, Self::Queued) => true,
            (Self::Paused, Self::Downloading) => true,
            (Self::Paused, Self::Cancelled) => true,

            // Processing can complete or fail
            (Self::Processing, Self::Completed) => true,
            (Self::Processing, Self::Failed) => true,

            // Terminal states (Completed, Failed, Cancelled) cannot transition
            // EXCEPT Failed/Cancelled can transition back to Queued if retried
            (Self::Failed, Self::Queued) => true,
            (Self::Cancelled, Self::Queued) => true,

            _ => false,
        }
    }
}
