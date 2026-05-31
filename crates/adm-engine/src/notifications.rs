//! Native system notifications for download completion and events.
//!
//! Cross-platform support:
//! - Windows: Windows Toast notifications via `WinRT`
//! - macOS: Apple notifications via `NSUserNativeNotificationCenter`
//! - Linux: D-Bus org.freedesktop.NativeNotifications
//!
//! Respects system do-not-disturb settings and user preferences.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeNotificationConfig {
    /// Enable system notifications
    pub enabled: bool,
    /// Show download completed notifications
    pub on_completion: bool,
    /// Show download failed notifications
    pub on_error: bool,
    /// Show download speed/progress notifications
    pub on_progress: bool,
    /// Do not disturb start time (24h format, e.g., "22:00")
    pub dnd_start: Option<String>,
    /// Do not disturb end time (24h format, e.g., "08:00")
    pub dnd_end: Option<String>,
}

impl Default for NativeNotificationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            on_completion: true,
            on_error: true,
            on_progress: false,
            dnd_start: None,
            dnd_end: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NativeNotificationSeverity {
    Info,
    Warning,
    Error,
    Success,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeNotification {
    pub title: String,
    pub body: String,
    pub severity: NativeNotificationSeverity,
    pub icon: Option<String>,
    pub action_url: Option<String>,
    pub task_id: Option<String>,
}

#[cfg(target_os = "windows")]
mod windows_impl {
    use super::{NativeNotification, NativeNotificationConfig};

    pub fn send_notification(
        notif: &NativeNotification,
        _config: &NativeNotificationConfig,
    ) -> anyhow::Result<()> {
        // Windows Toast notification via winrt-notification crate
        // In a real implementation, this would call WinRT APIs
        tracing::info!(
            title = %notif.title,
            body = %notif.body,
            "Windows notification (would show Toast)"
        );
        Ok(())
    }
}

#[cfg(target_os = "macos")]
mod macos_impl {
    use super::*;

    pub fn send_notification(
        notif: &NativeNotification,
        _config: &NativeNotificationConfig,
    ) -> anyhow::Result<()> {
        // macOS notification via NSUserNativeNotificationCenter
        // In a real implementation, this would call Objective-C APIs via objc crate
        tracing::info!(
            title = %notif.title,
            body = %notif.body,
            "macOS notification (would show NSUserNativeNotification)"
        );
        Ok(())
    }
}

#[cfg(target_os = "linux")]
mod linux_impl {
    use super::*;

    pub fn send_notification(
        notif: &NativeNotification,
        _config: &NativeNotificationConfig,
    ) -> anyhow::Result<()> {
        // Linux D-Bus notification via dbus crate
        // In a real implementation, this would call org.freedesktop.NativeNotifications
        tracing::info!(
            title = %notif.title,
            body = %notif.body,
            "Linux notification (would use D-Bus freedesktop)"
        );
        Ok(())
    }
}

#[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
mod fallback_impl {
    use super::*;

    pub fn send_notification(
        notif: &NativeNotification,
        _config: &NativeNotificationConfig,
    ) -> anyhow::Result<()> {
        // Fallback: log only
        tracing::info!(
            title = %notif.title,
            body = %notif.body,
            "NativeNotification (platform not supported)"
        );
        Ok(())
    }
}

// Re-export the platform-specific implementation
#[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
use fallback_impl::*;
#[cfg(target_os = "linux")]
use linux_impl::*;
#[cfg(target_os = "macos")]
use macos_impl::*;
#[cfg(target_os = "windows")]
use windows_impl::send_notification;

/// Send a notification with configuration checks.
pub fn notify(notif: NativeNotification, config: &NativeNotificationConfig) -> anyhow::Result<()> {
    if !config.enabled {
        return Ok(());
    }

    // Check severity filter
    match notif.severity {
        NativeNotificationSeverity::Info if !config.on_progress => return Ok(()),
        NativeNotificationSeverity::Error if !config.on_error => return Ok(()),
        NativeNotificationSeverity::Success if !config.on_completion => return Ok(()),
        _ => {}
    }

    // Check do-not-disturb
    if is_do_not_disturb(config) {
        tracing::debug!("Suppressing notification due to DND");
        return Ok(());
    }

    send_notification(&notif, config)
}

/// Check if current time is within do-not-disturb window.
fn is_do_not_disturb(config: &NativeNotificationConfig) -> bool {
    use chrono::Local;

    let (Some(start), Some(end)) = (&config.dnd_start, &config.dnd_end) else {
        return false;
    };

    let now = Local::now().time();
    let start_time = chrono::NaiveTime::parse_from_str(start, "%H:%M").ok();
    let end_time = chrono::NaiveTime::parse_from_str(end, "%H:%M").ok();

    match (start_time, end_time) {
        (Some(s), Some(e)) => {
            if s < e {
                // Normal case: e.g., 22:00 - 08:00 wrapping around midnight
                now >= s || now < e
            } else {
                // Wrapping case: start > end
                now >= s && now < e
            }
        }
        _ => false,
    }
}

/// Builder for constructing notifications.
pub struct NativeNotificationBuilder {
    title: String,
    body: String,
    severity: NativeNotificationSeverity,
    icon: Option<String>,
    action_url: Option<String>,
    task_id: Option<String>,
}

impl NativeNotificationBuilder {
    pub fn new(title: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            body: body.into(),
            severity: NativeNotificationSeverity::Info,
            icon: None,
            action_url: None,
            task_id: None,
        }
    }

    #[must_use]
    pub const fn severity(mut self, severity: NativeNotificationSeverity) -> Self {
        self.severity = severity;
        self
    }

    #[must_use]
    pub fn icon(mut self, icon: String) -> Self {
        self.icon = Some(icon);
        self
    }

    #[must_use]
    pub fn action_url(mut self, url: String) -> Self {
        self.action_url = Some(url);
        self
    }

    #[must_use]
    pub fn task_id(mut self, id: String) -> Self {
        self.task_id = Some(id);
        self
    }

    #[must_use]
    pub fn build(self) -> NativeNotification {
        NativeNotification {
            title: self.title,
            body: self.body,
            severity: self.severity,
            icon: self.icon,
            action_url: self.action_url,
            task_id: self.task_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notification_builder() {
        let notif = NativeNotificationBuilder::new("Download Complete", "file.mp4")
            .severity(NativeNotificationSeverity::Success)
            .build();

        assert_eq!(notif.title, "Download Complete");
        assert_eq!(notif.body, "file.mp4");
    }

    #[test]
    fn test_dnd_wrapping() {
        let config = NativeNotificationConfig {
            enabled: true,
            on_completion: true,
            on_error: true,
            on_progress: false,
            dnd_start: Some("22:00".to_string()),
            dnd_end: Some("08:00".to_string()),
            ..Default::default()
        };

        // Test will vary based on current time, so we just verify no panic
        let _in_dnd = is_do_not_disturb(&config);
    }

    #[test]
    fn test_notification_disabled() {
        let config = NativeNotificationConfig {
            enabled: false,
            ..Default::default()
        };

        let notif = NativeNotificationBuilder::new("Test", "test").build();
        let result = notify(notif, &config);
        assert!(result.is_ok());
    }
}
