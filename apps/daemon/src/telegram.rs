use std::sync::Arc;
use teloxide::{prelude::*, types::Message as TgMessage, utils::command::BotCommands};
use uuid::Uuid;

use crate::command::{Command as AppCommand, CommandHandler};
use crate::query::{Query as AppQuery, QueryHandler, QueryResult};
use adm_engine::EventBus;
use settings_schema::TelegramSettings;

/// Command definitions for the APEX Telegram Bot controller.
#[derive(BotCommands, Clone)]
#[command(
    rename_rule = "lowercase",
    description = "The following commands are supported:"
)]
pub enum BotCommand {
    #[command(description = "Display welcome message and bot commands.")]
    Start,
    #[command(description = "Show help instructions.")]
    Help,
    #[command(description = "Inspect the general status of the download manager.")]
    Status,
    #[command(description = "List all recent and active tasks.")]
    List,
    #[command(description = "Add a new download URL: /download <url> [filename].")]
    Download { args: String },
    #[command(description = "Pause an active task: /pause <task_id>.")]
    Pause { task_id: String },
    #[command(description = "Resume a paused task: /resume <task_id>.")]
    Resume { task_id: String },
    #[command(description = "Retry a failed task: /retry <task_id>.")]
    Retry { task_id: String },
    #[command(description = "Cancel/delete a task: /cancel <task_id>.")]
    Cancel { task_id: String },
    #[command(description = "List downloaded files in the storage folder.")]
    Files,
    #[command(description = "Show system performance and uptime stats.")]
    Stats,
    #[command(description = "Show logs for a specific task: /logs <task_id>.")]
    Logs { task_id: String },
    #[command(description = "Inspect storage disk usage and download directory path.")]
    Storage,
}

/// Start the polling Telegram bot controller and event notifier.
pub async fn start_telegram_integration(
    settings: TelegramSettings,
    command_handler: Arc<CommandHandler>,
    query_handler: Arc<QueryHandler>,
    event_bus: EventBus,
    download_dir: std::path::PathBuf,
) -> anyhow::Result<()> {
    if !settings.enabled || settings.bot_token.is_empty() {
        tracing::info!("Telegram integration is disabled or token is missing.");
        return Ok(());
    }

    let bot = Bot::new(settings.bot_token.clone());
    let chat_id = settings.chat_id.clone();

    tracing::info!("Starting Telegram Bot Remote Controller...");

    // 1. Spawning event notification worker
    let notifier_bot = bot.clone();
    let notifier_chat = chat_id.clone();
    let notifier_settings = settings;
    let mut event_rx = event_bus.subscribe();

    tokio::spawn(async move {
        let chat = if let Ok(id) = notifier_chat.parse::<i64>() {
            ChatId(id)
        } else {
            tracing::error!("Invalid Telegram Chat ID: {}", notifier_chat);
            return;
        };

        while let Ok(event) = event_rx.recv().await {
            // Mapping serialized/broadcast events into Telegram notifications
            let topic = event.topic.as_str();
            let data = event.data;

            match topic {
                "download.created" if notifier_settings.notify_on_start => {
                    let filename = data
                        .get("filename")
                        .and_then(|f| f.as_str())
                        .unwrap_or("Unknown");
                    let url = data.get("url").and_then(|f| f.as_str()).unwrap_or("");
                    let id = data.get("id").and_then(|f| f.as_str()).unwrap_or("");
                    let msg = format!(
                        "📥 *Download Created*\n*File:* `{filename}`\n*ID:* `{id}`\n*URL:* `{url}`"
                    );
                    let _ = notifier_bot
                        .send_message(chat, msg)
                        .parse_mode(teloxide::types::ParseMode::MarkdownV2)
                        .await;
                }
                "download.completed" if notifier_settings.notify_on_complete => {
                    let id = data.get("task_id").and_then(|f| f.as_str()).unwrap_or("");
                    let bytes = data
                        .get("total_bytes")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0);
                    let mb = bytes as f64 / 1024.0 / 1024.0;
                    let msg = format!("✅ *Download Completed*\n*ID:* `{id}`\n*Size:* `{mb:.2} MB`\nLocal file saved successfully.");
                    let _ = notifier_bot
                        .send_message(chat, msg)
                        .parse_mode(teloxide::types::ParseMode::MarkdownV2)
                        .await;
                }
                "download.failed" if notifier_settings.notify_on_fail => {
                    let id = data.get("task_id").and_then(|f| f.as_str()).unwrap_or("");
                    let err = data
                        .get("error")
                        .and_then(|f| f.as_str())
                        .unwrap_or("unknown error");
                    let msg = format!("❌ *Download Failed*\n*ID:* `{id}`\n*Error:* `{err}`");
                    let _ = notifier_bot
                        .send_message(chat, msg)
                        .parse_mode(teloxide::types::ParseMode::MarkdownV2)
                        .await;
                }
                _ => {}
            }
        }
    });

    // 2. Setting up command processing pipeline
    let handler = Update::filter_message().endpoint(
        move |bot: Bot, msg: TgMessage, cmd: BotCommand| {
            let cmd_handler = command_handler.clone();
            let q_handler = query_handler.clone();
            let dl_dir = download_dir.clone();
            let allowed_chat_id = chat_id.clone();
            async move {
                // Secure Bot: restrict access to the allowed chat ID only
                if msg.chat.id.to_string() != allowed_chat_id {
                    let _ = bot.send_message(msg.chat.id, "Unauthorized Access Denied.").await;
                    return Ok::<(), teloxide::RequestError>(());
                }

                match cmd {
                    BotCommand::Start => {
                        let text = "Welcome to *APEX Download Manager Controller*!\nUse /help to see all available commands.";
                        let _ = bot.send_message(msg.chat.id, text).parse_mode(teloxide::types::ParseMode::MarkdownV2).await;
                    }
                    BotCommand::Help => {
                        let text = BotCommand::descriptions().to_string();
                        let _ = bot.send_message(msg.chat.id, text).await;
                    }
                    BotCommand::Status => {
                        match q_handler.handle(AppQuery::ListTasks { state_filter: Some(vec!["running".into()]), limit: None, offset: None }).await {
                            Ok(QueryResult::TaskList { tasks, total }) => {
                                let mut text = format!("*Active Tasks: {total}*\n\n");
                                for t in tasks {
                                    let filename = t.filename.unwrap_or_else(|| "Unknown".into());
                                    let progress = t.progress_percent.unwrap_or(0.0);
                                    text.push_str(&format!("• `{filename}`: {progress:.1}%\n  ID: `{}`\n", t.id));
                                }
                                let _ = bot.send_message(msg.chat.id, text).parse_mode(teloxide::types::ParseMode::MarkdownV2).await;
                            }
                            _ => { let _ = bot.send_message(msg.chat.id, "Error loading status.").await; }
                        }
                    }
                    BotCommand::List => {
                        match q_handler.handle(AppQuery::ListTasks { state_filter: None, limit: Some(10), offset: None }).await {
                            Ok(QueryResult::TaskList { tasks, .. }) => {
                                if tasks.is_empty() {
                                    let _ = bot.send_message(msg.chat.id, "No download tasks found.").await;
                                    return Ok::<(), teloxide::RequestError>(());
                                }
                                let mut text = "*Recent Download Tasks:*\n\n".to_string();
                                for t in tasks {
                                    let filename = t.filename.unwrap_or_else(|| "Unknown".into());
                                    let state = format!("{:?}", t.state);
                                    let progress = t.progress_percent.unwrap_or(0.0);
                                    text.push_str(&format!("📄 `{filename}`\n  *State:* `{state}` | `{progress:.1}%`\n  *ID:* `{}`\n\n", t.id));
                                }
                                let _ = bot.send_message(msg.chat.id, text).parse_mode(teloxide::types::ParseMode::MarkdownV2).await;
                            }
                            _ => { let _ = bot.send_message(msg.chat.id, "Error listing tasks.").await; }
                        }
                    }
                    BotCommand::Download { args } => {
                        let parts: Vec<&str> = args.split_whitespace().collect();
                        if parts.is_empty() {
                            let _ = bot.send_message(msg.chat.id, "Usage: /download <url> [filename]").await;
                            return Ok::<(), teloxide::RequestError>(());
                        }
                        let url = parts[0].to_string();
                        let filename = if parts.len() > 1 { Some(parts[1].to_string()) } else { None };

                        match cmd_handler.handle(AppCommand::CreateDownload { url, filename }).await {
                            Ok(Some(id)) => {
                                let _ = bot.send_message(msg.chat.id, format!("Download task created successfully!\nID: `{id}`")).parse_mode(teloxide::types::ParseMode::MarkdownV2).await;
                            }
                            Err(e) => {
                                let _ = bot.send_message(msg.chat.id, format!("Failed to create download: {e}")).await;
                            }
                            _ => {}
                        }
                    }
                    BotCommand::Pause { task_id } => {
                        if let Ok(id) = Uuid::parse_str(&task_id) {
                            match cmd_handler.handle(AppCommand::PauseDownload { task_id: id }).await {
                                Ok(_) => { let _ = bot.send_message(msg.chat.id, format!("Task {id} paused.")).await; }
                                Err(e) => { let _ = bot.send_message(msg.chat.id, format!("Error: {e}")).await; }
                            }
                        } else {
                            let _ = bot.send_message(msg.chat.id, "Invalid UUID task_id format.").await;
                        }
                    }
                    BotCommand::Resume { task_id } => {
                        if let Ok(id) = Uuid::parse_str(&task_id) {
                            match cmd_handler.handle(AppCommand::ResumeDownload { task_id: id }).await {
                                Ok(_) => { let _ = bot.send_message(msg.chat.id, format!("Task {id} resumed.")).await; }
                                Err(e) => { let _ = bot.send_message(msg.chat.id, format!("Error: {e}")).await; }
                            }
                        } else {
                            let _ = bot.send_message(msg.chat.id, "Invalid UUID task_id format.").await;
                        }
                    }
                    BotCommand::Retry { task_id } => {
                        if let Ok(id) = Uuid::parse_str(&task_id) {
                            match cmd_handler.handle(AppCommand::RetryDownload { task_id: id }).await {
                                Ok(_) => { let _ = bot.send_message(msg.chat.id, format!("Task {id} scheduled for retry.")).await; }
                                Err(e) => { let _ = bot.send_message(msg.chat.id, format!("Error: {e}")).await; }
                            }
                        } else {
                            let _ = bot.send_message(msg.chat.id, "Invalid UUID task_id format.").await;
                        }
                    }
                    BotCommand::Cancel { task_id } => {
                        if let Ok(id) = Uuid::parse_str(&task_id) {
                            match cmd_handler.handle(AppCommand::CancelDownload { task_id: id }).await {
                                Ok(_) => { let _ = bot.send_message(msg.chat.id, format!("Task {id} cancelled and deleted.")).await; }
                                Err(e) => { let _ = bot.send_message(msg.chat.id, format!("Error: {e}")).await; }
                            }
                        } else {
                            let _ = bot.send_message(msg.chat.id, "Invalid UUID task_id format.").await;
                        }
                    }
                    BotCommand::Files => {
                        // Scan download directory for files
                        if let Ok(mut entries) = tokio::fs::read_dir(&dl_dir).await {
                            let mut text = "*Download Folder Files:*\n\n".to_string();
                            let mut count = 0;
                            while let Ok(Some(entry)) = entries.next_entry().await {
                                if let Ok(meta) = entry.metadata().await {
                                    if meta.is_file() {
                                        let name = entry.file_name().to_string_lossy().into_owned();
                                        let size_mb = meta.len() as f64 / 1024.0 / 1024.0;
                                        text.push_str(&format!("• `{name}` \\({size_mb:.1} MB\\)\n"));
                                        count += 1;
                                        if count >= 15 { break; } // Cap at 15 files
                                    }
                                }
                            }
                            if count == 0 {
                                let _ = bot.send_message(msg.chat.id, "Download directory is empty.").await;
                            } else {
                                let _ = bot.send_message(msg.chat.id, text).parse_mode(teloxide::types::ParseMode::MarkdownV2).await;
                            }
                        } else {
                            let _ = bot.send_message(msg.chat.id, "Failed to read download folder.").await;
                        }
                    }
                    BotCommand::Stats => {
                        // Fetch system stats using query bus
                        let boot_time = std::time::Instant::now(); // Wait, stats takes query boot_time
                        match q_handler.handle(AppQuery::GetSystemStats { boot_time }).await {
                            Ok(QueryResult::SystemStats { active_tasks, queued_tasks, uptime_secs }) => {
                                let uptime_hours = uptime_secs as f64 / 3600.0;
                                let text = format!("*APEX Daemon Statistics*\n\n• *Active Downloads:* `{active_tasks}`\n• *Queued Downloads:* `{queued_tasks}`\n• *Daemon Uptime:* `{uptime_hours:.2} hours`\n• *Platform:* `Native Windows` (APEX Engine)");
                                let _ = bot.send_message(msg.chat.id, text).parse_mode(teloxide::types::ParseMode::MarkdownV2).await;
                            }
                            _ => { let _ = bot.send_message(msg.chat.id, "Failed to load metrics.").await; }
                        }
                    }
                    BotCommand::Logs { task_id } => {
                        if let Ok(id) = Uuid::parse_str(&task_id) {
                            match q_handler.handle(AppQuery::GetLogs { task_id: id }).await {
                                Ok(QueryResult::Logs(entries)) => {
                                    if entries.is_empty() {
                                        let _ = bot.send_message(msg.chat.id, "No logs recorded for this task.").await;
                                        return Ok::<(), teloxide::RequestError>(());
                                    }
                                    let mut text = format!("*Log History for* `{id}`:\n\n");
                                    for (event, ts) in entries.iter().take(10) {
                                        text.push_str(&format!("• `{event}` at timestamp `{ts}`\n"));
                                    }
                                    let _ = bot.send_message(msg.chat.id, text).parse_mode(teloxide::types::ParseMode::MarkdownV2).await;
                                }
                                _ => { let _ = bot.send_message(msg.chat.id, "Error reading logs.").await; }
                            }
                        } else {
                            let _ = bot.send_message(msg.chat.id, "Invalid UUID task_id format.").await;
                        }
                    }
                    BotCommand::Storage => {
                        match q_handler.handle(AppQuery::GetStorageInfo { download_dir: dl_dir.clone() }).await {
                            Ok(QueryResult::StorageInfo { free_space_bytes, total_space_bytes }) => {
                                let free_gb = free_space_bytes as f64 / 1024.0 / 1024.0 / 1024.0;
                                let total_gb = total_space_bytes as f64 / 1024.0 / 1024.0 / 1024.0;
                                let used_gb = total_gb - free_gb;
                                let pct = (used_gb / total_gb) * 100.0;
                                let path_str = dl_dir.to_string_lossy();
                                let text = format!("*Disk Storage Info*\n\n• *Download Directory:* `{path_str}`\n• *Disk Space:* `{used_gb:.1} GB` / `{total_gb:.1} GB` Used \\({pct:.1}%\\)\n• *Free Space:* `{free_gb:.1} GB` Available");
                                let _ = bot.send_message(msg.chat.id, text).parse_mode(teloxide::types::ParseMode::MarkdownV2).await;
                            }
                            _ => { let _ = bot.send_message(msg.chat.id, "Failed to load storage details.").await; }
                        }
                    }
                }
                Ok(())
            }
        },
    );

    tokio::spawn(async move {
        let mut dispatcher = Dispatcher::builder(bot, handler)
            .enable_ctrlc_handler()
            .build();
        dispatcher.dispatch().await;
    });

    Ok(())
}
