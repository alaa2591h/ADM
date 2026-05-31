// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — bridge/callbacks.rs                                           ║
// ║  Wires every Slint callback to push an AppEvent onto the EventBus.      ║
// ╚══════════════════════════════════════════════════════════════════════════╝

use std::cell::RefCell;
use std::rc::Rc;

use crate::MainWindow;
use crate::bridge::event_bus::EventBus;
use crate::contracts::RuntimeCommand;

/// Register every callback on `window` so that it pushes the appropriate
/// `RuntimeCommand` onto `bus`. All callbacks are lightweight — they just enqueue
/// a command; the heavy logic runs in the runtime tick.
pub fn wire_callbacks(window: &MainWindow, bus: Rc<RefCell<EventBus>>) {

    macro_rules! push {
        ($bus:expr, $cmd:expr) => {
            $bus.borrow_mut().push($cmd)
        };
    }

    // ── Add download ──────────────────────────────────────────────────────
    {
        let b = bus.clone();
        window.on_add_download(move |url, name, conns| {
            let connections = conns.parse::<u32>().unwrap_or(4);
            push!(b, RuntimeCommand::AddDownload {
                url: url.to_string(),
                filename: name.to_string(),
                connections,
            });
        });
    }

    // ── Add scheduled ─────────────────────────────────────────────────────
    {
        let b = bus.clone();
        window.on_add_scheduled(move |url, name, time| {
            push!(b, RuntimeCommand::AddScheduledDownload {
                url: url.to_string(),
                filename: name.to_string(),
                scheduled_time: time.to_string(),
                connections: 4,
            });
        });
    }

    // ── Bulk actions ──────────────────────────────────────────────────────
    {
        let b = bus.clone();
        window.on_pause_all(move || push!(b, RuntimeCommand::PauseAll));
    }
    {
        let b = bus.clone();
        window.on_resume_all(move || push!(b, RuntimeCommand::ResumeAll));
    }
    {
        let b = bus.clone();
        window.on_delete_selected(move || push!(b, RuntimeCommand::DeleteSelected));
    }
    {
        let b = bus.clone();
        window.on_delete_all(move || push!(b, RuntimeCommand::DeleteAll));
    }

    // ── Per-item actions ──────────────────────────────────────────────────
    {
        let b = bus.clone();
        window.on_pause_item(move |id| push!(b, RuntimeCommand::PauseDownload(id.to_string())));
    }
    {
        let b = bus.clone();
        window.on_resume_item(move |id| push!(b, RuntimeCommand::ResumeDownload(id.to_string())));
    }
    {
        let b = bus.clone();
        window.on_delete_item(move |id| push!(b, RuntimeCommand::DeleteDownload(id.to_string())));
    }
    {
        let b = bus.clone();
        window.on_move_to_queue(move |id| push!(b, RuntimeCommand::MoveToQueue(id.to_string())));
    }
    {
        let b = bus.clone();
        window.on_move_selected_to_queue(move || push!(b, RuntimeCommand::MoveSelectedToQueue));
    }

    // ── Open folder ───────────────────────────────────────────────────────
    {
        let b = bus.clone();
        window.on_open_folder(move |id| {
            push!(b, RuntimeCommand::OpenFolder(id.to_string()));
        });
    }

    // ── Retry failed download ─────────────────────────────────────────────
    {
        let b = bus.clone();
        window.on_retry_item(move |id| {
            push!(b, RuntimeCommand::RetryDownload(id.to_string()));
        });
    }

    // ── Chunk details — push to state so ui_bridge can fill ChunkPanel props ──
    {
        let b = bus.clone();
        window.on_show_chunk_details(move |id| {
            push!(b, RuntimeCommand::ShowChunkDetails(id.to_string()));
        });
    }

    // ── UI state ──────────────────────────────────────────────────────────
    {
        let b = bus.clone();
        window.on_select_row(move |id, shift, control| push!(b, RuntimeCommand::SelectRow {
            id: id.to_string(),
            shift,
            control,
        }));
    }
    {
        let b = bus.clone();
        window.on_select_prev(move |shift, control| push!(b, RuntimeCommand::SelectPrev { shift, control }));
    }
    {
        let b = bus.clone();
        window.on_select_next(move |shift, control| push!(b, RuntimeCommand::SelectNext { shift, control }));
    }
    {
        let b = bus.clone();
        window.on_select_page_up(move |shift, control| push!(b, RuntimeCommand::SelectPageUp { shift, control }));
    }
    {
        let b = bus.clone();
        window.on_select_page_down(move |shift, control| push!(b, RuntimeCommand::SelectPageDown { shift, control }));
    }
    {
        let b = bus.clone();
        window.on_toggle_selected(move || push!(b, RuntimeCommand::ToggleSelected));
    }
    {
        let b = bus.clone();
        window.on_set_filter(move |f| push!(b, RuntimeCommand::SetFilter(f.to_string())));
    }
    {
        let b = bus.clone();
        window.on_search_changed(move |q| push!(b, RuntimeCommand::SearchChanged(q.to_string())));
    }

    // ── Layout direction ──────────────────────────────────────────────────
    {
        let b = bus.clone();
        window.on_toggle_rtl(move || push!(b, RuntimeCommand::ToggleRTL));
    }

    // ── Settings ──────────────────────────────────────────────────────────
    {
        let b = bus.clone();
        window.on_save_settings(move |
            // General
            path, launch_on_startup, start_minimized, auto_update, send_telemetry,
            ask_download_path, create_subfolder, open_folder_on_done, on_complete_action,
            // Downloads
            max_simultaneous, max_conns, retry_failed, retry_count, retry_delay,
            skip_duplicates, duplicate_action, verify_integrity, auto_extract,
            // Connection
            use_proxy, proxy_type, proxy_auth, bind_interface, timeout_secs,
            force_ipv4, server_rate_limit, remember_creds, user_agent,
            // Speed
            global_dl_limit, global_ul_limit, speed_profile, slow_speed_kbps,
            // Scheduler
            scheduler_enabled, sched_speed_mode,
            // Browser
            capture_chrome, capture_firefox, capture_edge, capture_brave, capture_opera,
            capture_all, capture_confirm, capture_video, min_capture_mb,
            // Notifications
            notif_done, notif_fail, notif_pause, notif_disk, notif_new,
            sound_done, sound_type, do_not_disturb,
            // Appearance
            theme_mode, font_size, taskbar_progress, speed_graph,
            compact_rows, show_chunks, animations,
            // Security
            app_lock, biometric, lock_timeout_mins, av_scan, block_malware,
            warn_exe, https_only, verify_ssl, history_retention, clear_on_exit,
            // Advanced
            workers, chunk_size_kb, buffer_mb, disk_write_strategy,
            rest_api, daemon_host, daemon_port, log_level, max_log_mb,
            // Language
            language, date_fmt, num_fmt,
        | {
            push!(b, RuntimeCommand::SaveSettings {
                path:                path.to_string(),
                launch_on_startup,
                start_minimized,
                auto_update,
                send_telemetry,
                ask_download_path,
                create_subfolder,
                open_folder_on_done,
                on_complete_action,
                max_simultaneous,
                max_conns:           max_conns.to_string(),
                retry_failed,
                retry_count,
                retry_delay,
                skip_duplicates,
                duplicate_action,
                verify_integrity,
                auto_extract,
                use_proxy,
                proxy_type,
                proxy_auth,
                bind_interface,
                timeout_secs,
                force_ipv4,
                server_rate_limit,
                remember_creds,
                user_agent,
                global_dl_limit,
                global_ul_limit,
                speed_profile,
                slow_speed_kbps,
                scheduler_enabled,
                sched_speed_mode,
                capture_chrome,
                capture_firefox,
                capture_edge,
                capture_brave,
                capture_opera,
                capture_all,
                capture_confirm,
                capture_video,
                min_capture_mb,
                notif_done,
                notif_fail,
                notif_pause,
                notif_disk,
                notif_new,
                sound_done,
                sound_type,
                do_not_disturb,
                theme_mode,
                font_size,
                taskbar_progress,
                speed_graph,
                compact_rows,
                show_chunks,
                animations,
                app_lock,
                biometric,
                lock_timeout_mins,
                av_scan,
                block_malware,
                warn_exe,
                https_only,
                verify_ssl,
                history_retention,
                clear_on_exit,
                workers:             workers.to_string(),
                chunk_size_kb,
                buffer_mb,
                disk_write_strategy,
                rest_api,
                daemon_host:         daemon_host.to_string(),
                daemon_port:         daemon_port.to_string(),
                log_level,
                max_log_mb,
                language,
                date_fmt,
                num_fmt,
            });
        });
    }

    // ── Browse folder (native file picker) ────────────────────────────────
    {
        let b = bus.clone();
        window.on_browse_folder(move || {
            push!(b, RuntimeCommand::BrowseFolder);
        });
    }

    // ── Reset defaults ────────────────────────────────────────────────────
    {
        let b = bus.clone();
        window.on_reset_defaults(move || {
            push!(b, RuntimeCommand::ResetSettings);
        });
    }

    // ── Language buttons ──────────────────────────────────────────────────
    {
        let b = bus.clone();
        window.on_open_translation_tool(move || {
            push!(b, RuntimeCommand::OpenUrl("https://translate.apex-dm.app".to_string()));
        });
    }
    {
        let b = bus.clone();
        window.on_browse_language_pack(move || {
            push!(b, RuntimeCommand::BrowseLanguagePack);
        });
    }

    // ── Open logs folder ──────────────────────────────────────────────────
    {
        let b = bus.clone();
        window.on_open_logs_folder(move || {
            push!(b, RuntimeCommand::OpenLogsFolder);
        });
    }

    // ── Dismiss notification ──────────────────────────────────────────────
    {
        let b = bus.clone();
        window.on_dismiss_notification(move || {
            push!(b, RuntimeCommand::DismissNotification);
        });
    }
}
