// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — models/statistics.rs                                          ║
// ║  Computes aggregate statistics from the download list.                   ║
// ╚══════════════════════════════════════════════════════════════════════════╝

use crate::models::download_item::{DownloadEntry, DownloadStatus, format_bytes, format_speed};

#[derive(Debug, Clone)]
pub struct AppStatistics {
    pub total_downloaded: String,
    pub active_speed: String,
    pub active_count: i32,
    pub completed_today: i32,
}

impl AppStatistics {
    pub fn compute(downloads: &[DownloadEntry]) -> Self {
        let mut total_bytes: u64 = 0;
        let mut speed_sum: f64   = 0.0;
        let mut active_count     = 0i32;
        let mut completed_today  = 0i32;

        for dl in downloads {
            total_bytes += dl.downloaded_bytes;
            if dl.status == DownloadStatus::Running {
                speed_sum += dl.speed_bps;
                active_count += 1;
            }
            if dl.status == DownloadStatus::Completed {
                completed_today += 1;
            }
        }

        AppStatistics {
            total_downloaded: format_bytes(total_bytes),
            active_speed:     if active_count > 0 { format_speed(speed_sum) } else { "—".to_string() },
            active_count,
            completed_today,
        }
    }
}
