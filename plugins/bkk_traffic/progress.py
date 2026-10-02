"""สรุปความคืบหน้า: ดึงไปแล้วเท่าไหร่, checkpoint อยู่ตรงไหน, เหลืออีกนานแค่ไหน."""
import json
from datetime import datetime

from bkk_traffic import config, storage


def summary() -> dict:
    with storage.connect() as conn:
        q = lambda sql, *a: conn.execute(sql, a).fetchone()[0]  # noqa: E731
        bf = storage.get_checkpoint(conn, "backfill")
        rt = storage.get_checkpoint(conn, "realtime")
        s = {
            "generated_at": storage.now_str(),
            "eids_seen_all": q("SELECT COUNT(*) FROM seen_eids"),
            "incidents_bangkok": q("SELECT COUNT(*) FROM incidents WHERE is_bangkok=1"),
            "accidents_bangkok": q("SELECT COUNT(*) FROM incidents WHERE is_bangkok=1 AND is_accident=1"),
            "start_time_range": list(conn.execute("SELECT MIN(start_time), MAX(start_time) FROM incidents").fetchone()),
            "eid_range": list(conn.execute("SELECT MIN(eid), MAX(eid) FROM incidents").fetchone()),
            "by_type": {r[0]: r[1] for r in conn.execute(
                "SELECT event_type, COUNT(*) FROM incidents WHERE is_bangkok=1 GROUP BY 1 ORDER BY 2 DESC")},
            "traffic_index_points": q("SELECT COUNT(*) FROM traffic_index"),
            "layers": {
                "bronze_raw": q("SELECT COUNT(*) FROM bronze_incidents"),
                "bronze_not_cleaned": q("SELECT COUNT(*) FROM bronze_incidents WHERE cleaned_at IS NULL"),
                "silver_clean": q("SELECT COUNT(*) FROM incidents"),
                "silver_duplicates": q("SELECT COUNT(*) FROM incidents WHERE is_duplicate=1"),
                "silver_flagged": q("SELECT COUNT(*) FROM incidents WHERE dq_flags IS NOT NULL"),
                "quarantine": q("SELECT COUNT(*) FROM quarantine"),
            },
            "missing_eids": {r[0]: r[1] for r in conn.execute("SELECT status, COUNT(*) FROM missing_eids GROUP BY 1")},
            "requests_total": q("SELECT COALESCE(SUM(requests),0) FROM scrape_runs"),
            "runs_last_24h": {f"{r[0]}:{r[1]}": r[2] for r in conn.execute(
                "SELECT job, status, COUNT(*) FROM scrape_runs "
                "WHERE started_at >= datetime('now','localtime','-1 day') GROUP BY 1,2")},
            "realtime_checkpoint": rt,
            "backfill_checkpoint": bf,
        }
    # ประมาณ % ความคืบหน้า backfill จากช่วงเวลา (วันนี้ -> target) ที่ไล่ไปแล้ว
    try:
        anchor = datetime.fromisoformat(bf.get("anchor_time") or "")
        target = datetime.fromisoformat(bf.get("target_start_date") or "")
    except ValueError:  # วันที่เสียจากต้นทาง ไม่ให้ทั้ง task ล้ม
        anchor = target = None
    if anchor and target:
        now = datetime.now()
        span = (now - target).total_seconds()
        done = min(1.0, max(0.0, (now - anchor).total_seconds() / span)) if span > 0 else 1.0
        pages_done = bf.get("pages_fetched_total", 0)
        est_pages_left = int(pages_done / done - pages_done) if done > 0 else None
        s["backfill_progress"] = {
            "percent": round(100 * done, 2),
            "reached_back_to": bf["anchor_time"],
            "target": bf["target_start_date"],
            "current_page": bf.get("page"),
            "source_total_pages": bf.get("source_total_pages"),
            "est_pages_left": est_pages_left,
            "est_hours_left": round(est_pages_left * config.CRAWL_DELAY_SEC / 3600, 1) if est_pages_left else 0,
            "done": bf.get("done", False),
        }
    return s


def write_progress_file() -> dict:
    s = summary()
    config.CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    (config.CHECKPOINT_DIR / "progress.json").write_text(json.dumps(s, ensure_ascii=False, indent=2),
                                                         encoding="utf-8")
    return s
