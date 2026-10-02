"""งานดึงข้อมูล: realtime, backfill (มี checkpoint), traffic index, เติม eid ที่ขาด.

API คืนเหตุการณ์ทั้งประเทศ เราเก็บเฉพาะกรุงเทพมหานคร (ทั้งใน DB และไฟล์ raw)
"""
import logging
import time
from datetime import datetime

from bkk_traffic import storage
from bkk_traffic.client import LongdoClient

log = logging.getLogger(__name__)


def _store(conn, client, items, fetched_via, raw_name) -> int:
    """upsert + บันทึก raw เฉพาะรายการ กทม.; คืนจำนวนเหตุการณ์ กทม. ใหม่."""
    new, bkk_items = storage.upsert_incidents(conn, items, fetched_via)
    if bkk_items:
        client.save_raw(raw_name, {"item": bkk_items})
    return new


def refresh_event_types():
    client = LongdoClient("event_types")
    types = client.event_types()
    with storage.connect() as conn:
        for t in types:
            conn.execute(
                "INSERT OR REPLACE INTO event_types(id, name, name_en, icon, severity) VALUES(?,?,?,?,?)",
                (t.get("id"), t.get("name"), t.get("name_en"), t.get("icon"), t.get("severity")),
            )
    log.info("event types: %d", len(types))
    return len(types)


def _ensure_event_types():
    with storage.connect() as conn:
        has = conn.execute("SELECT COUNT(*) FROM event_types").fetchone()[0]
    if not has:
        refresh_event_types()


def run_realtime_incidents(max_pages: int):
    """ดึงหน้า 1 (เหตุการณ์ที่ยัง active ทั้งหมด) แล้วไล่หน้า 2.. จนเจอหน้าที่รู้จักทุก eid แล้ว."""
    _ensure_event_types()
    started = storage.now_str()
    client = LongdoClient("incidents_realtime")
    total_items = new_items = 0
    total_pages = None
    new_this_run = set()  # หน้า 1 ซ้อนกับหน้า 2.. จึงต้องเทียบกับสิ่งที่มีก่อนรอบนี้เท่านั้น
    with storage.connect() as conn:
        try:
            for page in range(1, max_pages + 1):
                items, total_pages = client.incident_page(page)
                if not items:
                    break
                eids = {storage.to_int(i.get("eid")) for i in items} - {None}
                unknown = eids - storage.known_eids(conn, eids)
                new_this_run |= unknown
                all_known_before = not (eids & new_this_run)
                new_items += _store(conn, client, items, "realtime", f"page{page:06d}")
                total_items += len(items)
                if page >= 2 and all_known_before:
                    break
            newest = conn.execute("SELECT MAX(eid) m, MAX(start_time) t FROM incidents").fetchone()
            storage.set_checkpoint(conn, "realtime", {
                "last_success": storage.now_str(), "pages_read": page, "items_last_run": total_items,
                "new_last_run": new_items, "source_total_pages": total_pages,
                "max_eid": newest["m"], "max_start_time": newest["t"],
            })
            storage.log_run(conn, "realtime", started, client.request_count, total_items, new_items, "success")
        except Exception as err:
            storage.log_run(conn, "realtime", started, client.request_count, total_items, new_items,
                            "failed", str(err)[:500])
            raise
    log.info("realtime: items=%d new=%d requests=%d", total_items, new_items, client.request_count)
    return {"items": total_items, "new": new_items}


def run_traffic_index():
    client = LongdoClient("traffic_index")
    data = client.traffic_index()
    ts = storage.to_int(data.get("time"))
    with storage.connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO traffic_index(ts, ts_local, index_value, fetched_at) VALUES(?,?,?,?)",
            (ts, datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else None,
             storage.to_float(data.get("index")), storage.now_str()),
        )
        if ts:  # ต่อท้าย time series 5 นาที (ค่าจากไฟล์ย้อนหลังจะทับทีหลังถ้ามี)
            conn.execute(
                "INSERT OR IGNORE INTO traffic_index_history(ts, ts_local, index_value, source) VALUES(?,?,?,?)",
                (ts, datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S"), storage.to_float(data.get("index")),
                 "realtime"),
            )
    return data


def run_backfill_batch(pages_per_run: int, start_date: str, page_size: int = 10,
                       target_rows: int = 0, max_seconds: int = 0):
    """ไล่หน้าย้อนหลังต่อจาก checkpoint.

    รายการเรียงตาม start_time ใหม่->เก่า และมีเหตุการณ์ใหม่แทรกด้านบนตลอด ทำให้หน้าเลื่อน
    จึงเก็บ anchor_time (start_time เก่าสุดที่เคยเห็น) ไว้: ถ้าหน้าที่เปิดมี median ใหม่กว่า anchor
    แปลว่าหน้าเลื่อน -> ข้ามไปหน้าถัดไป (ข้อมูลซ้ำไม่เป็นไร เพราะ upsert ด้วย eid)

    target_rows > 0: หยุดเมื่อเหตุการณ์ กทม. ใน warehouse ครบจำนวนนี้
    """
    _ensure_event_types()
    started = storage.now_str()
    t0 = time.monotonic()
    client = LongdoClient("incidents_backfill")
    with storage.connect() as conn:
        state = storage.get_checkpoint(conn, "backfill")
        if state.get("done") and (state.get("target_start_date") != start_date
                                  or state.get("target_rows") != target_rows):
            state["done"] = False  # เปลี่ยนเป้าหมาย -> ทำต่อ
        state.setdefault("page", 2)  # หน้า 1 เป็นรายการ active พิเศษ realtime ดึงอยู่แล้ว
        state.setdefault("page_size", 10)
        state.setdefault("pages_fetched_total", 0)
        state.setdefault("items_total", 0)
        state.setdefault("skipped_shift_pages", 0)
        if state["page_size"] != page_size:
            # เปลี่ยนขนาดหน้า: แปลงเลขหน้าจาก offset เดิม (ถอยหลังเผื่อไว้ 1 หน้า ซ้ำได้ ไม่ขาด)
            offset = (state["page"] - 1) * state["page_size"]
            state["page"] = max(2, offset // page_size)
            state["page_size"] = page_size
        state["target_start_date"] = start_date
        state["target_rows"] = target_rows
        if state.get("done"):
            log.info("backfill already done: %s", state.get("done_reason"))
            return state

        def bkk_rows():
            return conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]

        items_run = new_run = 0
        try:
            for _ in range(pages_per_run):
                if target_rows and bkk_rows() >= target_rows:
                    state["done"] = True
                    state["done_reason"] = f"reached target {target_rows:,} Bangkok rows"
                    break
                if max_seconds and time.monotonic() - t0 > max_seconds:
                    break
                page = state["page"]
                items, total_pages = client.incident_page(page, page_size)
                state["pages_fetched_total"] += 1
                if total_pages:
                    state["source_total_pages"] = total_pages
                if not items:
                    state["done"] = True
                    state["done_reason"] = "end_of_list"
                    break
                # ข้ามวันที่เสีย (0000-00-00, 1467, 1970) ไม่ให้ทำ anchor ผิดจนหยุดก่อนจบ
                times = sorted(t for t in (i.get("start_time") or "" for i in items) if t >= "2000")
                if not times:
                    state["page"] = page + 1
                    storage.set_checkpoint(conn, "backfill", state)
                    continue
                median = times[len(times) // 2]
                anchor = state.get("anchor_time")
                new_run += _store(conn, client, items, "backfill", f"page{page:06d}")
                items_run += len(items)
                state["page"] = page + 1
                if anchor and median > anchor:
                    state["skipped_shift_pages"] += 1
                else:
                    oldest = next((t for t in times if t), None)
                    if oldest and (not anchor or oldest < anchor):
                        state["anchor_time"] = oldest
                state["items_total"] += len(items)
                storage.set_checkpoint(conn, "backfill", state)
                if state.get("anchor_time") and state["anchor_time"] < start_date:
                    state["done"] = True
                    state["done_reason"] = f"reached {start_date}"
                    break
            if target_rows and not state.get("done") and bkk_rows() >= target_rows:
                state["done"] = True
                state["done_reason"] = f"reached target {target_rows:,} Bangkok rows"
            state["bangkok_rows"] = bkk_rows()
            storage.set_checkpoint(conn, "backfill", state)
            storage.log_run(conn, "backfill", started, client.request_count, items_run, new_run, "success",
                            f"page={state['page']} anchor={state.get('anchor_time')}")
        except Exception as err:
            storage.set_checkpoint(conn, "backfill", state)
            storage.log_run(conn, "backfill", started, client.request_count, items_run, new_run,
                            "failed", str(err)[:500])
            raise
    log.info("backfill: page=%s anchor=%s new=%d", state["page"], state.get("anchor_time"), new_run)
    return state


def detect_missing_eids():
    """หา eid ที่ขาดในช่วงที่ backfill/realtime ครอบคลุมแล้ว แล้วใส่ลง missing_eids (pending)."""
    with storage.connect() as conn:
        state = storage.get_checkpoint(conn, "backfill")
        floor_time = state.get("anchor_time")
        if not floor_time:
            return {"range": None, "new_missing": 0}
        # ใช้ percentile 1% ของ eid ในช่วงเวลาที่ครอบคลุม กัน event เก่าที่ยัง active ทำให้ช่วงกว้างเกิน
        eids = [r[0] for r in conn.execute(
            "SELECT eid FROM incidents WHERE start_time >= ? ORDER BY eid", (floor_time,))]
        if len(eids) < 100:
            return {"range": None, "new_missing": 0}
        lo = eids[len(eids) // 100]
        hi = conn.execute("SELECT MAX(eid) FROM seen_eids").fetchone()[0]
        # eid นอก กทม. ที่เคยเห็นก็นับว่า "ไม่ขาด" (เก็บไว้ใน seen_eids แค่ id)
        present = {r[0] for r in conn.execute("SELECT eid FROM seen_eids WHERE eid BETWEEN ? AND ?", (lo, hi))}
        tracked = {r[0] for r in conn.execute("SELECT eid FROM missing_eids WHERE eid BETWEEN ? AND ?", (lo, hi))}
        missing = [e for e in range(lo, hi + 1) if e not in present and e not in tracked]
        ts = storage.now_str()
        conn.executemany("INSERT OR IGNORE INTO missing_eids(eid, detected_at, tries, status) VALUES(?,?,0,'pending')",
                         [(e, ts) for e in missing])
    log.info("gap range %s-%s new missing=%d", lo, hi, len(missing))
    return {"range": [lo, hi], "new_missing": len(missing)}


def recover_missing_eids(limit: int, max_seconds: int = 0):
    """ลองดึง eid ที่ขาดทีละตัว (ใหม่สุดก่อน). ถ้า API คืนว่าง = ไม่เปิดเผย/ถูกลบ -> not_available.

    max_seconds: หยุดเมื่อหมดเวลา (rate limit แชร์กับ backfill/realtime ทำให้ช้ากว่าที่คำนวณ) ที่เหลือทำรอบหน้า
    """
    _ensure_event_types()
    started = storage.now_str()
    t0 = time.monotonic()
    client = LongdoClient("incidents_recover")
    recovered = recovered_bkk = unavailable = 0
    with storage.connect() as conn:
        pending = [r[0] for r in conn.execute(
            "SELECT eid FROM missing_eids WHERE status='pending' ORDER BY eid DESC LIMIT ?", (limit,))]
        for eid in pending:
            if max_seconds and time.monotonic() - t0 > max_seconds:
                break
            ts = storage.now_str()
            try:
                item = client.event_by_id(eid)
            except Exception as err:  # noqa: BLE001 - ข้ามตัวที่ error ไว้ลองรอบหน้า
                log.warning("eid %s error: %s", eid, err)
                conn.execute("UPDATE missing_eids SET tries=tries+1, last_try_at=? WHERE eid=?", (ts, eid))
                continue
            if item:
                recovered_bkk += _store(conn, client, [item], "recover", f"eid{eid}")
                recovered += 1
            else:
                conn.execute("UPDATE missing_eids SET status='not_available', tries=tries+1, last_try_at=? "
                             "WHERE eid=?", (ts, eid))
                unavailable += 1
            conn.commit()
        storage.log_run(conn, "recover", started, client.request_count, len(pending), recovered_bkk, "success",
                        f"recovered={recovered} not_available={unavailable}")
    return {"tried": len(pending), "recovered": recovered, "recovered_bangkok": recovered_bkk,
            "not_available": unavailable}
