"""ขั้น CLEAN: bronze_incidents (raw JSON) -> incidents (silver) หรือ quarantine.

แยกจากขั้นดึงข้อมูล จึงสั่ง clean ใหม่ทั้งหมดจาก bronze ได้โดยไม่ต้องสแคปซ้ำ (reclean_all)

กฎรายแถว (ไม่ผ่านกฎ "กักไว้" = ไปตาราง quarantine พร้อมเหตุผล ไม่ถูก export):
  R1 มี eid เป็นตัวเลข                      -> กักไว้
  R2 อยู่ในกรุงเทพฯ                          -> กักไว้ (ปกติถูกกรองตั้งแต่ขั้น scrape แล้ว)
  R3 start_time แปลงเป็นวันเวลาได้ และ >= 2008 -> กักไว้ (0000-00-00, 1467, 1970 = ข้อมูลเสีย)
  R4 stop_time ไม่ก่อน start_time            -> ตั้ง stop_time = NULL + flag
  R5 มีชื่อเหตุการณ์ (title)                  -> กักไว้
  R6 severity (จาก severity_level) อยู่ใน 1-3 -> ตั้ง severity = NULL + flag
  R7 start_time ไม่เลยวันนี้ไปเกิน 365 วัน    -> กักไว้
  R8 เกี่ยวกับการจราจร (traffic_relevance)     -> กักไว้ (ปกติถูกกรองตั้งแต่ขั้น scrape แล้ว)
  R9 start_time ไม่ก่อน createtime เกิน 1 ปี   -> กักไว้ (ปีที่พิมพ์ผิด)
  (R2 ไม่มีพิกัด ตัดสินจากข้อความ -> flag R2_no_coords)

ตัดรายงานซ้ำ (mark_duplicates): ผู้รายงาน+ชื่อ+เวลาเริ่ม+พิกัด(4 ตำแหน่ง) ตรงกัน
-> เก็บ eid น้อยสุด ตัวอื่น is_duplicate=1 (ยังอยู่ใน warehouse แต่ไม่ถูก export)
"""
import html
import json
import logging
import re
from datetime import datetime, timedelta

from bkk_traffic import config, geo

log = logging.getLogger(__name__)

_TAG_RE = re.compile(r"<[^>]+>")
_CREDIT_RE = re.compile(r"(\bcr\b\.?|เครดิต|ขอบคุณ|ที่มา\s*:|source\s*:).*$", re.IGNORECASE)

MIN_VALID_TIME = datetime(2008, 1, 1)  # Longdo Traffic เริ่มให้บริการปี 2009


def parse_time(value):
    """แปลง 'YYYY-MM-DD HH:MM:SS'; ค่าที่ไม่ใช่วันที่จริง (เช่น 0000-00-00) คืน None."""
    try:
        return datetime.fromisoformat(str(value).strip())
    except (TypeError, ValueError):
        return None


SILVER_COLS = ("eid", "nid", "title", "title_en", "comment", "comment_en", "latitude", "longitude", "severity",
               "start_time", "stop_time", "createtime", "updatedtime", "contributor", "source", "status",
               "event_type_id", "event_type", "traffic_relevance", "is_accident", "is_bangkok", "province_guess",
               "dq_flags")

# เก็บเฉพาะเหตุการณ์ที่เกี่ยวกับการจราจร (ดูจากตัวอย่างจริงของแต่ละประเภท)
TRAFFIC_DIRECT = {"Accident", "Car Break Down", "Trafficjam", "Diversion", "Road Closed", "Construction",
                  "Check Point"}
TRAFFIC_AFFECTS = {"Warning", "Rain", "Flood", "Crowd", "Event"}
# ประเภทอื่น (Fire, MISC, Information, Earthquake, Complaint, Sale, ...) เก็บเฉพาะเมื่อชื่อมีคำเหล่านี้
TRAFFIC_KEYWORDS = ("จราจร", "ปิดถนน", "ปิดช่องทาง", "เบี่ยง", "รถติด", "อุบัติเหตุ", "รถชน", "รถเสีย", "ทางด่วน",
                    "ทางพิเศษ", "traffic", "road closed", "accident")


def traffic_relevance(event_type, title, title_en) -> str | None:
    """direct / affects / keyword / None (= ไม่เกี่ยวกับการจราจร -> ไม่เก็บ)."""
    if event_type in TRAFFIC_DIRECT:
        return "direct"
    if event_type in TRAFFIC_AFFECTS:
        return "affects"
    text = f"{title or ''} {title_en or ''}".lower()
    return "keyword" if any(k in text for k in TRAFFIC_KEYWORDS) else None


# ---------------- แปลงค่า ----------------
def clean_text(value):
    if value is None:
        return None
    text = html.unescape(_TAG_RE.sub(" ", str(value)))
    return re.sub(r"\s+", " ", text).strip() or None


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def classify_type(title, title_en, type_names):
    title = title or ""
    title_en = (title_en or "").lower()
    for name, name_en in type_names:
        if name and title.startswith(name):
            return name_en or name
    for name, name_en in type_names:
        if name_en and title_en.startswith(name_en.lower()):
            return name_en
    return "Unknown"


def guess_province(text: str, lat, lon):
    """คืน (province_guess, is_bangkok). พิกัดมาก่อน, ไม่มีพิกัดค่อยดูชื่อจังหวัดท้ายสุดในข้อความ."""
    if lat and lon:
        return ("bangkok", 1) if geo.in_bangkok(lat, lon) else ("outside_bangkok", 0)
    low = _CREDIT_RE.sub("", (text or "").lower())
    best, best_pos = None, -1
    for word in config.BANGKOK_WORDS + config.OTHER_PROVINCE_WORDS:
        pos = low.rfind(word)
        if pos > best_pos:
            best, best_pos = word, pos
    if best is None:
        return None, 0
    return ("bangkok(text)", 1) if best in config.BANGKOK_WORDS else (best, 0)


def _texts(item):
    return [clean_text(item.get(k)) for k in ("title", "title_en", "comment", "comment_en")]


def in_scope(item, types) -> tuple[int, str | None]:
    """ใช้ตอน scrape (ก่อนเก็บ raw): คืน (อยู่ใน กทม.?, traffic_relevance). เก็บเมื่อ กทม. และเกี่ยวกับจราจร."""
    lat, lon = to_float(item.get("latitude")), to_float(item.get("longitude"))
    texts = _texts(item)
    _, is_bkk = guess_province(" ".join(t for t in texts if t), lat, lon)
    type_names, type_map = types
    event_type = type_map.get(to_int(item.get("severity"))) or classify_type(texts[0], texts[1], type_names)
    return is_bkk, traffic_relevance(event_type, texts[0], texts[1])


def normalize(item: dict, type_names, type_map: dict | None = None) -> dict:
    """หมายเหตุ API: ฟิลด์ "severity" จริงๆ คือ id ประเภทเหตุการณ์ (ตรงกับ geteventtype)
    ส่วนความรุนแรงจริงอยู่ที่ "severity_level" (1 หรือ 3)
    """
    lat, lon = to_float(item.get("latitude")), to_float(item.get("longitude"))
    title, title_en, comment, comment_en = _texts(item)
    all_text = " ".join(t for t in (title, title_en, comment, comment_en) if t)
    province, is_bkk = guess_province(all_text, lat, lon)
    type_id = to_int(item.get("severity"))
    event_type = (type_map or {}).get(type_id) or classify_type(title, title_en, type_names)
    is_accident = int(event_type == "Accident" or any(w in all_text.lower() for w in config.ACCIDENT_WORDS))
    contributor = item.get("contributor")
    if isinstance(contributor, list):
        contributor = ",".join(map(str, contributor))
    return {
        "eid": to_int(item.get("eid")), "nid": to_int(item.get("nid")),
        "title": title, "title_en": title_en, "comment": comment, "comment_en": comment_en,
        "latitude": lat, "longitude": lon, "severity": to_int(item.get("severity_level")),
        "event_type_id": type_id,
        "start_time": item.get("start_time"), "stop_time": item.get("stop_time"),
        "createtime": item.get("createtime"), "updatedtime": item.get("updatedtime"),
        "contributor": contributor, "source": item.get("source"), "status": item.get("status"),
        "event_type": event_type, "traffic_relevance": traffic_relevance(event_type, title, title_en),
        "is_accident": is_accident, "is_bangkok": is_bkk,
        "province_guess": province,
    }


# ---------------- กฎรายแถว ----------------
def apply_rules(row: dict, now: datetime | None = None) -> tuple[dict, list[str], list[str]]:
    """คืน (row ที่แก้แล้ว, flags, reject_reasons). reject_reasons ไม่ว่าง = กักไว้."""
    now = now or datetime.now()
    flags, rejects = [], []
    if row["eid"] is None:
        rejects.append("R1_no_eid")
    if not row["is_bangkok"]:
        rejects.append("R2_not_bangkok")
    elif row["latitude"] is None or row["longitude"] is None:
        flags.append("R2_no_coords")
    start = parse_time(row["start_time"])
    if start is None:
        rejects.append("R3_bad_start_time")
    elif start < MIN_VALID_TIME:
        rejects.append("R3_start_out_of_range")       # เช่น 0000-00-00, 1467, 1970 (epoch 0)
    created = parse_time(row["createtime"]) if row.get("createtime") else None
    if start and created and start < created - timedelta(days=365):
        rejects.append("R9_start_long_before_create")  # เช่น start 2009 แต่สร้างรายงานปี 2020 = พิมพ์ปีผิด
    if not row["title"]:
        rejects.append("R5_no_title")
    if not row["traffic_relevance"]:
        rejects.append("R8_not_traffic")
    if start and start > now + timedelta(days=365):
        rejects.append("R7_start_too_far_future")
    stop = parse_time(row["stop_time"]) if row["stop_time"] else None
    if row["stop_time"] and (stop is None or (start and stop < start)):
        row["stop_time"] = None
        flags.append("R4_stop_before_start")
    if row["severity"] is not None and row["severity"] not in (1, 2, 3):
        row["severity"] = None
        flags.append("R6_bad_severity")
    row["dq_flags"] = ",".join(flags) or None
    return row, flags, rejects


def load_type_names(conn):
    rows = conn.execute("SELECT name, name_en FROM event_types ORDER BY length(name) DESC").fetchall()
    return [(r[0], r[1]) for r in rows]


def load_types(conn):
    """(รายชื่อสำหรับเดาจาก title, map id -> name_en)."""
    type_map = {r[0]: r[1] for r in conn.execute("SELECT id, name_en FROM event_types")}
    return load_type_names(conn), type_map


# ---------------- รัน clean ----------------
def clean_eids(conn, eids, types=None) -> dict:
    """clean เฉพาะ eid ที่ระบุ จาก bronze -> silver/quarantine."""
    type_names, type_map = types if types is not None else load_types(conn)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stats = {"clean": 0, "quarantined": 0, "flagged": 0}
    for eid in eids:
        b = conn.execute("SELECT raw_json, fetched_via FROM bronze_incidents WHERE eid=?", (eid,)).fetchone()
        if not b:
            continue
        raw = json.loads(b[0])
        row, flags, rejects = apply_rules(normalize(raw, type_names, type_map))
        if rejects:
            conn.execute("DELETE FROM incidents WHERE eid=?", (eid,))
            conn.execute("INSERT INTO quarantine(eid, reasons, raw_json, detected_at) VALUES(?,?,?,?) "
                         "ON CONFLICT(eid) DO UPDATE SET reasons=excluded.reasons, raw_json=excluded.raw_json",
                         (eid, ",".join(rejects), b[0], ts))
            stats["quarantined"] += 1
        else:
            conn.execute("DELETE FROM quarantine WHERE eid=?", (eid,))
            vals = [row[c] for c in SILVER_COLS]
            conn.execute(
                f"INSERT INTO incidents({','.join(SILVER_COLS)}, fetched_via, first_seen_at, last_seen_at) "
                f"VALUES({','.join('?' * len(SILVER_COLS))}, ?, ?, ?) ON CONFLICT(eid) DO UPDATE SET "
                + ",".join(f"{c}=excluded.{c}" for c in SILVER_COLS if c != "eid")
                + ", fetched_via=excluded.fetched_via, last_seen_at=excluded.last_seen_at",
                vals + [b[1], ts, ts],
            )
            stats["clean"] += 1
            stats["flagged"] += bool(flags)
        conn.execute("UPDATE bronze_incidents SET cleaned_at=? WHERE eid=?", (ts, eid))
    conn.commit()
    return stats


def clean_pending(conn) -> dict:
    """clean แถวใน bronze ที่ยังไม่ถูก clean (เช่น clean ตอน scrape ล้มกลางทาง)."""
    eids = [r[0] for r in conn.execute("SELECT eid FROM bronze_incidents WHERE cleaned_at IS NULL")]
    return clean_eids(conn, eids) if eids else {"clean": 0, "quarantined": 0, "flagged": 0}


def reclean_all(conn, chunk=5000) -> dict:
    """clean ใหม่ทั้งหมดจาก bronze (ใช้เมื่อแก้กฎ clean)."""
    types = load_types(conn)
    eids = [r[0] for r in conn.execute("SELECT eid FROM bronze_incidents ORDER BY eid")]
    total = {"clean": 0, "quarantined": 0, "flagged": 0}
    for i in range(0, len(eids), chunk):
        for k, v in clean_eids(conn, eids[i:i + chunk], types).items():
            total[k] += v
    return total


def mark_duplicates(conn) -> int:
    """ตั้ง is_duplicate; คืนจำนวนแถวที่เป็นรายงานซ้ำ."""
    conn.execute("DROP TABLE IF EXISTS temp.dups")
    conn.execute("""CREATE TEMP TABLE dups AS SELECT eid FROM (
        SELECT eid, ROW_NUMBER() OVER (PARTITION BY contributor, title, start_time,
               ROUND(latitude, 4), ROUND(longitude, 4) ORDER BY eid) rn FROM incidents) WHERE rn > 1""")
    conn.execute("UPDATE incidents SET is_duplicate=1 WHERE eid IN (SELECT eid FROM temp.dups) "
                 "AND is_duplicate IS NOT 1")
    conn.execute("UPDATE incidents SET is_duplicate=0 WHERE eid NOT IN (SELECT eid FROM temp.dups) "
                 "AND is_duplicate IS NOT 0")
    n = conn.execute("SELECT COUNT(*) FROM temp.dups").fetchone()[0]
    conn.execute("DROP TABLE temp.dups")
    conn.commit()
    return n
