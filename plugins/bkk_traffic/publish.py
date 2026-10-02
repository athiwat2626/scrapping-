"""ขั้น VALIDATE (gate) + PUBLISH: ตรวจ silver ก่อนปล่อย gold (Parquet/CSV).

ถ้า gate ไม่ผ่าน -> ไม่เขียนไฟล์ใหม่ (ไฟล์ชุดล่าสุดที่ผ่านยังอยู่) และ task ขึ้นสีแดงใน Airflow
ผลการตรวจล่าสุดเขียนไว้ที่ data/quality/gate_last.json และแนบใน _manifest.json ของ Parquet
"""
import json

from bkk_traffic import clean, config, storage

PASS, FAIL, WARN = "PASS", "FAIL", "WARN"
EXPORTABLE = "FROM incidents WHERE is_bangkok=1 AND is_duplicate=0"
GATE_FILE = config.QUALITY_DIR / "gate_last.json"
MAX_DROP_RATIO = 0.95        # จำนวนแถวที่จะปล่อยต้องไม่ลดลงต่ำกว่า 95% ของชุดก่อน
MAX_NO_COORDS_PCT = 5.0
MAX_QUARANTINE_PCT = 5.0


def _prev_published_rows():
    try:
        m = json.loads((config.DATA_DIR / "parquet" / "incidents_bangkok" / "_manifest.json").read_text("utf-8"))
        return m.get("total_rows")
    except (OSError, ValueError):
        return None


def run_clean_stage() -> dict:
    with storage.connect() as conn:
        result = clean.clean_pending(conn)
        result["duplicates"] = clean.mark_duplicates(conn)
    return result


def gate_checks(allow_drop: bool = False) -> dict:
    """allow_drop=True ใช้เฉพาะตอนตั้งใจลดข้อมูล (เช่น migration คัดประเภทที่ไม่เกี่ยวออก)."""
    checks = []

    def add(name, ok, value, detail="", warn_only=False):
        checks.append({"check": name, "status": PASS if ok else (WARN if warn_only else FAIL),
                       "value": value, "detail": detail})

    with storage.connect() as conn:
        q = lambda sql, *a: conn.execute(sql, a).fetchone()[0]  # noqa: E731
        have = {r[1] for r in conn.execute("PRAGMA table_info(incidents)")}
        missing_cols = sorted((set(clean.SILVER_COLS) | {"is_duplicate", "traffic_relevance"}) - have)
        add("G1_schema", not missing_cols, missing_cols, "คอลัมน์ที่ต้องมีครบ")

        rows = q(f"SELECT COUNT(*) {EXPORTABLE}")
        add("G2_has_rows", rows > 0, rows)
        add("G3_non_bangkok_rows", q("SELECT COUNT(*) FROM incidents WHERE is_bangkok!=1") == 0,
            q("SELECT COUNT(*) FROM incidents WHERE is_bangkok!=1"))
        nulls = q(f"SELECT COUNT(*) {EXPORTABLE} AND (title IS NULL OR start_time IS NULL OR eid IS NULL)")
        add("G4_required_not_null", nulls == 0, nulls, "eid/title/start_time ต้องไม่ว่าง")
        no_coords = round(100 * q(f"SELECT COUNT(*) {EXPORTABLE} AND latitude IS NULL") / rows, 2) if rows else 0
        add("G5_no_coords_pct", no_coords <= MAX_NO_COORDS_PCT, no_coords, f"<= {MAX_NO_COORDS_PCT}%")
        quarantined = q("SELECT COUNT(*) FROM quarantine")
        q_pct = round(100 * quarantined / (rows + quarantined), 2) if rows + quarantined else 0
        add("G6_quarantine_pct", q_pct <= MAX_QUARANTINE_PCT, q_pct, f"{quarantined} แถว, <= {MAX_QUARANTINE_PCT}%")
        prev = _prev_published_rows()
        add("G7_no_big_drop", allow_drop or not prev or rows >= prev * MAX_DROP_RATIO, {"now": rows, "prev": prev},
            f"ต้อง >= {int(MAX_DROP_RATIO * 100)}% ของชุดก่อน (กันข้อมูลหายแล้วเผลอปล่อย)"
            + (" [allow_drop]" if allow_drop else ""))
        non_traffic = q("SELECT COUNT(*) FROM incidents WHERE traffic_relevance IS NULL")
        add("G9_non_traffic_rows", non_traffic == 0, non_traffic, "ต้องเป็น 0 - เก็บเฉพาะที่เกี่ยวกับจราจร")
        pending = q("SELECT COUNT(*) FROM bronze_incidents WHERE cleaned_at IS NULL")
        add("G8_clean_backlog", pending < 1000, pending, "bronze ที่ยังไม่ clean", warn_only=True)

    result = {"generated_at": storage.now_str(), "passed": all(c["status"] != FAIL for c in checks),
              "checks": checks}
    config.QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    GATE_FILE.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


class GateFailed(RuntimeError):
    pass


def _require_gate() -> dict:
    gate = gate_checks()
    if not gate["passed"]:
        bad = [f"{c['check']}={c['value']}" for c in gate["checks"] if c["status"] == FAIL]
        raise GateFailed("validation gate FAIL -> ไม่ปล่อยไฟล์ใหม่: " + ", ".join(bad))
    return gate


def publish_parquet(gate: dict | None = None) -> dict:
    from bkk_traffic import parquet_export
    gate = gate or _require_gate()
    return parquet_export.export_all(validation={"gate_passed_at": gate["generated_at"],
                                                 "gate": {c["check"]: c["status"] for c in gate["checks"]}})


def publish_csv() -> dict:
    from bkk_traffic import export
    _require_gate()
    return export.export_all()
