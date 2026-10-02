"""ตรวจคุณภาพข้อมูล: อะไรขาด (วัน/ช่วงเวลา/eid/ฟิลด์), อะไรผิดปกติ -> report.json + report.md."""
import csv
import json
from datetime import date, datetime, timedelta

from bkk_traffic import config, storage

OK, WARN, FAIL = "OK", "WARN", "FAIL"
KEY_FIELDS = ("title", "latitude", "longitude", "start_time", "stop_time", "severity", "event_type")
EXPECTED_SNAPSHOTS_PER_DAY = 96   # realtime DAG ทุก 15 นาที


def _check(name, status, value, detail=""):
    return {"check": name, "status": status, "value": value, "detail": detail}


def run_quality_checks(report_day: date | None = None) -> dict:
    report_day = report_day or (date.today() - timedelta(days=1))
    day_s = report_day.isoformat()
    checks, extras = [], {}
    with storage.connect() as conn:
        q = lambda sql, *a: conn.execute(sql, a).fetchone()[0]  # noqa: E731
        total = q("SELECT COUNT(*) FROM incidents")
        if total == 0:
            checks.append(_check("has_data", FAIL, 0, "ยังไม่มีข้อมูลเหตุการณ์"))
            return _write_report(day_s, checks, extras)

        # 1) ความสดของข้อมูล - วัดจาก realtime เท่านั้น (backfill ใส่ของเก่าตลอด ใช้ first_seen_at วัดไม่ได้)
        now = datetime.now()
        rt_last = storage.get_checkpoint(conn, "realtime").get("last_success")
        rt_age = (now - datetime.fromisoformat(rt_last)).total_seconds() / 3600 if rt_last else 999
        checks.append(_check("realtime_last_success_hours", OK if rt_age < 1 else WARN if rt_age < 3 else FAIL,
                             round(rt_age, 2), f"realtime สำเร็จล่าสุด {rt_last}"))
        newest = q("SELECT MAX(createtime) FROM incidents WHERE createtime <= ?", now.strftime("%Y-%m-%d %H:%M:%S"))
        age_h = (now - datetime.fromisoformat(newest)).total_seconds() / 3600 if newest else 999
        checks.append(_check("newest_event_age_hours", OK if age_h < 3 else WARN if age_h < 12 else FAIL,
                             round(age_h, 2), f"เหตุการณ์ กทม. ล่าสุดสร้างเมื่อ {newest}"))
        last_seen = newest or now.strftime("%Y-%m-%d")

        # 2) ความครบของฟิลด์ (% null)
        completeness = {}
        for f in KEY_FIELDS:
            nulls = q(f"SELECT COUNT(*) FROM incidents WHERE {f} IS NULL OR {f}=''")
            completeness[f] = round(100 * nulls / total, 2)
        unknown_type = round(100 * q("SELECT COUNT(*) FROM incidents WHERE event_type='Unknown'") / total, 2)
        completeness["event_type_unknown"] = unknown_type
        worst = max(v for k, v in completeness.items() if k in ("latitude", "longitude", "start_time"))
        checks.append(_check("field_null_pct", OK if worst < 1 else WARN if worst < 5 else FAIL,
                             completeness, "% ค่าว่างต่อฟิลด์ (ทั้งหมด)"))

        # 3) ความถูกต้อง
        not_bkk = q("SELECT COUNT(*) FROM incidents WHERE is_bangkok=0")
        checks.append(_check("non_bangkok_rows", OK if not_bkk == 0 else FAIL, not_bkk,
                             "ต้องเป็น 0 - warehouse เก็บเฉพาะ กทม."))
        text_only = q("SELECT COUNT(*) FROM incidents WHERE province_guess='bangkok(text)'")
        checks.append(_check("bangkok_by_text_only", OK if text_only < 0.05 * total else WARN, text_only,
                             "ไม่มีพิกัด ตัดสินว่าเป็น กทม. จากข้อความ - ควรสุ่มตรวจ"))
        # ผลของขั้น clean
        flags = {}
        for (f,) in conn.execute("SELECT dq_flags FROM incidents WHERE dq_flags IS NOT NULL"):
            for name in f.split(","):
                flags[name] = flags.get(name, 0) + 1
        checks.append(_check("clean_rule_flags", OK, flags, "แถวที่ clean แก้ค่าให้ (ยังถูก export)"))
        reasons = {}
        for (r,) in conn.execute("SELECT reasons FROM quarantine"):
            for name in r.split(","):
                reasons[name] = reasons.get(name, 0) + 1
        n_q = q("SELECT COUNT(*) FROM quarantine")
        q_pct = 100 * n_q / (total + n_q)
        checks.append(_check("quarantine_rows", OK if n_q == 0 else WARN if q_pct < 5 else FAIL,
                             {"rows": n_q, **reasons}, "ไม่ผ่านกฎรายแถว - ไม่ถูก export (ดู quarantine_rows.csv)"))
        dup = q("SELECT COUNT(*) FROM incidents WHERE is_duplicate=1")
        checks.append(_check("duplicate_reports_removed", OK, dup, "รายงานซ้ำ ถูกตัดออกจาก Parquet/CSV แล้ว"))
        by_source = {r[0]: r[1] for r in conn.execute("SELECT fetched_via, COUNT(*) FROM incidents GROUP BY 1")}
        checks.append(_check("rows_by_source", OK, by_source,
                             "feed = จากไฟล์รายปี (ไม่มี severity/createtime) จะถูกแทนด้วย API เมื่อ backfill ไปถึง"))
        pending = q("SELECT COUNT(*) FROM bronze_incidents WHERE cleaned_at IS NULL")
        checks.append(_check("bronze_not_cleaned", OK if pending < 1000 else WARN, pending))
        extras["quarantine_sample"] = [dict(r) for r in conn.execute(
            "SELECT eid, reasons, detected_at FROM quarantine ORDER BY eid DESC LIMIT 1000")]

        # 4) วันที่ขาดหาย / จำนวนต่อวันผิดปกติ (ในช่วงที่ backfill ครอบคลุมแล้ว)
        bf = storage.get_checkpoint(conn, "backfill")
        start = (bf.get("anchor_time") or last_seen)[:10]
        daily = {r[0]: r[1] for r in conn.execute(
            "SELECT substr(start_time,1,10) d, COUNT(*) FROM incidents WHERE start_time >= ? "
            "AND start_time < date('now','localtime','+1 day') GROUP BY d", (start,))}
        d0, d1 = date.fromisoformat(start) + timedelta(days=1), date.today() - timedelta(days=1)
        all_days = [(d0 + timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]
        missing_days = [d for d in all_days if daily.get(d, 0) == 0]
        counts = sorted(daily.get(d, 0) for d in all_days) or [0]
        median = counts[len(counts) // 2]
        low_days = [d for d in all_days if 0 < daily.get(d, 0) < 0.3 * median]
        checks.append(_check("days_without_incidents", OK if not missing_days else FAIL, len(missing_days),
                             f"ช่วง {d0}..{d1}, ดู missing_days.csv"))
        checks.append(_check("days_abnormally_low", OK if not low_days else WARN, len(low_days),
                             f"< 30% ของ median ({median}/วัน)"))
        extras["missing_days"] = missing_days
        extras["low_days"] = low_days
        extras["daily_counts_last_14"] = {d: daily.get(d, 0) for d in all_days[-14:]}

        # 5) time series traffic index ของวันที่รายงาน: ช่องเวลา 15 นาทีที่ขาด
        for table in ("traffic_index", "travel_times"):
            slots = {r[0] for r in conn.execute(
                f"SELECT DISTINCT substr(fetched_at,12,2)*4 + substr(fetched_at,15,2)/15 FROM {table} "
                f"WHERE substr(fetched_at,1,10)=?", (day_s,))}
            # นับเฉพาะช่วงหลังเริ่มเก็บครั้งแรก (วันแรกที่เริ่มเก็บไม่ควรโดนนับว่าขาดตั้งแต่เที่ยงคืน)
            first = q(f"SELECT MIN(fetched_at) FROM {table}") or "9999"
            first_slot = 0
            if first[:10] == day_s:
                first_slot = int(first[11:13]) * 4 + int(first[14:16]) // 15
            elif first[:10] > day_s:
                first_slot = EXPECTED_SNAPSHOTS_PER_DAY
            expected = range(first_slot, EXPECTED_SNAPSHOTS_PER_DAY)
            missing_slots = [f"{s // 4:02d}:{(s % 4) * 15:02d}" for s in expected if s not in slots]
            if not expected:
                checks.append(_check(f"{table}_coverage_pct", OK, None, f"{day_s}: ยังไม่เริ่มเก็บ"))
                continue
            pct = round(100 * (len(expected) - len(missing_slots)) / len(expected), 1)
            checks.append(_check(f"{table}_coverage_pct", OK if pct >= 95 else WARN if pct >= 80 else FAIL,
                                 pct, f"{day_s}: ขาด {len(missing_slots)} ช่วง"))
            extras[f"{table}_missing_slots"] = missing_slots

        # 5.5) สภาพจราจรปกติ (ดัชนีทุก 5 นาที = 288 จุด/วัน): วันที่ข้อมูลไม่ครบ
        days = conn.execute("SELECT substr(ts_local,1,10) d, COUNT(*) FROM traffic_index_history "
                            "GROUP BY d ORDER BY d").fetchall()
        if days:
            incomplete = [d for d, n in days[:-1] if n < 288 * 0.9]  # ไม่นับวันล่าสุด (ยังไม่จบวัน)
            checks.append(_check("traffic_index_5min_incomplete_days",
                                 OK if len(incomplete) <= 0.02 * len(days) else WARN, len(incomplete),
                                 f"จาก {len(days):,} วัน ({days[0][0]}..{days[-1][0]}) วันที่มี < 90% ของ 288 จุด"))
            extras["traffic_index_incomplete_days"] = incomplete[:500]

        # 6) eid ที่ขาด
        miss = {r[0]: r[1] for r in conn.execute("SELECT status, COUNT(*) FROM missing_eids GROUP BY status")}
        checks.append(_check("missing_eids_pending", OK if miss.get("pending", 0) < 500 else WARN,
                             miss.get("pending", 0), json.dumps(miss)))

        # 6.5) ความผิดปกติ: วันที่รายงาน เทียบ 7 วันก่อนหน้า
        prev = [(report_day - timedelta(days=i)).isoformat() for i in range(1, 8)]
        day_n = q("SELECT COUNT(*) FROM incidents WHERE substr(start_time,1,10)=? AND is_duplicate=0", day_s)
        day_acc = q("SELECT COUNT(*) FROM incidents WHERE substr(start_time,1,10)=? AND is_duplicate=0 "
                    "AND is_accident=1", day_s)
        prev_n = q(f"SELECT COUNT(*) FROM incidents WHERE substr(start_time,1,10) IN ({','.join('?' * 7)}) "
                   "AND is_duplicate=0", *prev)
        prev_acc = q(f"SELECT COUNT(*) FROM incidents WHERE substr(start_time,1,10) IN ({','.join('?' * 7)}) "
                     "AND is_duplicate=0 AND is_accident=1", *prev)
        if prev_n and day_n:
            share, base = day_acc / day_n, prev_acc / prev_n
            ok = not base or 0.5 <= share / base <= 2
            checks.append(_check("accident_share_vs_7d", OK if ok else WARN,
                                 {"day": round(100 * share, 1), "prev_7d": round(100 * base, 1)},
                                 "% อุบัติเหตุ เทียบค่าเฉลี่ย 7 วัน (ต่างเกิน 2 เท่า = WARN)"))
            avg = prev_n / 7
            checks.append(_check("volume_vs_7d", OK if 0.3 * avg <= day_n <= 5 * avg else WARN,
                                 {"day": day_n, "avg_7d": round(avg, 1)}, "จำนวนเหตุการณ์ (ช่วงปกติ 0.3x-5x)"))

        # 6.6) ไฟล์ gold ที่ปล่อยออกไปจริง: schema + จำนวนแถวตรง manifest
        checks.append(_check_parquet_files())

        # 7) งานที่ล้มเหลว 24 ชม.
        failed = q("SELECT COUNT(*) FROM scrape_runs WHERE status='failed' "
                   "AND started_at >= datetime('now','localtime','-1 day')")
        checks.append(_check("failed_runs_24h", OK if failed == 0 else WARN if failed < 5 else FAIL, failed))

        extras["pending_eids_sample"] = [r[0] for r in conn.execute(
            "SELECT eid FROM missing_eids WHERE status='pending' ORDER BY eid DESC LIMIT 1000")]
    return _write_report(day_s, checks, extras)


def _check_parquet_files():
    import pyarrow.parquet as pq

    from bkk_traffic import parquet_export
    problems = []
    for name, (_, schema) in parquet_export.DATASETS.items():
        folder = parquet_export.PARQUET_DIR / name
        try:
            manifest = json.loads((folder / "_manifest.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            problems.append(f"{name}: ไม่มี manifest")
            continue
        rows = 0
        for f in manifest["files"]:
            try:
                meta = pq.read_metadata(folder / f["file"])
            except OSError as err:
                problems.append(f"{name}/{f['file']}: อ่านไม่ได้ ({err})")
                continue
            rows += meta.num_rows
            if not meta.schema.to_arrow_schema().equals(schema):
                problems.append(f"{name}/{f['file']}: schema ไม่ตรง")
            if meta.num_rows > parquet_export.ROWS_PER_FILE:
                problems.append(f"{name}/{f['file']}: เกิน {parquet_export.ROWS_PER_FILE} แถว")
        if rows != manifest["total_rows"]:
            problems.append(f"{name}: แถวในไฟล์ {rows} != manifest {manifest['total_rows']}")
    return _check("parquet_files_valid", OK if not problems else FAIL, len(problems),
                  "; ".join(problems[:5]) or "schema/จำนวนแถว ตรงกับ manifest")


def _write_report(day_s, checks, extras):
    out = config.QUALITY_DIR / f"dt={day_s}"
    out.mkdir(parents=True, exist_ok=True)
    overall = FAIL if any(c["status"] == FAIL for c in checks) else WARN if any(
        c["status"] == WARN for c in checks) else OK
    report = {"report_day": day_s, "generated_at": storage.now_str(), "overall": overall, "checks": checks,
              **{k: v for k, v in extras.items() if k not in ("pending_eids_sample", "quarantine_sample")}}
    with open(out / "quarantine_rows.csv", "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(["eid", "reasons", "detected_at"])
        w.writerows([(r["eid"], r["reasons"], r["detected_at"]) for r in extras.get("quarantine_sample", [])])
    (out / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    with open(out / "missing_days.csv", "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(["date", "problem"])
        w.writerows([(d, "no_incidents") for d in extras.get("missing_days", [])])
        w.writerows([(d, "abnormally_low") for d in extras.get("low_days", [])])
    with open(out / "missing_eids_pending.csv", "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(["eid"])
        w.writerows([(e,) for e in extras.get("pending_eids_sample", [])])

    icon = {OK: "✅", WARN: "⚠️", FAIL: "❌"}
    lines = [f"# Data Quality Report {day_s}", "", f"Overall: **{icon[overall]} {overall}**", "",
             "| check | status | value | detail |", "|---|---|---|---|"]
    for c in checks:
        val = json.dumps(c["value"], ensure_ascii=False) if isinstance(c["value"], dict) else c["value"]
        lines.append(f"| {c['check']} | {icon[c['status']]} {c['status']} | {val} | {c['detail']} |")
    (out / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report
