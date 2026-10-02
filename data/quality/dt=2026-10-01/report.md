# Data Quality Report 2026-10-01

Overall: **⚠️ WARN**

| check | status | value | detail |
|---|---|---|---|
| realtime_last_success_hours | ✅ OK | 0.14 | realtime สำเร็จล่าสุด 2026-10-02 10:15:43 |
| newest_event_age_hours | ✅ OK | 0.22 | เหตุการณ์ กทม. ล่าสุดสร้างเมื่อ 2026-10-02 10:10:42 |
| field_null_pct | ✅ OK | {"title": 0.0, "latitude": 0.0, "longitude": 0.0, "start_time": 0.0, "stop_time": 0.0, "severity": 38.63, "event_type": 0.0, "event_type_unknown": 0.0} | % ค่าว่างต่อฟิลด์ (ทั้งหมด) |
| non_bangkok_rows | ✅ OK | 0 | ต้องเป็น 0 - warehouse เก็บเฉพาะ กทม. |
| bangkok_by_text_only | ✅ OK | 167 | ไม่มีพิกัด ตัดสินว่าเป็น กทม. จากข้อความ - ควรสุ่มตรวจ |
| clean_rule_flags | ✅ OK | {"R4_stop_before_start": 5, "R6_bad_severity": 15} | แถวที่ clean แก้ค่าให้ (ยังถูก export) |
| quarantine_rows | ⚠️ WARN | {"rows": 1, "R5_no_title": 1} | ไม่ผ่านกฎรายแถว - ไม่ถูก export (ดู quarantine_rows.csv) |
| duplicate_reports_removed | ✅ OK | 1255 | รายงานซ้ำ ถูกตัดออกจาก Parquet/CSV แล้ว |
| rows_by_source | ✅ OK | {"backfill": 275965, "feed": 164938, "realtime": 298, "recover": 13} | feed = จากไฟล์รายปี (ไม่มี severity/createtime) จะถูกแทนด้วย API เมื่อ backfill ไปถึง |
| bronze_not_cleaned | ✅ OK | 0 |  |
| days_without_incidents | ✅ OK | 0 | ช่วง 2015-12-30..2026-10-01, ดู missing_days.csv |
| days_abnormally_low | ⚠️ WARN | 26 | < 30% ของ median (66/วัน) |
| traffic_index_coverage_pct | ✅ OK | 99.0 | 2026-10-01: ขาด 1 ช่วง |
| travel_times_coverage_pct | ✅ OK | None | 2026-10-01: ยังไม่เริ่มเก็บ |
| traffic_index_5min_incomplete_days | ⚠️ WARN | 308 | จาก 5,049 วัน (2012-01-01..2026-10-02) วันที่มี < 90% ของ 288 จุด |
| missing_eids_pending | ⚠️ WARN | 1454 | {"not_available": 303, "pending": 1454, "recovered": 283} |
| accident_share_vs_7d | ⚠️ WARN | {"day": 23.2, "prev_7d": 7.1} | % อุบัติเหตุ เทียบค่าเฉลี่ย 7 วัน (ต่างเกิน 2 เท่า = WARN) |
| volume_vs_7d | ⚠️ WARN | {"day": 69, "avg_7d": 405.0} | จำนวนเหตุการณ์ (ช่วงปกติ 0.3x-5x) |
| parquet_files_valid | ✅ OK | 0 | schema/จำนวนแถว ตรงกับ manifest |
| failed_runs_24h | ✅ OK | 0 |  |
