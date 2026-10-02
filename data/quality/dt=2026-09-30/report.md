# Data Quality Report 2026-09-30

Overall: **⚠️ WARN**

| check | status | value | detail |
|---|---|---|---|
| realtime_last_success_hours | ✅ OK | 0.17 | realtime สำเร็จล่าสุด 2026-10-01 01:16:02 |
| newest_event_age_hours | ✅ OK | 0.27 | เหตุการณ์ กทม. ล่าสุดสร้างเมื่อ 2026-10-01 01:10:06 |
| field_null_pct | ✅ OK | {"title": 0.0, "latitude": 0.0, "longitude": 0.0, "start_time": 0.0, "stop_time": 0.0, "severity": 0.0, "event_type": 0.0, "event_type_unknown": 4.15} | % ค่าว่างต่อฟิลด์ (ทั้งหมด) |
| non_bangkok_rows | ✅ OK | 0 | ต้องเป็น 0 - warehouse เก็บเฉพาะ กทม. |
| bangkok_by_text_only | ✅ OK | 0 | ไม่มีพิกัด ตัดสินว่าเป็น กทม. จากข้อความ - ควรสุ่มตรวจ |
| stop_before_start | ⚠️ WARN | 1 |  |
| possible_duplicate_reports | ⚠️ WARN | 513 | title+เวลา+พิกัด ซ้ำกันแต่ eid ต่างกัน |
| days_without_incidents | ✅ OK | 0 | ช่วง 2025-02-28..2026-09-30, ดู missing_days.csv |
| days_abnormally_low | ✅ OK | 0 | < 30% ของ median (80/วัน) |
| traffic_index_coverage_pct | ✅ OK | 97.6 | 2026-09-30: ขาด 1 ช่วง |
| missing_eids_pending | ✅ OK | 228 | {"not_available": 311, "pending": 228, "recovered": 20} |
| failed_runs_24h | ✅ OK | 0 |  |
