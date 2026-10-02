# Data Quality Report 2026-09-29

Overall: **⚠️ WARN**

| check | status | value | detail |
|---|---|---|---|
| realtime_last_success_hours | ✅ OK | 0.07 | realtime สำเร็จล่าสุด 2026-09-30 18:31:12 |
| newest_event_age_hours | ✅ OK | 0.09 | เหตุการณ์ กทม. ล่าสุดสร้างเมื่อ 2026-09-30 18:29:51 |
| field_null_pct | ✅ OK | {"title": 0.0, "latitude": 0.0, "longitude": 0.0, "start_time": 0.0, "stop_time": 0.0, "severity": 0.0, "event_type": 0.0, "event_type_unknown": 4.34} | % ค่าว่างต่อฟิลด์ (ทั้งหมด) |
| non_bangkok_rows | ✅ OK | 0 | ต้องเป็น 0 - warehouse เก็บเฉพาะ กทม. |
| bangkok_by_text_only | ✅ OK | 0 | ไม่มีพิกัด ตัดสินว่าเป็น กทม. จากข้อความ - ควรสุ่มตรวจ |
| stop_before_start | ⚠️ WARN | 1 |  |
| possible_duplicate_reports | ⚠️ WARN | 431 | title+เวลา+พิกัด ซ้ำกันแต่ eid ต่างกัน |
| days_without_incidents | ✅ OK | 0 | ช่วง 2025-06-05..2026-09-29, ดู missing_days.csv |
| days_abnormally_low | ✅ OK | 0 | < 30% ของ median (81/วัน) |
| traffic_index_coverage_pct | ✅ OK | None | 2026-09-29: ยังไม่เริ่มเก็บ |
| missing_eids_pending | ✅ OK | 313 | {"not_available": 162, "pending": 313, "recovered": 19} |
| failed_runs_24h | ✅ OK | 0 |  |
