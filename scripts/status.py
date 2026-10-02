"""เช็คว่าดึงข้อมูลไปแล้วเท่าไหร่ / checkpoint อยู่ตรงไหน.

รันใน container:  docker compose exec airflow-scheduler python /opt/airflow/scripts/status.py
หรือบน Windows:    .\\status.ps1
"""
import json
import sys

sys.path.insert(0, "/opt/airflow/plugins")
from bkk_traffic import progress  # noqa: E402

s = progress.write_progress_file()
bp = s.get("backfill_progress") or {}
rt = s.get("realtime_checkpoint") or {}

print("=" * 64)
print(f" BKK Traffic scrape status  ({s['generated_at']})")
print("=" * 64)
print(f" เหตุการณ์ที่สแคปผ่าน (ทั้งปท.)  : {s['eids_seen_all']:,}  (เก็บแค่ eid)")
print(f" เหตุการณ์ในกรุงเทพฯ (เก็บ)   : {s['incidents_bangkok']:,}")
print(f" อุบัติเหตุในกรุงเทพฯ         : {s['accidents_bangkok']:,}")
print(f" ช่วงเวลา start_time         : {s['start_time_range'][0]}  ->  {s['start_time_range'][1]}")
print(f" ช่วง eid (กทม.)             : {s['eid_range'][0]}  ->  {s['eid_range'][1]}")
print(f" Traffic index (จุดเวลา)     : {s['traffic_index_points']:,}")
print(f" Requests ทั้งหมด             : {s['requests_total']:,}")
print("-" * 64)
print(" [Realtime checkpoint]")
print(f"   last success : {rt.get('last_success')}   max_eid={rt.get('max_eid')}"
      f"   new last run={rt.get('new_last_run')}")
print(" [Backfill checkpoint]")
if bp:
    print(f"   progress     : {bp['percent']}%  (ย้อนถึง {bp['reached_back_to']} / เป้าหมาย {bp['target']})")
    print(f"   page         : {bp['current_page']:,} / {bp['source_total_pages'] or '?'} หน้าทั้งเว็บ")
    print(f"   เหลือประมาณ  : {bp['est_pages_left'] or 0:,} หน้า ~ {bp['est_hours_left']} ชม. (ไม่รวมเวลาแชร์ rate limit)")
    print(f"   done         : {bp['done']}")
else:
    print("   ยังไม่เริ่ม")
print(" [Missing eids]  ", json.dumps(s["missing_eids"]))
print(" [Runs 24h]      ", json.dumps(s["runs_last_24h"]))
print(" [BKK by type]   ", json.dumps(s["by_type"], ensure_ascii=False))
print("=" * 64)
print(" รายละเอียดเต็ม: data/checkpoints/progress.json")
