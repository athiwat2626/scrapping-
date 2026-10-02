# BKK Traffic & Accident Pipeline (traffic.longdo.com)

เก็บข้อมูลเหตุการณ์จราจร/อุบัติเหตุ และดัชนีจราจรกรุงเทพฯ จาก [Longdo Traffic](https://traffic.longdo.com/)
ด้วย Apache Airflow (Docker) — มี checkpoint, ตัวเช็คความคืบหน้า และ pipeline ตรวจคุณภาพข้อมูล

## 1. แหล่งข้อมูล (สำรวจจาก JS ของเว็บ)

| Endpoint | ข้อมูล | ใช้ทำอะไร |
|---|---|---|
| `incident.json?page=N` | เหตุการณ์ 10 รายการ/หน้า เรียงใหม่→เก่า (~93,000 หน้า ≈ 9.3 แสนเหตุการณ์ ตั้งแต่ 2013) หน้า 1 = เหตุการณ์ที่ยัง active ทั้งหมด (~360) | realtime + backfill |
| `event.json?eid=X` | เหตุการณ์เดียวตาม id | เติม eid ที่ขาด |
| `api/json/traffic/index` | ดัชนีจราจร กทม. (ค่าปัจจุบัน, อัปเดตทุก 5 นาที) | สร้าง time series |
| `geteventtype` | ประเภทเหตุการณ์ (อุบัติเหตุ, รถเสีย, น้ำท่วม, …) | จัดประเภท |
| `api/raw/trafficindex/{ปี}` | **สภาพจราจรปกติ**: ดัชนีรถติดทุก 5 นาที 2012-ปัจจุบัน (ไฟล์ CSV รายปีที่ Longdo เปิดให้ดาวน์โหลด, ต้องอ้างแหล่งที่มา) | DAG `bkk_traffic_history` ทุกวัน 02:30 |
| `api.longdo.com/RouteService` (ต้องมี API key) | **เวลาเดินทางจุด A → B** ตามสภาพจราจรขณะนั้น | DAG `bkk_traffic_traveltime` ทุก 15 นาที |

ข้อจำกัด: `robots.txt` กำหนด **Crawl-delay: 10 วินาที** → client ใช้ file lock ร่วมกันทุก task ให้เว้น ≥10 วิ/request เสมอ
(ต่อวันดึงได้สูงสุด ~8,640 request). ข้อมูลเป็นของ Longdo/iTIC — ใช้ตามเงื่อนไขของผู้ให้บริการ

## 2. Pipeline: SCRAPE → CLEAN → VALIDATE → PUBLISH

```
 ┌──────────── 1. SCRAPE ────────────┐   ┌──────────── 2. CLEAN ─────────────┐   ┌──── 3. VALIDATE ────┐   ┌─ 4. PUBLISH ─┐
 │ realtime  ทุก 15 นาที             │   │ แปลงชนิด / ตัด HTML / เวลา        │   │ gate ก่อนปล่อย       │   │ Parquet      │
 │ backfill  ต่อเนื่อง (checkpoint)  │──▶│ ประเภทจาก type id / polygon กทม.  │──▶│ (ทุกนาที, G1-G8)     │──▶│ 10,000/ไฟล์  │
 │ recover   eid ที่ขาด (ทุกคืน)      │   │ กฎ R1-R7 → ผ่าน / quarantine      │   │ ตรวจละเอียด (ทุกคืน) │   │ CSV          │
 │ ≤ 1 req/10 วิ (robots.txt)        │   │ ตัดรายงานซ้ำ (is_duplicate)        │   │ FAIL = ไม่ปล่อย+แดง  │   │ (ไม่รวมซ้ำ)  │
 └───────────────┬───────────────────┘   └───────────────┬───────────────────┘   └──────────┬──────────┘   └──────────────┘
                 ▼                                       ▼                                  ▼
   BRONZE  bronze_incidents + data/raw     SILVER  incidents (+ quarantine)       data/quality/gate_last.json
           (raw JSON เฉพาะ กทม.)                                                  data/quality/dt=*/report.md
```

| ขั้น | โค้ด | DAG / task |
|---|---|---|
| SCRAPE | `scrape.py`, `client.py`, `storage.upsert_incidents` | `bkk_traffic_realtime`, `bkk_traffic_backfill`, `bkk_traffic_quality.recover_missing_eids` |
| CLEAN | `clean.py` (ทำทันทีหลัง scrape + เก็บตกทุกนาที) | `bkk_traffic_parquet.clean` |
| VALIDATE | `publish.gate_checks` (ทุกนาที), `quality.py` (ทุกคืน) | `bkk_traffic_parquet.validate_gate`, `bkk_traffic_quality.quality_checks` |
| PUBLISH | `publish.py` → `parquet_export.py`, `export.py` | `bkk_traffic_parquet.publish_parquet`, `bkk_traffic_quality.export_csv` |

### CLEAN: กฎรายแถว
| รหัส | กฎ | ไม่ผ่าน |
|---|---|---|
| R1 | มี `eid` | quarantine |
| R2 | อยู่ใน กทม. (polygon / ไม่มีพิกัดดูจากข้อความ → flag `R2_no_coords`) | quarantine |
| R3 | `start_time` แปลงเป็นวันเวลาได้ | quarantine |
| R4 | `stop_time` ≥ `start_time` | ตั้ง `stop_time`=NULL + flag |
| R5 | มีชื่อเหตุการณ์ | quarantine |
| R6 | `severity` อยู่ใน 1–3 | ตั้ง NULL + flag |
| R7 | `start_time` ไม่เลยวันนี้ไปเกิน 365 วัน | quarantine |

- แถวที่ถูกแก้ค่าจะมีคอลัมน์ `dq_flags` บอกไว้ (ยังถูก export) แถวที่ไม่ผ่านอยู่ในตาราง `quarantine` พร้อม `reasons` (ไม่ถูก export)
- **รายงานซ้ำ**: ผู้รายงาน + ชื่อ + เวลาเริ่ม + พิกัด ตรงกัน → เก็บ eid น้อยสุด ที่เหลือ `is_duplicate=1` (ไม่ถูก export)
- **หมายเหตุ API**: ฟิลด์ `severity` ของ Longdo คือ **id ประเภทเหตุการณ์** (3=Accident, 15=Fire, 6=Flood, …) → เก็บเป็น `event_type_id`;
  ความรุนแรงจริงคือ `severity_level` → เก็บเป็น `severity`
- แก้กฎแล้วต้องการ clean ใหม่ทั้งหมดจาก bronze (ไม่ต้องสแคปซ้ำ): `clean.reclean_all(conn)`

### VALIDATE: gate ก่อนปล่อย Parquet (ทุกนาที)
G1 schema ครบ · G2 มีข้อมูล · G3 แถวนอก กทม. = 0 · G4 eid/title/start_time ไม่ว่าง · G5 ไม่มีพิกัด ≤ 5% ·
G6 quarantine ≤ 5% · G7 จำนวนแถวไม่ลดต่ำกว่า 95% ของชุดก่อน · G8 bronze ค้าง clean < 1,000 (WARN)
→ ไม่ผ่าน = **ไม่เขียนทับ Parquet ชุดเดิม** + task แดง · ผลล่าสุด: `data/quality/gate_last.json` และใน `_manifest.json`

**เก็บเฉพาะกรุงเทพมหานคร**: API คืนข้อมูลทั้งประเทศ แต่ระบบเก็บเฉพาะเหตุการณ์ที่พิกัดอยู่ใน**ขอบเขตจริงของ กทม.**
(point-in-polygon กับ `plugins/bkk_traffic/bangkok_boundary.geojson`, ที่มา: chingchai/OpenGISData-Thailand)
ถ้าไม่มีพิกัด ใช้ชื่อจังหวัดที่อยู่ท้ายสุดในข้อความ (ตัด Cr./เครดิต ออก). เหตุการณ์นอก กทม. ไม่ถูกเก็บทั้งใน DB และไฟล์ raw
— เก็บแค่เลข `eid` ในตาราง `seen_eids` เพื่อให้ตรวจ eid ที่ขาด และรู้ว่าหน้าไหนดึงแล้ว

## 3. Checkpoint — ดึงไปถึงไหนแล้ว

- `checkpoints` table + ไฟล์ `data/checkpoints/backfill.json` : `page` หน้าถัดไป, `anchor_time` เวลาเก่าสุดที่ไปถึง,
  `pages_fetched_total`, `target_start_date`, `done`
- รายการในเว็บเลื่อนลงเรื่อยๆ เมื่อมีเหตุการณ์ใหม่ → ถ้าหน้าที่เปิดมี median start_time ใหม่กว่า `anchor_time` ถือว่าหน้าเลื่อน
  ให้ข้ามต่อไป (ข้อมูลซ้ำไม่เป็นปัญหาเพราะ upsert ด้วย eid) — ถ้า task ตายกลางทาง รอบถัดไปทำต่อจากหน้าล่าสุดที่บันทึก
- `data/checkpoints/realtime.json` : รอบ realtime ล่าสุด, max eid
- `data/checkpoints/progress.json` : สรุปรวม (อัปเดตทุกรอบ)

เช็คสถานะ:
```powershell
.\status.ps1
```
แสดงจำนวนเหตุการณ์ทั้งหมด / กทม. / อุบัติเหตุ กทม., ช่วงเวลา, % backfill, หน้าที่อยู่, เวลาที่เหลือโดยประมาณ, eid ที่ขาด, ผลรัน 24 ชม.

## 4. ตรวจข้อมูลขาด (bkk_traffic_quality)

| check | ตรวจอะไร | FAIL เมื่อ |
|---|---|---|
| freshness_hours | eid ใหม่ล่าสุดเข้ามานานแค่ไหน | > 12 ชม. |
| field_null_pct | % ค่าว่างรายฟิลด์ (พิกัด, เวลา, title, ประเภท Unknown) | พิกัด/เวลา ว่าง > 5% |
| days_without_incidents | วันที่ไม่มีเหตุการณ์เลยในช่วงที่ backfill แล้ว | มีอย่างน้อย 1 วัน |
| days_abnormally_low | วันที่จำนวน < 30% ของ median | (WARN) |
| traffic_index_coverage_pct | ช่วง 15 นาทีของเมื่อวานที่ขาด (คาด 96 ช่วง/วัน) | < 80% |
| missing_eids_pending | eid ที่เว้นช่วง (ตรวจแล้วดึงเติมด้วย `event.json?eid`) ถ้า API คืนว่าง = `not_available` | (WARN > 500) |
| non_bangkok_rows | มีแถวนอก กทม. หลุดเข้ามาไหม | > 0 |
| bangkok_by_text_only, stop_before_start, possible_duplicate_reports | ความถูกต้อง | (WARN) |
| failed_runs_24h | จำนวน task ที่ล้มเหลว | ≥ 5 |

ผลลัพธ์: `data/quality/dt=YYYY-MM-DD/report.md`, `report.json`, `missing_days.csv`, `missing_eids_pending.csv`

## 5. โครงสร้างโฟลเดอร์

```
scrap ข้อมูล/
├── docker-compose.yaml        Airflow 2.10 (LocalExecutor + Postgres)
├── .env                       user/password, crawl delay
├── status.ps1                 เช็คว่าดึงไปเท่าไหร่แล้ว
├── dags/
│   ├── bkk_traffic_realtime.py
│   ├── bkk_traffic_backfill.py
│   └── bkk_traffic_quality.py
├── plugins/bkk_traffic/       โค้ดหลัก (Airflow ใส่ plugins/ ใน PYTHONPATH ให้)
│   ├── config.py   client.py   storage.py   scrape.py
│   ├── quality.py  export.py   progress.py
├── scripts/status.py
├── data/
│   ├── raw/<source>/dt=YYYY-MM-DD/HHMMSS_*.json.gz   bronze: response ดิบ
│   ├── warehouse/bkk_traffic.db                      silver: SQLite clean
│   ├── processed/*.csv                               gold: incidents_bangkok, accidents_bangkok, traffic_index, ...
│   ├── parquet/<dataset>/part-00000.parquet ...      gold: Parquet ไฟล์ละ 50,000 แถว (เรียงตาม eid/ts) + _manifest.json
│   │                                                 dataset: incidents_bangkok, accidents_bangkok (เฉพาะที่เกี่ยวกับจราจร),
│   │                                                          traffic_index (ทุก 5 นาที ตั้งแต่ 2012), travel_times
│   │                                                          (DAG bkk_traffic_parquet ทุกนาที หลังผ่าน gate, ไม่รวมรายงานซ้ำ)
│   ├── quality/dt=YYYY-MM-DD/                        รายงานคุณภาพ
│   └── checkpoints/*.json                            checkpoint + progress
└── logs/                      Airflow logs
```

## 6. วิธีใช้

ติดตั้งครั้งแรก (ต้องมี Docker Desktop):
```powershell
git clone https://github.com/athiwat2626/scrapping-.git
cd scrapping-
copy .env.example .env        # แล้วแก้ AIRFLOW_ADMIN_PASSWORD
docker compose up -d
# (ถ้าต้องการเวลาเดินทาง) สมัคร key ฟรีที่ https://map.longdo.com/console แล้ว:
docker compose exec airflow-scheduler airflow variables set longdo_api_key <YOUR_KEY>
```
`data/` และ `logs/` ไม่อยู่ใน git — ระบบสร้างและดึงข้อมูลใหม่เองหลัง `docker compose up -d`

```powershell
docker compose up -d          # เปิด Airflow  -> http://localhost:8080  (user/รหัสตาม .env)
.\status.ps1                  # ดูความคืบหน้า
docker compose down           # ปิด (ข้อมูลอยู่ใน data/)
docker compose restart airflow-scheduler   # ต้องรันทุกครั้งที่แก้โค้ดใน plugins/ (scheduler cache โมดูลเก่าไว้)
```

ปรับค่าได้ที่ Airflow UI → Admin → Variables:

| Variable | default | ความหมาย |
|---|---|---|
| `bkk_backfill_start_date` | 2025-01-01 | ย้อนหลังถึงวันไหน (เปลี่ยนให้เก่ากว่านี้แล้ว backfill จะทำต่อ) |
| `bkk_backfill_pages_per_run` | 80 | หน้า/รอบ backfill |
| `bkk_realtime_max_pages` | 30 | หน้าสูงสุดต่อรอบ realtime |
| `bkk_recover_per_run` | 150 | eid ที่ขาดที่จะลองดึงต่อวัน |

ประมาณเวลา backfill: ~42 หน้า/วันของข้อมูล → ย้อนถึง 2025-01-01 ≈ 27,000 หน้า ≈ 4–6 วัน (ด้วย 10 วิ/request)
