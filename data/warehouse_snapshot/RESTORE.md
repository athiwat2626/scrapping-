# Warehouse snapshot (SQLite) — bkk_traffic.db

สำเนาฐานข้อมูล `data/warehouse/bkk_traffic.db` ณ **2026-10-03 00:23:50** (สร้างด้วย SQLite backup API ขณะระบบรันอยู่, `PRAGMA integrity_check = ok`)
บีบอัด gzip แล้วแบ่งไฟล์ละ 95 MB เพราะ GitHub รับไม่เกิน 100 MB/ไฟล์

| | ขนาด | SHA-256 |
|---|---:|---|
| `bkk_traffic.db` (หลังรวม+แตกไฟล์) | 1,440.3 MB | `3898558fd9a5623e28e984f00aee782923071e0c2e0926a221a1f6ebfcde7317` |
| `bkk_traffic.db.gz` (หลังรวมไฟล์) | 151.6 MB | `d70ac54af85146a18aafe037f8a41f6c8268cdc5ea3acf80900a3256cfe257ba` |

ค่าทั้งหมด (รวมแต่ละ part) อยู่ใน `SHA256SUMS` — ทดสอบรวม/แตกไฟล์กลับแล้ว SHA-256 ตรงกับต้นฉบับ

จำนวนแถว: incidents 486,160 · bronze_incidents 486,168 · quarantine 8 · traffic_index_history 1,414,013 · travel_times 1,127 · seen_eids 926,627

## วิธีรวมไฟล์กลับ

**Windows (PowerShell)**
```powershell
cmd /c copy /b bkk_traffic.db.gz.part-000+bkk_traffic.db.gz.part-001 bkk_traffic.db.gz
$in = [IO.File]::OpenRead("$PWD\bkk_traffic.db.gz"); $gz = New-Object IO.Compression.GZipStream($in, [IO.Compression.CompressionMode]::Decompress)
$out = [IO.File]::Create("$PWD\bkk_traffic.db"); $gz.CopyTo($out); $out.Close(); $gz.Close(); $in.Close()
(Get-FileHash bkk_traffic.db -Algorithm SHA256).Hash   # ต้องตรงกับตารางด้านบน
```

**Linux / macOS / Git Bash**
```bash
cat bkk_traffic.db.gz.part-* | gunzip > bkk_traffic.db
sha256sum bkk_traffic.db          # ต้องตรงกับ SHA256SUMS
```

จะใช้แทนฐานข้อมูลของระบบ: หยุด Airflow (`docker compose down`) แล้ววางไฟล์ที่ `data/warehouse/bkk_traffic.db`

ที่มาข้อมูล: Longdo Traffic (https://traffic.longdo.com)
