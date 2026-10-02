"""ค่าตั้งค่ากลางของ pipeline (override ได้ด้วย environment variable)."""
import os
from pathlib import Path

BASE_URL = "https://traffic.longdo.com"
INCIDENT_URL = f"{BASE_URL}/incident.json"          # ?page=N  (10 รายการ/หน้า, เรียงใหม่ -> เก่า)
EVENT_URL = f"{BASE_URL}/event.json"                # ?eid=X   (ดึงเหตุการณ์เดียวตาม id)
TRAFFIC_INDEX_URL = f"{BASE_URL}/api/json/traffic/index"
EVENT_TYPE_URL = f"{BASE_URL}/geteventtype"

# robots.txt ของ traffic.longdo.com กำหนด Crawl-delay: 10
CRAWL_DELAY_SEC = float(os.environ.get("BKK_CRAWL_DELAY", "10"))
REQUEST_TIMEOUT_SEC = 180   # หน้าลึกๆ เซิร์ฟเวอร์ตอบช้า (วัดได้ถึง ~100 วิ)
MAX_RETRIES = 4
USER_AGENT = os.environ.get(
    "BKK_USER_AGENT",
    "bkk-traffic-research/1.0 (academic data collection; respects robots.txt crawl-delay)",
)

DATA_DIR = Path(os.environ.get("BKK_DATA_DIR", "/opt/airflow/data"))
RAW_DIR = DATA_DIR / "raw"
WAREHOUSE_DB = DATA_DIR / "warehouse" / "bkk_traffic.db"
PROCESSED_DIR = DATA_DIR / "processed"
QUALITY_DIR = DATA_DIR / "quality"
CHECKPOINT_DIR = DATA_DIR / "checkpoints"
RATE_LOCK_FILE = CHECKPOINT_DIR / ".rate_limit.lock"

# เก็บเฉพาะกรุงเทพมหานคร: ตัดสินด้วยพิกัดเทียบขอบเขตจริง (geo.py)
# ถ้าไม่มีพิกัด ใช้ชื่อจังหวัดที่ปรากฏ "หลังสุด" ในข้อความ (ที่อยู่มักอยู่ท้าย) โดยตัดส่วน Cr./เครดิต ออกก่อน
BANGKOK_WORDS = ("bangkok", "กรุงเทพ", "กทม")
OTHER_PROVINCE_WORDS = (
    "samut prakan", "สมุทรปราการ", "nonthaburi", "นนทบุรี", "pathum thani", "ปทุมธานี",
    "samut sakhon", "สมุทรสาคร", "nakhon pathom", "นครปฐม", "chachoengsao", "ฉะเชิงเทรา",
    "ayutthaya", "อยุธยา", "chon buri", "chonburi", "ชลบุรี",
)
ACCIDENT_WORDS = ("อุบัติเหตุ", "accident", "crash", "collision", "รถชน", "ชนกัน")

# ค่า default ของ Airflow Variables
DEFAULT_BACKFILL_START_DATE = "2025-01-01"   # backfill ย้อนหลังถึงวันนี้
DEFAULT_BACKFILL_PAGES_PER_RUN = 200          # เพดานหน้า/รอบ (ถูกจำกัดด้วยเวลาอีกชั้น)
DEFAULT_BACKFILL_MAX_SECONDS = 14 * 60 + 30   # รอบละ ~14.5 นาที (DAG รันทุก 15 นาที) = ดึงต่อเนื่องแทบไม่มีช่วงว่าง
DEFAULT_BACKFILL_PAGE_SIZE = 1000             # limit=1000: ทดสอบแล้ว ตอบ ~14 วิ (limit=100 ตอบ ~100 วิ ที่ offset ลึก)
DEFAULT_BACKFILL_TARGET_ROWS = 0              # หยุดเมื่อเหตุการณ์ กทม. ครบจำนวนนี้ (0 = ไม่จำกัด)
DEFAULT_REALTIME_MAX_PAGES = 30
DEFAULT_RECOVER_PER_RUN = 150                 # เติม eid ที่ขาด ต่อรอบ (~25 นาที)
