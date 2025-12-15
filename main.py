#!/usr/bin/env python3
"""
每日爬取港交所主板新上市信息，写入 Zeabur MySQL 并推送 n8n webhook。
计划任务：北京时间 20:40。
"""

import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Optional

import mysql.connector
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from bs4 import BeautifulSoup
from mysql.connector import pooling, errorcode
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pytz
import requests


LISTING_URL = "https://www2.hkexnews.hk/New-Listings/New-Listing-Information/Main-Board?sc_lang=zh-HK"
WEBHOOK_URL = os.getenv(
    "WEBHOOK_URL", "https://n8n.imixtu.re/webhook/1aa73d9a-6615-4480-a28a-29968f399bb5"
)
ANNOUNCEMENT_WEBHOOK_URL = os.getenv(
    "ANNOUNCEMENT_WEBHOOK_URL", "https://n8n.imixtu.re/webhook/ipo-new"
)
ALLOTMENT_WEBHOOK_URL = os.getenv(
    "ALLOTMENT_WEBHOOK_URL", "https://n8n.imixtu.re/webhook/ipo-offer-result-2025"
)
N8N_SEND_INTERVAL = int(os.getenv("N8N_SEND_INTERVAL", "30"))
_last_n8n_sent_at: Optional[float] = None

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "sjc1.clusters.zeabur.com"),
    "port": int(os.getenv("MYSQL_PORT", "21517")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "4w5QpVI7b1jJUNeF8POK02h36nklDzC9"),
    "database": os.getenv("MYSQL_DB", "scrape"),
    "pool_name": "ipo_pool",
    "pool_size": int(os.getenv("MYSQL_POOL_SIZE", "5")),
    "charset": "utf8mb4",
    "use_unicode": True,
}

TIMEZONE = pytz.timezone("Asia/Shanghai")
SCHEDULE_HOUR = 20
SCHEDULE_MINUTE = 40

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("hkex_ipo")


def build_session() -> Session:
    """Create a requests session with retry strategy."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session


def fetch_listing_page(session: Session) -> Optional[str]:
    """Fetch listing page HTML."""
    try:
        resp: Response = session.get(LISTING_URL, timeout=30)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        return resp.text
    except requests.RequestException as exc:
        logger.error("Failed to fetch listing page: %s", exc)
        return None


def parse_listings(html: str) -> List[Dict[str, str]]:
    """Parse IPO rows from listing HTML."""
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        # Fallback to html.parser if lxml is not available
        soup = BeautifulSoup(html, "html.parser")
    target_table = None
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        if "股份代號" in headers and "新上市公告" in headers:
            target_table = table
            break

    if not target_table:
        logger.warning("Listing table not found in page")
        return []

    rows = target_table.find_all("tr")[1:]  # skip header
    listings: List[Dict[str, str]] = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 3:
            continue

        stock_id = cols[0].get_text(strip=True)
        name = cols[1].get_text(strip=True)
        announcement_link = ""
        announcement_anchor = cols[2].find("a")
        if announcement_anchor and announcement_anchor.get("href"):
            announcement_link = announcement_anchor["href"].strip()

        # 提取股份配发结果链接（第4列，索引为4）
        allotment_link = ""
        if len(cols) >= 5:
            allotment_anchor = cols[4].find("a")
            if allotment_anchor and allotment_anchor.get("href"):
                allotment_link = allotment_anchor["href"].strip()

        listings.append(
            {
                "id": stock_id,
                "name": name,
                "announcement_url": announcement_link,
                "allotment_url": allotment_link,
            }
        )

    return listings


class Database:
    def __init__(self, config: Dict[str, str]):
        try:
            self.pool = pooling.MySQLConnectionPool(
                pool_name=config["pool_name"],
                pool_size=config["pool_size"],
                host=config["host"],
                port=config["port"],
                user=config["user"],
                password=config["password"],
                database=config["database"],
                charset=config["charset"],
                use_unicode=config["use_unicode"],
            )
        except mysql.connector.Error as exc:
            logger.exception("Failed to create MySQL connection pool: %s", exc)
            raise

    def _get_connection(self):
        conn = self.pool.get_connection()
        conn.ping(reconnect=True, attempts=3, delay=2)
        return conn
 
    def _add_column_if_missing(
        self, cursor, table: str, column: str, definition: str, schema: str
    ) -> None:
        """Add a column only if it does not already exist (MySQL 5.7+)."""
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
            """,
            (schema, table, column),
        )
        exists = cursor.fetchone()[0] > 0
        if not exists:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
 
    def ensure_schema(self) -> None:
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ipo (
                        id VARCHAR(32) PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        announcement_url TEXT,
                        allotment_url TEXT,
                        announcement_sent TINYINT(1) DEFAULT 0,
                        allotment_sent TINYINT(1) DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ) DEFAULT CHARSET=utf8mb4
                    """
                )
                schema = MYSQL_CONFIG["database"]
                self._add_column_if_missing(
                    cursor, "ipo", "announcement_sent", "TINYINT(1) DEFAULT 0", schema
                )
                self._add_column_if_missing(
                    cursor, "ipo", "allotment_url", "TEXT", schema
                )
                self._add_column_if_missing(
                    cursor, "ipo", "allotment_sent", "TINYINT(1) DEFAULT 0", schema
                )
            conn.commit()
        finally:
            conn.close()

    def ipo_exists(self, stock_id: str) -> bool:
        query = "SELECT 1 FROM ipo WHERE id = %s LIMIT 1"
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (stock_id,))
                return cursor.fetchone() is not None
        finally:
            conn.close()

    def insert_ipo(self, record: Dict[str, str]) -> None:
        query = "INSERT INTO ipo (id, name, announcement_url, allotment_url) VALUES (%s, %s, %s, %s)"
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    query, (
                        record["id"],
                        record["name"],
                        record.get("announcement_url", ""),
                        record.get("allotment_url", ""),
                    )
                )
            conn.commit()
        finally:
            conn.close()

    def get_ipo_without_allotment(self) -> List[Dict[str, str]]:
        """获取 id 存在但 allotment_url 为空的记录"""
        query = "SELECT id, name FROM ipo WHERE (allotment_url IS NULL OR allotment_url = '')"
        conn = self._get_connection()
        try:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                return rows or []
        finally:
            conn.close()

    def update_allotment_url(self, stock_id: str, allotment_url: str) -> None:
        """更新股份配发结果 URL"""
        query = "UPDATE ipo SET allotment_url = %s WHERE id = %s"
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (allotment_url, stock_id))
            conn.commit()
        finally:
            conn.close()

    def get_unsent_announcements(self) -> List[Dict[str, str]]:
        query = """
            SELECT id, name, announcement_url
            FROM ipo
            WHERE (announcement_sent = 0 OR announcement_sent IS NULL)
        """
        conn = self._get_connection()
        try:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                return rows or []
        finally:
            conn.close()

    def is_announcement_sent(self, stock_id: str) -> bool:
        """检查公告是否已发送"""
        query = "SELECT announcement_sent FROM ipo WHERE id = %s"
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (stock_id,))
                result = cursor.fetchone()
                if result:
                    return bool(result[0])
                return False
        finally:
            conn.close()

    def mark_announcement_sent(self, stock_id: str) -> None:
        query = "UPDATE ipo SET announcement_sent = 1 WHERE id = %s"
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (stock_id,))
            conn.commit()
        finally:
            conn.close()

    def get_unsent_allotments(self) -> List[Dict[str, str]]:
        query = """
            SELECT id, name, allotment_url
            FROM ipo
            WHERE (allotment_sent = 0 OR allotment_sent IS NULL)
              AND allotment_url IS NOT NULL
              AND allotment_url <> ''
        """
        conn = self._get_connection()
        try:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                return rows or []
        finally:
            conn.close()

    def mark_allotment_sent(self, stock_id: str) -> None:
        query = "UPDATE ipo SET allotment_sent = 1 WHERE id = %s"
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (stock_id,))
            conn.commit()
        finally:
            conn.close()


def send_webhook(payload: Dict[str, str], session: Session) -> bool:
    """Send webhook to n8n for new IPO."""
    if not WEBHOOK_URL:
        logger.warning("WEBHOOK_URL not configured, skip webhook for %s", payload["id"])
        return False

    try:
        resp = session.post(WEBHOOK_URL, json=payload, timeout=15)
        resp.raise_for_status()
        logger.info("Webhook sent for %s %s", payload["id"], payload["name"])
        return True
    except requests.RequestException as exc:
        logger.error("Webhook failed for %s: %s", payload["id"], exc)
        return False


def send_announcement_url(record: Dict[str, str], session: Session) -> bool:
    """Send announcement URL of new IPO to dedicated webhook."""
    if not ANNOUNCEMENT_WEBHOOK_URL:
        logger.warning(
            "ANNOUNCEMENT_WEBHOOK_URL not configured, skip announcement webhook for %s",
            record["id"],
        )
        return False

    if not record.get("announcement_url"):
        logger.info(
            "No announcement_url for %s, skip announcement webhook", record["id"]
        )
        return False

    global _last_n8n_sent_at
    if _last_n8n_sent_at is not None:
        elapsed = time.time() - _last_n8n_sent_at
        if elapsed < N8N_SEND_INTERVAL:
            sleep_seconds = N8N_SEND_INTERVAL - elapsed
            logger.info(
                "Throttle n8n webhook for %s, sleep %.1fs to respect %ss interval",
                record["id"],
                sleep_seconds,
                N8N_SEND_INTERVAL,
            )
            time.sleep(sleep_seconds)

    payload = {"url": record["announcement_url"]}
    try:
        resp = session.post(ANNOUNCEMENT_WEBHOOK_URL, json=payload, timeout=15)
        resp.raise_for_status()
        logger.info("Announcement webhook sent for %s", record["id"])
        _last_n8n_sent_at = time.time()
        return True
    except requests.RequestException as exc:
        logger.error("Announcement webhook failed for %s: %s", record["id"], exc)
        return False


def send_allotment_url(record: Dict[str, str], session: Session) -> bool:
    """Send allotment URL to dedicated webhook."""
    if not ALLOTMENT_WEBHOOK_URL:
        logger.warning(
            "ALLOTMENT_WEBHOOK_URL not configured, skip allotment webhook for %s",
            record["id"],
        )
        return False

    if not record.get("allotment_url"):
        logger.info("No allotment_url for %s, skip allotment webhook", record["id"])
        return False

    global _last_n8n_sent_at
    if _last_n8n_sent_at is not None:
        elapsed = time.time() - _last_n8n_sent_at
        if elapsed < N8N_SEND_INTERVAL:
            sleep_seconds = N8N_SEND_INTERVAL - elapsed
            logger.info(
                "Throttle n8n webhook for %s, sleep %.1fs to respect %ss interval",
                record["id"],
                sleep_seconds,
                N8N_SEND_INTERVAL,
            )
            time.sleep(sleep_seconds)

    payload = {"url": record["allotment_url"]}
    try:
        resp = session.post(ALLOTMENT_WEBHOOK_URL, json=payload, timeout=15)
        resp.raise_for_status()
        logger.info("Allotment webhook sent for %s", record["id"])
        _last_n8n_sent_at = time.time()
        return True
    except requests.RequestException as exc:
        logger.error("Allotment webhook failed for %s: %s", record["id"], exc)
        return False


def send_pending_announcements(db: Database, session: Session) -> int:
    """Backfill announcement webhooks for existing unsent records."""
    pending = db.get_unsent_announcements()
    if not pending:
        logger.info("No pending announcements to send")
        return 0

    logger.info("Found %d pending announcements to send: %s", len(pending), [r["id"] for r in pending])
    sent = 0
    for record in pending:
        stock_id = record["id"]
        # 再次检查状态，避免重复发送
        if db.is_announcement_sent(stock_id):
            logger.warning("Stock %s announcement_sent is already 1, skipping", stock_id)
            continue
        
        logger.info("Processing pending announcement for %s %s", stock_id, record.get("name", ""))
        if send_announcement_url(record, session):
            db.mark_announcement_sent(stock_id)
            sent += 1
            logger.info("Marked announcement_sent=1 for %s", stock_id)
        else:
            logger.warning("Failed to send announcement for %s, not marking as sent", stock_id)
    return sent


def send_pending_allotments(db: Database, session: Session) -> int:
    """Send allotment URLs for records not yet sent."""
    pending = db.get_unsent_allotments()
    if not pending:
        logger.info("No pending allotment urls to send")
        return 0

    logger.info("Found %d pending allotment urls to send: %s", len(pending), [r["id"] for r in pending])
    sent = 0
    for record in pending:
        stock_id = record["id"]
        if send_allotment_url(record, session):
            db.mark_allotment_sent(stock_id)
            sent += 1
            logger.info("Marked allotment_sent=1 for %s", stock_id)
        else:
            logger.warning("Failed to send allotment for %s, not marking as sent", stock_id)
    return sent


def run_pipeline():
    logger.info("Start IPO sync job")
    session = build_session()
    html = fetch_listing_page(session)
    if not html:
        return

    listings = parse_listings(html)
    if not listings:
        logger.info("No listings parsed from page")
        return

    db = Database(MYSQL_CONFIG)
    try:
        db.ensure_schema()
    except mysql.connector.Error as exc:
        logger.error("Failed to ensure schema: %s", exc)
        return
    backfill_ann_sent = send_pending_announcements(db, session)
    if backfill_ann_sent:
        logger.info("Backfilled %s existing announcement urls", backfill_ann_sent)

    new_count = 0
    updated_allotment_count = 0

    # 创建 allotment_url 的映射，用于更新已存在的记录
    allotment_map = {
        record["id"]: record.get("allotment_url", "")
        for record in listings
        if record.get("allotment_url")
    }

    # 更新已存在但 allotment_url 为空的记录
    records_without_allotment = db.get_ipo_without_allotment()
    for record in records_without_allotment:
        stock_id = record["id"]
        if stock_id in allotment_map:
            try:
                db.update_allotment_url(stock_id, allotment_map[stock_id])
                updated_allotment_count += 1
                logger.info(
                    "Updated allotment_url for %s %s: %s",
                    stock_id,
                    record["name"],
                    allotment_map[stock_id],
                )
            except mysql.connector.Error as exc:
                logger.error("Failed to update allotment_url for %s: %s", stock_id, exc)

    backfill_allot_sent = send_pending_allotments(db, session)
    if backfill_allot_sent:
        logger.info("Backfilled %s existing allotment urls", backfill_allot_sent)

    for record in listings:
        if db.ipo_exists(record["id"]):
            continue

        try:
            db.insert_ipo(record)
            new_count += 1
            if send_announcement_url(record, session):
                db.mark_announcement_sent(record["id"])
            if send_allotment_url(record, session):
                db.mark_allotment_sent(record["id"])
            logger.info("Inserted new IPO %s %s", record["id"], record["name"])
        except mysql.connector.Error as exc:
            logger.error("DB insert failed for %s: %s", record["id"], exc)

    logger.info(
        "Job finished, new records: %s, updated allotment_url: %s, backfilled allotment: %s, backfilled announcement: %s",
        new_count,
        updated_allotment_count,
        backfill_allot_sent,
        backfill_ann_sent,
    )


def schedule_jobs():
    scheduler = BackgroundScheduler(timezone=TIMEZONE)
    scheduler.add_job(
        run_pipeline,
        CronTrigger(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE, timezone=TIMEZONE),
        id="hkex_ipo_daily",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(
        "Scheduler started, daily run at %02d:%02d Asia/Shanghai",
        SCHEDULE_HOUR,
        SCHEDULE_MINUTE,
    )
    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")


def main():
    run_pipeline()  # run once on startup
    schedule_jobs()


if __name__ == "__main__":
    main()
