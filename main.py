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
from mysql.connector import pooling
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pytz
import requests


LISTING_URL = "https://www2.hkexnews.hk/New-Listings/New-Listing-Information/Main-Board?sc_lang=zh-HK"
WEBHOOK_URL = os.getenv(
    "WEBHOOK_URL", "https://n8n.imixtu.re/webhook/1aa73d9a-6615-4480-a28a-29968f399bb5"
)

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
    soup = BeautifulSoup(html, "lxml")
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

        listings.append(
            {
                "id": stock_id,
                "name": name,
                "announcement_url": announcement_link,
            }
        )

    return listings


class Database:
    """MySQL connection pool wrapper."""

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
        query = "INSERT INTO ipo (id, name, announcement_url) VALUES (%s, %s, %s)"
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    query, (record["id"], record["name"], record["announcement_url"])
                )
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


def run_pipeline():
    """Fetch listings, insert new IPOs, and notify."""
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
    new_count = 0

    for record in listings:
        if db.ipo_exists(record["id"]):
            continue

        try:
            db.insert_ipo(record)
            new_count += 1
            send_webhook(record, session)
            logger.info("Inserted new IPO %s %s", record["id"], record["name"])
        except mysql.connector.Error as exc:
            logger.error("DB insert failed for %s: %s", record["id"], exc)

    logger.info("Job finished, new records: %s", new_count)


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
