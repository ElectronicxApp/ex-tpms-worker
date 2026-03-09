"""
worker.py – TPMS Listing Checker Worker
========================================
Runs every 4 hours (or once with no args):
  1. Process NEW scraped_data documents (no listing_status entry yet).
  2. Re-check PENDING documents (listing_status.summary.all_complete == False).
  3. Sleep for 4 hours, then repeat.

Features:
  • Exponential-backoff retry for MongoDB & JTL API timeouts.
  • Multi-strategy JTL search for high-confidence duplicate detection.
  • Rich Live dashboard (same style as scraperv2.py).
"""

import os
import re
import sys
import time
import random
import logging
import functools
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
from pymongo import MongoClient, ASCENDING
from pymongo.errors import (
    ServerSelectionTimeoutError,
    NetworkTimeout,
    AutoReconnect,
    ConnectionFailure,
    OperationFailure,
)
from pymongo.server_api import ServerApi
from bson import ObjectId
from dotenv import load_dotenv

from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.live import Live
from rich.text import Text
from rich.align import Align
from rich.progress import BarColumn, Progress, TextColumn, TimeRemainingColumn

# ─── Load .env ───────────────────────────────────────────────────────────────
load_dotenv()

# ─── Constants ───────────────────────────────────────────────────────────────
COLORS = [
    "Metallventil Silber",
    "Metallventil Schwarz",
    "Gummiventil",
    "Metallventil Gunmetal",
]
QUANTITIES = ["4x", "1x"]

# Color keywords used to detect color from item name (order matters: longest first)
COLOR_KEYWORDS = {
    "Metallventil Silber":   ["metallventil silber", "alu silber", "silber"],
    "Metallventil Schwarz":  ["metallventil schwarz", "alu schwarz"],
    "Gummiventil":           ["gummiventil", "gummi schwarz", "gummi sch."],
    "Metallventil Gunmetal": ["metallventil gunmetal", "alu gunmetal", "gunmetal"],
}

# Sales channel prefixes
EBAY_PREFIXES = ("5-30",)
AMAZON_PREFIXES = ("6-50", "6-51")

# JTL API Config
JTL_API_KEY = os.getenv("JTL_API_KEY", "2842abdb-5ffe-43f1-b156-4f128c8bfd2b")
JTL_CHALLENGE_CODE = os.getenv("JTL_CHALLENGE_CODE", "Priyam@2026")
JTL_APP_VERSION = os.getenv("JTL_APP_VERSION", "2.2.2026")
JTL_APP_ID = os.getenv("JTL_APP_ID", "electronicxApp/v2")
JTL_BASE_URL = os.getenv("JTL_BASE_URL", "http://127.0.0.1:5883/api/eazybusiness")

# MongoDB
MONDB_URI = os.getenv("MONDB_URI", "")

# Retry config
MAX_RETRIES = 5
BASE_BACKOFF = 2  # seconds

# Loop interval
LOOP_INTERVAL_HOURS = 4


# ═════════════════════════════════════════════════════════════════════════════
#  Rich Dashboard
# ═════════════════════════════════════════════════════════════════════════════

class Dashboard:
    """Live terminal dashboard for the TPMS Listing Checker."""

    def __init__(self):
        self.console = Console()
        self.layout = Layout()
        self.logs: list[str] = []
        self.stats = {
            "Cycle":           0,
            "Total Docs":      0,
            "New":             0,
            "Pending":         0,
            "Complete (8/8)":  0,
            "Incomplete":      0,
            "Old Style":       0,
            "Errors":          0,
            "Retries":         0,
        }
        self.current_status = "Initializing..."
        self.current_item = ""
        self.next_run = ""
        self.progress_current = 0
        self.progress_total = 0

        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="main"),
            Layout(name="footer", size=3),
        )
        self.layout["main"].split_row(
            Layout(name="left", ratio=1),
            Layout(name="right", ratio=2),
        )
        self.layout["left"].split_column(
            Layout(name="stats"),
            Layout(name="current", size=7),
        )
        self.layout["right"].name = "logs"

    def make_header(self) -> Panel:
        grid = Table.grid(expand=True)
        grid.add_column(justify="center", ratio=1)
        cycle = self.stats["Cycle"]
        grid.add_row(Text(f"TPMS Listing Checker — Cycle #{cycle}", style="bold magenta"))
        return Panel(grid, style="white on blue")

    def make_stats_table(self) -> Panel:
        table = Table(show_header=True, header_style="bold cyan", expand=True)
        table.add_column("Category", style="dim")
        table.add_column("Count", justify="right")
        for key, value in self.stats.items():
            color = (
                "bold magenta" if key == "Cycle"          else
                "white"        if key == "Total Docs"     else
                "cyan"         if key == "New"             else
                "yellow"       if key == "Pending"         else
                "bold green"   if key == "Complete (8/8)"  else
                "red"          if key == "Incomplete"      else
                "dim"          if key == "Old Style"       else
                "bold red"     if key == "Errors"          else
                "yellow"       if key == "Retries"         else
                "white"
            )
            table.add_row(key, f"[{color}]{value}[/]")
        return Panel(table, title="[bold]Statistics[/]")

    def make_current_panel(self) -> Panel:
        content = Text()
        content.append("Status: ", style="bold")
        content.append(f"{self.current_status}\n", style="yellow")
        content.append("Item:   ", style="bold")
        content.append(f"{self.current_item}\n", style="cyan")
        if self.progress_total > 0:
            pct = (self.progress_current / self.progress_total) * 100
            content.append("Progress: ", style="bold")
            content.append(f"{self.progress_current}/{self.progress_total} ({pct:.0f}%)\n", style="green")
        content.append("Next run: ", style="bold")
        content.append(self.next_run or "—", style="dim")
        return Panel(Align.center(content, vertical="middle"), title="[bold]Current Action[/]")

    def make_logs_panel(self) -> Panel:
        table = Table.grid(expand=True)
        for entry in self.logs[-20:]:
            table.add_row(entry)
        return Panel(table, title="[bold]Activity Log[/]")

    def make_footer(self) -> Panel:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return Panel(Align.center(Text(f"Last Update: {now}", style="dim")), style="white on blue")

    def refresh(self):
        """Rebuild all layout panels."""
        self.layout["header"].update(self.make_header())
        self.layout["stats"].update(self.make_stats_table())
        self.layout["current"].update(self.make_current_panel())
        self.layout["logs"].update(self.make_logs_panel())
        self.layout["footer"].update(self.make_footer())

    def log(self, message: str, style: str = "white"):
        now = datetime.now().strftime("%H:%M:%S")
        self.logs.append(f"[{now}] [{style}]{message}[/]")
        if len(self.logs) > 200:
            self.logs.pop(0)

    def update_status(self, status: str, item: str = ""):
        self.current_status = status
        if item:
            self.current_item = item

    def set_stat(self, key: str, value):
        self.stats[key] = value

    def increment_stat(self, key: str, amount: int = 1):
        if key in self.stats and isinstance(self.stats[key], int):
            self.stats[key] += amount

    def reset_cycle_stats(self):
        """Reset per-cycle counters (keep Cycle number)."""
        for key in ("Total Docs", "New", "Pending", "Complete (8/8)",
                     "Incomplete", "Old Style", "Errors", "Retries"):
            self.stats[key] = 0
        self.progress_current = 0
        self.progress_total = 0



# ═════════════════════════════════════════════════════════════════════════════
#  Plain Logger (for --worker / RDP mode)
# ═════════════════════════════════════════════════════════════════════════════

class PlainLogger:
    """Drop-in replacement for Dashboard that uses plain logging.
    Same public API so the rest of the code doesn't care which is active."""

    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s  %(levelname)-8s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.log_obj = logging.getLogger("listing_checker")
        self.stats = {
            "Cycle":           0,
            "Total Docs":      0,
            "New":             0,
            "Pending":         0,
            "Complete (8/8)":  0,
            "Incomplete":      0,
            "Old Style":       0,
            "Errors":          0,
            "Retries":         0,
        }
        self.current_status = "Initializing..."
        self.current_item = ""
        self.next_run = ""
        self.progress_current = 0
        self.progress_total = 0

    def log(self, message: str, style: str = "white"):
        # Map rich styles to log levels
        msg = message.strip("═ ")
        if not msg:
            return
        if "red" in style:
            self.log_obj.error(message)
        elif "yellow" in style:
            self.log_obj.warning(message)
        else:
            self.log_obj.info(message)

    def update_status(self, status: str, item: str = ""):
        self.current_status = status
        if item:
            self.current_item = item

    def set_stat(self, key: str, value):
        self.stats[key] = value

    def increment_stat(self, key: str, amount: int = 1):
        if key in self.stats and isinstance(self.stats[key], int):
            self.stats[key] += amount

    def reset_cycle_stats(self):
        for key in ("Total Docs", "New", "Pending", "Complete (8/8)",
                     "Incomplete", "Old Style", "Errors", "Retries"):
            self.stats[key] = 0
        self.progress_current = 0
        self.progress_total = 0

    def refresh(self):
        pass  # no-op for plain logging


# Will be set in main() based on --worker flag
dashboard: Dashboard | PlainLogger = None  # type: ignore


# ═════════════════════════════════════════════════════════════════════════════
#  Retry decorator with exponential backoff + jitter
# ═════════════════════════════════════════════════════════════════════════════

# Exceptions worth retrying
MONGO_RETRY_ERRORS = (
    ServerSelectionTimeoutError,
    NetworkTimeout,
    AutoReconnect,
    ConnectionFailure,
)
API_RETRY_ERRORS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
)


def retry_with_backoff(max_retries: int = MAX_RETRIES, base_backoff: float = BASE_BACKOFF,
                       retry_on: tuple = ()):
    """Decorator: retry a function on specified exceptions with exponential backoff + jitter."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_on as exc:
                    last_exc = exc
                    if attempt == max_retries:
                        dashboard.log(
                            f"❌ {func.__name__} failed after {max_retries} retries: {exc}",
                            style="bold red",
                        )
                        raise
                    sleep_time = base_backoff * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    dashboard.log(
                        f"⟳ {func.__name__} retry {attempt}/{max_retries} "
                        f"in {sleep_time:.1f}s — {type(exc).__name__}: {exc}",
                        style="yellow",
                    )
                    dashboard.increment_stat("Retries")
                    time.sleep(sleep_time)
            raise last_exc  # should never reach here
        return wrapper
    return decorator


# ═════════════════════════════════════════════════════════════════════════════
#  JTL API Client (with retry)
# ═════════════════════════════════════════════════════════════════════════════

class JTLClient:
    """Lightweight JTL Wawi REST API client with retry logic."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "X-ChallengeCode": JTL_CHALLENGE_CODE,
            "X-AppId": JTL_APP_ID,
            "X-AppVersion": JTL_APP_VERSION,
            "Authorization": f"Wawi {JTL_API_KEY}",
            "api-version": "1.0",
        })
        self.base_url = JTL_BASE_URL

    @retry_with_backoff(max_retries=MAX_RETRIES, retry_on=API_RETRY_ERRORS)
    def search_items(self, keyword: str, page: int = 1, page_size: int = 100) -> dict:
        """Search items by keyword (single page), with retry."""
        url = f"{self.base_url}/v1/items"
        params = {
            "searchKeyWord": keyword,
            "pageNumber": page,
            "pageSize": page_size,
        }
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def search_all_items(self, keyword: str, max_pages: int = 20) -> list[dict]:
        """Auto-paginate and return ALL items matching keyword."""
        all_items = []
        page = 1
        while page <= max_pages:
            data = self.search_items(keyword, page=page, page_size=100)
            items = data.get("Items", [])
            all_items.extend(items)
            total = data.get("TotalItems", 0)
            if len(all_items) >= total or not items:
                break
            page += 1
        return all_items


# ═════════════════════════════════════════════════════════════════════════════
#  MongoDB Manager (with retry)
# ═════════════════════════════════════════════════════════════════════════════

class MongoManager:
    """Manages connections to TPMS database and listing_status collection."""

    def __init__(self, uri: str):
        if not uri:
            raise ValueError("MONDB_URI is empty. Set it in .env")
        self._uri = uri
        self._connect()

    @retry_with_backoff(max_retries=MAX_RETRIES, retry_on=MONGO_RETRY_ERRORS)
    def _connect(self):
        """Connect (or reconnect) to MongoDB with retry."""
        self.client = MongoClient(self._uri, server_api=ServerApi("1"),
                                   serverSelectionTimeoutMS=10000,
                                   connectTimeoutMS=10000,
                                   socketTimeoutMS=30000)
        self.client.admin.command("ping")
        dashboard.log("Connected to MongoDB Atlas", style="bold green")

        self.db = self.client["TPMS"]
        self.scraped = self.db["scraped_data"]
        self.status = self.db["listing_status"]
        self._ensure_indexes()

    def _reconnect_if_needed(self):
        """Test the connection; reconnect if dead."""
        try:
            self.client.admin.command("ping")
        except MONGO_RETRY_ERRORS:
            dashboard.log("MongoDB connection lost — reconnecting…", style="yellow")
            self._connect()

    def _ensure_indexes(self):
        """Create indexes on listing_status if they don't exist."""
        existing = {idx["name"] for idx in self.status.list_indexes()}

        indexes = {
            "document_id_unique": ([(  "document_id", ASCENDING)], {"unique": True}),
            "brand_model_idx":    ([("brand", ASCENDING), ("model", ASCENDING)], {}),
            "all_complete_idx":   ([("summary.all_complete", ASCENDING)], {}),
            "last_checked_idx":   ([("last_checked_at", ASCENDING)], {}),
        }
        for name, (keys, opts) in indexes.items():
            if name not in existing:
                self.status.create_index(keys, name=name, **opts)
                dashboard.log(f"Created index: {name}", style="dim")

    @retry_with_backoff(max_retries=MAX_RETRIES, retry_on=MONGO_RETRY_ERRORS)
    def get_all_documents(self) -> list[dict]:
        """Return all scraped_data documents."""
        self._reconnect_if_needed()
        return list(self.scraped.find({}))

    @retry_with_backoff(max_retries=MAX_RETRIES, retry_on=MONGO_RETRY_ERRORS)
    def get_unprocessed_documents(self) -> list[dict]:
        """Return scraped_data documents that have NO entry in listing_status yet."""
        self._reconnect_if_needed()
        processed_ids = self.status.distinct("document_id")
        return list(self.scraped.find({"_id": {"$nin": processed_ids}}))

    @retry_with_backoff(max_retries=MAX_RETRIES, retry_on=MONGO_RETRY_ERRORS)
    def get_pending_documents(self) -> list[dict]:
        """Return scraped_data documents whose listing_status has all_complete == False."""
        self._reconnect_if_needed()
        pending_statuses = list(self.status.find(
            {"summary.all_complete": False},
            {"document_id": 1, "_id": 0},
        ))
        pending_ids = [s["document_id"] for s in pending_statuses]
        if not pending_ids:
            return []
        return list(self.scraped.find({"_id": {"$in": pending_ids}}))

    @retry_with_backoff(max_retries=MAX_RETRIES, retry_on=MONGO_RETRY_ERRORS)
    def upsert_status(self, document_id: ObjectId, status_doc: dict):
        """Upsert a listing_status document for a scraped_data document."""
        self._reconnect_if_needed()
        now = datetime.now(timezone.utc)
        status_doc["last_checked_at"] = now
        status_doc["updated_at"] = now

        self.status.update_one(
            {"document_id": document_id},
            {
                "$set": status_doc,
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )

    def close(self):
        self.client.close()


# ═════════════════════════════════════════════════════════════════════════════
#  Name Parser — extracts (qty, color, date_range, style) from JTL item name
# ═════════════════════════════════════════════════════════════════════════════

def parse_qty(name: str) -> Optional[str]:
    """Extract quantity from item name.  '4x ...' or '4 Reifendrucksensoren' → '4x',  '1x ...' → '1x'."""
    name_s = name.strip()
    if re.match(r"^4x\b", name_s, re.IGNORECASE):
        return "4x"
    if re.match(r"^4\s", name_s):
        return "4x"
    if re.match(r"^1x\b", name_s, re.IGNORECASE):
        return "1x"
    if re.match(r"^1\s", name_s):
        return "1x"
    return None


def parse_color(name: str) -> Optional[str]:
    """Detect color from item name using keyword matching."""
    name_lower = name.lower()
    for color, keywords in COLOR_KEYWORDS.items():
        for kw in keywords:
            if kw in name_lower:
                return color
    return None


def parse_years(text: str) -> tuple[Optional[int], Optional[int]]:
    """
    Extract start/end years from a date string.
    Handles:  '01.2014-12.2015', '2015-2021', '01.2014 - 12.2015'
    """
    m = re.search(r"(\d{2})\.(\d{4})\s*-\s*(\d{2})\.(\d{4})", text)
    if m:
        return int(m.group(2)), int(m.group(4))

    m = re.search(r"(\d{4})\s*-\s*(\d{4})", text)
    if m:
        return int(m.group(1)), int(m.group(2))

    m = re.search(r"(\d{4})", text)
    if m:
        return int(m.group(1)), int(m.group(1))

    return None, None


def detect_style(name: str, sku: str) -> str:
    """Classify listing style: 'old', 'mid', or 'new'."""
    name_lower = name.lower()
    if "stück" in name_lower or "kompatibel" in name_lower or sku.startswith("AUTEL"):
        return "old"
    if "rdks sensoren" in name_lower:
        if re.match(r"^\d+x\b", name.strip()):
            return "new"
        return "mid"
    return "unknown"


def years_overlap(start1, end1, start2, end2) -> bool:
    """Check if two year ranges overlap."""
    if any(v is None for v in (start1, end1, start2, end2)):
        return False
    return start1 <= end2 and start2 <= end1


def has_ebay(channels: list[str]) -> bool:
    return any(ch.startswith(pfx) for ch in channels for pfx in EBAY_PREFIXES)


def has_amazon(channels: list[str]) -> bool:
    return any(ch.startswith(pfx) for ch in channels for pfx in AMAZON_PREFIXES)


def normalize_model(model: str) -> str:
    """Normalize model name for fuzzy matching: lowercase, strip spaces/dashes/dots."""
    return re.sub(r"[\s\-\.]+", "", model.lower())


# ═════════════════════════════════════════════════════════════════════════════
#  Core Matching Engine — Multi-Strategy Search
# ═════════════════════════════════════════════════════════════════════════════

def _filter_and_match(jtl_items: list[dict], brand: str, model: str,
                      doc_start, doc_end) -> tuple[dict, list[dict]]:
    """
    Filter JTL items that match brand + model + date range.
    Returns (matched_slots, old_style_items).
    """
    matched: dict[tuple[str, str], dict] = {}
    old_style_items: list[dict] = []
    norm_model = normalize_model(model)

    for item in jtl_items:
        name = item.get("Name", "")
        sku = item.get("SKU", "")
        channels = item.get("ActiveSalesChannels", [])

        qty = parse_qty(name)
        color = parse_color(name)
        style = detect_style(name, sku)
        item_start, item_end = parse_years(name)

        # Brand must appear in name
        name_lower = name.lower()
        if brand.lower() not in name_lower:
            continue

        # Model matching: normalize both for fuzzy compare
        name_normalized = normalize_model(name)
        if norm_model not in name_normalized:
            continue

        # Check date range overlap
        if not years_overlap(doc_start, doc_end, item_start, item_end):
            continue

        # Old-style listing → track separately
        if style == "old":
            old_style_items.append({
                "jtl_item_id": item.get("Id"),
                "jtl_sku": sku,
                "jtl_name": name,
                "is_active": item.get("IsActive", False),
                "parent_item_id": item.get("ParentItemId", 0),
            })
            continue

        # New/mid style → try to match to (qty, color) slot
        if qty is None or color is None:
            continue

        key = (qty, color)
        if key not in matched or (item.get("IsActive") and not matched[key].get("is_active")):
            matched[key] = {
                "qty": qty,
                "color": color,
                "status": "FOUND",
                "jtl_item_id": item.get("Id"),
                "jtl_sku": sku,
                "jtl_name": name,
                "is_active": item.get("IsActive", False),
                "listing_style": style,
                "has_ebay": has_ebay(channels),
                "has_amazon": has_amazon(channels),
                "active_channels": channels,
            }

    return matched, old_style_items


def check_document(jtl: JTLClient, doc: dict) -> dict:
    """
    For a single scraped_data document, search JTL using multiple strategies
    and build the status record with confidence scoring.

    Strategies:
      1. Primary   — search "{brand} {model}"
      2. Secondary — search "{brand}" only, filter locally by model + date
      3. Tertiary  — search by part number (if available)

    Returns the full status_doc ready for upsert.
    """
    brand = doc.get("brand", "")
    model = doc.get("model", "")
    date_range = doc.get("date_range", "")
    part_numbers = doc.get("part_numbers", [])
    doc_start, doc_end = parse_years(date_range)

    all_matched: dict[tuple[str, str], dict] = {}
    all_old_style: list[dict] = []
    total_jtl_results = 0
    strategies_used = 0

    # ── Strategy 1: Primary search — "{brand} {model}" ──────────────────
    search_term_1 = f"{brand} {model}"
    dashboard.log(f"  🔍 Strategy 1: '{search_term_1}'", style="dim")
    jtl_items_1 = jtl.search_all_items(search_term_1)
    total_jtl_results += len(jtl_items_1)

    if jtl_items_1:
        strategies_used += 1
        matched_1, old_1 = _filter_and_match(jtl_items_1, brand, model, doc_start, doc_end)
        # Merge into results (primary wins by default)
        for key, val in matched_1.items():
            if key not in all_matched:
                all_matched[key] = val
        all_old_style.extend(old_1)

    # ── Strategy 2: Secondary search — "{brand}" only ───────────────────
    search_term_2 = brand
    dashboard.log(f"  🔍 Strategy 2: '{search_term_2}'", style="dim")
    jtl_items_2 = jtl.search_all_items(search_term_2)
    total_jtl_results += len(jtl_items_2)

    if jtl_items_2:
        strategies_used += 1
        matched_2, old_2 = _filter_and_match(jtl_items_2, brand, model, doc_start, doc_end)
        for key, val in matched_2.items():
            if key not in all_matched:
                all_matched[key] = val
        # Deduplicate old style by jtl_item_id
        existing_old_ids = {o["jtl_item_id"] for o in all_old_style}
        all_old_style.extend([o for o in old_2 if o["jtl_item_id"] not in existing_old_ids])

    # ── Strategy 3: Tertiary search — part numbers ──────────────────────
    if part_numbers:
        for pn in part_numbers[:3]:  # Limit to first 3 part numbers
            dashboard.log(f"  🔍 Strategy 3: part# '{pn}'", style="dim")
            jtl_items_3 = jtl.search_all_items(pn)
            total_jtl_results += len(jtl_items_3)
            if jtl_items_3:
                strategies_used += 1
                matched_3, old_3 = _filter_and_match(jtl_items_3, brand, model, doc_start, doc_end)
                for key, val in matched_3.items():
                    if key not in all_matched:
                        all_matched[key] = val
                existing_old_ids = {o["jtl_item_id"] for o in all_old_style}
                all_old_style.extend([o for o in old_3 if o["jtl_item_id"] not in existing_old_ids])

    # Small delay between documents to avoid hammering the JTL API
    time.sleep(0.2)

    # ── Build the 8-slot listings array ──────────────────────────────────
    listings = []
    for qty in QUANTITIES:
        for color in COLORS:
            key = (qty, color)
            if key in all_matched:
                listings.append(all_matched[key])
            else:
                listings.append({
                    "qty": qty,
                    "color": color,
                    "status": "MISSING",
                    "jtl_item_id": None,
                    "jtl_sku": None,
                    "jtl_name": None,
                    "is_active": None,
                    "listing_style": None,
                    "has_ebay": False,
                    "has_amazon": False,
                    "active_channels": [],
                })

    # ── Confidence scoring ───────────────────────────────────────────────
    found = sum(1 for l in listings if l["status"] == "FOUND")
    missing = len(listings) - found

    if strategies_used >= 2:
        confidence = "HIGH"
    elif strategies_used == 1 and total_jtl_results > 0:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    # ── Build colors matrix ──────────────────────────────────────────────
    colors_matrix = {}
    for color in COLORS:
        colors_matrix[color] = {}
        for qty in QUANTITIES:
            key = (qty, color)
            colors_matrix[color][qty] = "FOUND" if key in all_matched else "MISSING"

    ebay_count = sum(1 for l in listings if l["has_ebay"])
    amazon_count = sum(1 for l in listings if l["has_amazon"])

    summary = {
        "total_expected": 8,
        "found": found,
        "missing": missing,
        "active_on_ebay": ebay_count,
        "active_on_amazon": amazon_count,
        "old_style_count": len(all_old_style),
        "all_complete": found == 8,
        "confidence": confidence,
        "total_jtl_results": total_jtl_results,
        "colors": colors_matrix,
    }

    return {
        "document_id": doc["_id"],
        "brand": brand,
        "model": model,
        "date_range": date_range,
        "listings": listings,
        "old_style_listings": all_old_style,
        "summary": summary,
    }


# ═════════════════════════════════════════════════════════════════════════════
#  Runner — processes a list of documents
# ═════════════════════════════════════════════════════════════════════════════

def process_documents(jtl: JTLClient, mongo: MongoManager,
                      documents: list[dict], label: str) -> dict:
    """
    Check a batch of documents and upsert results.
    Returns stats dict: {complete, incomplete, errors}.
    """
    total = len(documents)
    stats = {"complete": 0, "incomplete": 0, "errors": 0}

    for i, doc in enumerate(documents, 1):
        brand = doc.get("brand", "?")
        model = doc.get("model", "?")
        date_range = doc.get("date_range", "?")
        item_label = f"{brand} {model} ({date_range})"

        dashboard.update_status(f"Checking [{label}]", item=item_label)
        dashboard.progress_current = i
        dashboard.progress_total = total

        try:
            dashboard.log(f"[{i}/{total}] {label}: {item_label}")
            status_doc = check_document(jtl, doc)
            mongo.upsert_status(doc["_id"], status_doc)

            s = status_doc["summary"]
            if s["all_complete"]:
                stats["complete"] += 1
                dashboard.increment_stat("Complete (8/8)")
                dashboard.log(f"  ✅ Complete: {s['found']}/8 — confidence: {s['confidence']}", style="bold green")
            else:
                stats["incomplete"] += 1
                dashboard.increment_stat("Incomplete")
                missing_details = []
                for color, bundles in s["colors"].items():
                    for qty, st in bundles.items():
                        if st == "MISSING":
                            missing_details.append(f"{qty} {color}")
                dashboard.log(
                    f"  ⚠️ Incomplete: {s['found']}/8 — missing: {', '.join(missing_details)} "
                    f"[{s['confidence']}]",
                    style="yellow",
                )

            if s["old_style_count"] > 0:
                dashboard.increment_stat("Old Style", s["old_style_count"])
                dashboard.log(f"  🏚️ {s['old_style_count']} old-style listings", style="dim")

        except Exception as e:
            stats["errors"] += 1
            dashboard.increment_stat("Errors")
            dashboard.log(f"  ❌ Error: {item_label}: {e}", style="bold red")

    return stats


# ═════════════════════════════════════════════════════════════════════════════
#  Main check cycle
# ═════════════════════════════════════════════════════════════════════════════

def run_check(cycle_number: int):
    """Execute a full check cycle: new docs first, then pending docs."""
    dashboard.reset_cycle_stats()
    dashboard.set_stat("Cycle", cycle_number)
    dashboard.log("═" * 50, style="bold magenta")
    dashboard.log(f"CYCLE #{cycle_number} STARTED", style="bold magenta")
    dashboard.log("═" * 50, style="bold magenta")

    start_time = time.time()

    try:
        dashboard.update_status("Connecting to MongoDB…")
        mongo = MongoManager(MONDB_URI)
        jtl = JTLClient()
    except Exception as e:
        dashboard.log(f"Initialization failed: {e}", style="bold red")
        return

    try:
        # ── Pass 1: New (unprocessed) documents ─────────────────────────
        dashboard.update_status("Loading new documents…")
        new_docs = mongo.get_unprocessed_documents()
        dashboard.set_stat("New", len(new_docs))
        dashboard.log(f"📋 New (unprocessed) documents: {len(new_docs)}")

        if new_docs:
            stats_new = process_documents(jtl, mongo, new_docs, "NEW")
            dashboard.log(
                f"Pass 1 done — ✅ {stats_new['complete']}  ⚠️ {stats_new['incomplete']}  "
                f"❌ {stats_new['errors']}",
                style="cyan",
            )

        # ── Pass 2: Pending (incomplete) documents ──────────────────────
        dashboard.update_status("Loading pending documents…")
        pending_docs = mongo.get_pending_documents()
        dashboard.set_stat("Pending", len(pending_docs))
        dashboard.log(f"📋 Pending (incomplete) documents: {len(pending_docs)}")

        if pending_docs:
            stats_pending = process_documents(jtl, mongo, pending_docs, "PENDING")
            dashboard.log(
                f"Pass 2 done — ✅ {stats_pending['complete']}  ⚠️ {stats_pending['incomplete']}  "
                f"❌ {stats_pending['errors']}",
                style="cyan",
            )

        # ── Get total stats from DB ─────────────────────────────────────
        total_docs = mongo.scraped.count_documents({})
        total_complete = mongo.status.count_documents({"summary.all_complete": True})
        dashboard.set_stat("Total Docs", total_docs)
        dashboard.set_stat("Complete (8/8)", total_complete)
        dashboard.set_stat("Incomplete", total_docs - total_complete)

        elapsed = time.time() - start_time
        dashboard.log("═" * 50, style="bold magenta")
        dashboard.log(f"CYCLE #{cycle_number} COMPLETE — {elapsed:.1f}s", style="bold magenta")
        dashboard.log("═" * 50, style="bold magenta")

    except Exception as e:
        dashboard.log(f"Cycle failed: {e}", style="bold red")
    finally:
        mongo.close()


# ═════════════════════════════════════════════════════════════════════════════
#  Entry Point
# ═════════════════════════════════════════════════════════════════════════════

def _run_loop(loop_mode: bool):
    """Core loop logic shared by both dashboard and worker modes."""
    cycle = 0
    while True:
        cycle += 1
        run_check(cycle)

        if not loop_mode:
            dashboard.update_status("Done — single run complete")
            dashboard.next_run = "N/A (single run)"
            dashboard.refresh()
            break

        # ── Sleep countdown ─────────────────────────────────────────
        next_run_time = datetime.now() + timedelta(hours=LOOP_INTERVAL_HOURS)
        dashboard.next_run = next_run_time.strftime("%Y-%m-%d %H:%M:%S")
        dashboard.update_status("Sleeping until next cycle…")
        dashboard.log(f"💤 Next run at {dashboard.next_run}", style="dim")

        sleep_seconds = LOOP_INTERVAL_HOURS * 3600
        wake_at = time.time() + sleep_seconds

        while time.time() < wake_at:
            remaining = int(wake_at - time.time())
            hours, remainder = divmod(remaining, 3600)
            minutes, secs = divmod(remainder, 60)
            dashboard.update_status(
                f"Sleeping — next cycle in {hours}h {minutes}m {secs}s"
            )
            dashboard.refresh()
            time.sleep(10)


def main():
    global dashboard

    loop_mode = "--loop" in sys.argv
    worker_mode = "--worker" in sys.argv

    # ── Choose output mode ──────────────────────────────────────────
    if worker_mode:
        dashboard = PlainLogger()
    else:
        dashboard = Dashboard()

    if loop_mode:
        dashboard.log("Starting in LOOP mode (every 4 hours)", style="bold magenta")
    else:
        dashboard.log("Running single check…", style="bold magenta")

    if worker_mode:
        # Plain logging mode — no Rich Live, just run directly
        dashboard.log("Running in WORKER mode (plain logging)", style="bold magenta")
        _run_loop(loop_mode)
    else:
        # Rich dashboard mode
        with Live(dashboard.layout, console=dashboard.console,
                  refresh_per_second=2, screen=True):
            _run_loop(loop_mode)
            if not loop_mode:
                time.sleep(5)  # Keep dashboard visible briefly


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 Listing checker stopped by user.")
        sys.exit(0)
