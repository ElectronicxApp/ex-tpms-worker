"""
TPMS Listing Checker Worker
===========================
Standalone worker that verifies JTL Wawi listings exist for every scraped
TPMS document, persists results in MongoDB `listing_status` collection,
and runs on a 4-hour schedule.

Usage:
    python -m listing_checker.worker          # single run
    python -m listing_checker.worker --loop   # continuous 4h loop
"""

import os
import re
import time
import sys
import logging
from datetime import datetime, timezone
from typing import Optional

import requests
import schedule
from pymongo import MongoClient, ASCENDING
from pymongo.server_api import ServerApi
from bson import ObjectId
from dotenv import load_dotenv

# ─── Load .env ───────────────────────────────────────────────────────────────
load_dotenv()

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("listing_checker")

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


# ═════════════════════════════════════════════════════════════════════════════
#  JTL API Client (standalone)
# ═════════════════════════════════════════════════════════════════════════════
class JTLClient:
    """Lightweight JTL Wawi REST API client."""

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

    def search_items(self, keyword: str, page: int = 1, page_size: int = 100) -> dict:
        """Search items by keyword (single page)."""
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
#  MongoDB Manager
# ═════════════════════════════════════════════════════════════════════════════
class MongoManager:
    """Manages connections to TPMS database and listing_status collection."""

    def __init__(self, uri: str):
        if not uri:
            raise ValueError("MONDB_URI is empty. Set it in .env")
        self.client = MongoClient(uri, server_api=ServerApi("1"))
        self.client.admin.command("ping")
        log.info("Connected to MongoDB Atlas")

        self.db = self.client["TPMS"]
        self.scraped = self.db["scraped_data"]
        self.status = self.db["listing_status"]
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Create indexes on listing_status if they don't exist."""
        existing = {idx["name"] for idx in self.status.list_indexes()}

        indexes = {
            "document_id_unique": ([("document_id", ASCENDING)], {"unique": True}),
            "brand_model_idx": ([("brand", ASCENDING), ("model", ASCENDING)], {}),
            "all_complete_idx": ([("summary.all_complete", ASCENDING)], {}),
            "last_checked_idx": ([("last_checked_at", ASCENDING)], {}),
        }
        for name, (keys, opts) in indexes.items():
            if name not in existing:
                self.status.create_index(keys, name=name, **opts)
                log.info(f"Created index: {name}")

    def get_all_documents(self) -> list[dict]:
        """Return all scraped_data documents."""
        return list(self.scraped.find({}))
    
    def get_unprocessed_documents(self) -> list[dict]:
        """Return scraped_data documents that have NO entry in listing_status yet."""
        processed_ids = self.status.distinct("document_id")
        return list(self.scraped.find({"_id": {"$nin": processed_ids}}))

    def upsert_status(self, document_id: ObjectId, status_doc: dict):
        """Upsert a listing_status document for a scraped_data document."""
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
    # Pattern: MM.YYYY-MM.YYYY or MM.YYYY - MM.YYYY
    m = re.search(r"(\d{2})\.(\d{4})\s*-\s*(\d{2})\.(\d{4})", text)
    if m:
        return int(m.group(2)), int(m.group(4))

    # Pattern: YYYY-YYYY
    m = re.search(r"(\d{4})\s*-\s*(\d{4})", text)
    if m:
        return int(m.group(1)), int(m.group(2))

    # Single year
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
        # Distinguish mid (no 'x' prefix) from new (has 'x' prefix)
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


# ═════════════════════════════════════════════════════════════════════════════
#  Core Matching Engine
# ═════════════════════════════════════════════════════════════════════════════

def check_document(jtl: JTLClient, doc: dict) -> dict:
    """
    For a single scraped_data document, search JTL and build the status record.
    Returns the full status_doc ready for upsert.
    """
    brand = doc.get("brand", "")
    model = doc.get("model", "")
    date_range = doc.get("date_range", "")
    doc_start, doc_end = parse_years(date_range)

    # ── Search JTL ───────────────────────────────────────────────────────
    search_term = f"{brand} {model}"
    log.debug(f"  Searching JTL: '{search_term}'")
    jtl_items = jtl.search_all_items(search_term)
    log.debug(f"  Found {len(jtl_items)} JTL items")

    # ── Parse each JTL item ──────────────────────────────────────────────
    # Map: (qty, color) → best matching JTL item
    matched: dict[tuple[str, str], dict] = {}
    old_style_items: list[dict] = []

    for item in jtl_items:
        name = item.get("Name", "")
        sku = item.get("SKU", "")
        channels = item.get("ActiveSalesChannels", [])

        # Parse attributes
        qty = parse_qty(name)
        color = parse_color(name)
        style = detect_style(name, sku)
        item_start, item_end = parse_years(name)

        # Check if brand & model appear in the name (case-insensitive)
        name_lower = name.lower()
        if brand.lower() not in name_lower:
            continue
        # Model matching: handle "595 C" vs "595C" vs "595"
        model_variants = [model.lower(), model.lower().replace(" ", "")]
        if not any(mv in name_lower.replace(" ", "") for mv in model_variants):
            continue

        # Check date range overlap
        if not years_overlap(doc_start, doc_end, item_start, item_end):
            continue

        # ── Old-style listing → track separately ─────────────────────
        if style == "old":
            old_style_items.append({
                "jtl_item_id": item.get("Id"),
                "jtl_sku": sku,
                "jtl_name": name,
                "is_active": item.get("IsActive", False),
                "parent_item_id": item.get("ParentItemId", 0),
            })
            continue

        # ── New/mid style → try to match to (qty, color) slot ────────
        if qty is None or color is None:
            continue

        key = (qty, color)
        # Prefer active items; if already matched, keep the better one
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

    # ── Build the 8-slot listings array ──────────────────────────────────
    listings = []
    for qty in QUANTITIES:
        for color in COLORS:
            key = (qty, color)
            if key in matched:
                listings.append(matched[key])
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

    # ── Build summary ────────────────────────────────────────────────────
    found = sum(1 for l in listings if l["status"] == "FOUND")
    missing = len(listings) - found

    colors_matrix = {}
    for color in COLORS:
        colors_matrix[color] = {}
        for qty in QUANTITIES:
            key = (qty, color)
            colors_matrix[color][qty] = "FOUND" if key in matched else "MISSING"

    ebay_count = sum(1 for l in listings if l["has_ebay"])
    amazon_count = sum(1 for l in listings if l["has_amazon"])

    summary = {
        "total_expected": 8,
        "found": found,
        "missing": missing,
        "active_on_ebay": ebay_count,
        "active_on_amazon": amazon_count,
        "old_style_count": len(old_style_items),
        "all_complete": found == 8,
        "colors": colors_matrix,
    }

    return {
        "document_id": doc["_id"],
        "brand": brand,
        "model": model,
        "date_range": date_range,
        "listings": listings,
        "old_style_listings": old_style_items,
        "summary": summary,
    }


# ═════════════════════════════════════════════════════════════════════════════
#  Runner
# ═════════════════════════════════════════════════════════════════════════════

def run_check():
    """Execute a full check cycle across all scraped documents."""
    log.info("=" * 60)
    log.info("LISTING CHECK STARTED")
    log.info("=" * 60)

    start_time = time.time()

    try:
        mongo = MongoManager(MONDB_URI)
        jtl = JTLClient()
    except Exception as e:
        log.error(f"Initialization failed: {e}")
        return

    try:
        documents = mongo.get_unprocessed_documents()
        total = len(documents)
        log.info(f"Loaded {total} scraped documents from MongoDB")

        stats = {"complete": 0, "incomplete": 0, "errors": 0}

        for i, doc in enumerate(documents, 1):
            brand = doc.get("brand", "?")
            model = doc.get("model", "?")
            date_range = doc.get("date_range", "?")
            label = f"{brand} {model} ({date_range})"

            try:
                log.info(f"[{i}/{total}] Checking: {label}")
                status_doc = check_document(jtl, doc)
                mongo.upsert_status(doc["_id"], status_doc)

                s = status_doc["summary"]
                if s["all_complete"]:
                    stats["complete"] += 1
                    log.info(f"  ✅ Complete: {s['found']}/8 found")
                else:
                    stats["incomplete"] += 1
                    # Log which colors/bundles are missing
                    missing_details = []
                    for color, bundles in s["colors"].items():
                        for qty, st in bundles.items():
                            if st == "MISSING":
                                missing_details.append(f"{qty} {color}")
                    log.warning(f"  ⚠️  Incomplete: {s['found']}/8 found — missing: {', '.join(missing_details)}")

                if s["old_style_count"] > 0:
                    log.info(f"  🏚️  {s['old_style_count']} old-style listings found")
                if s["active_on_ebay"] > 0 or s["active_on_amazon"] > 0:
                    log.info(f"  📢 eBay: {s['active_on_ebay']}, Amazon: {s['active_on_amazon']}")

            except Exception as e:
                stats["errors"] += 1
                log.error(f"  ❌ Error checking {label}: {e}")

            # Small delay to avoid hammering the JTL API
            time.sleep(0.2)

        elapsed = time.time() - start_time
        log.info("")
        log.info("=" * 60)
        log.info("LISTING CHECK COMPLETE")
        log.info(f"  Total documents: {total}")
        log.info(f"  ✅ Complete (8/8): {stats['complete']}")
        log.info(f"  ⚠️  Incomplete:    {stats['incomplete']}")
        log.info(f"  ❌ Errors:         {stats['errors']}")
        log.info(f"  ⏱️  Duration:      {elapsed:.1f}s")
        log.info("=" * 60)

    except Exception as e:
        log.error(f"Check cycle failed: {e}")
    finally:
        mongo.close()


# ═════════════════════════════════════════════════════════════════════════════
#  Entry Point
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if "--loop" in sys.argv:
        log.info("Starting listing checker in LOOP mode (every 4 hours)")
        run_check()  # Run immediately on start
        schedule.every(4).hours.do(run_check)
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        log.info("Running single listing check...")
        run_check()
