"""
Temporary script to fetch more sample data from JTL API 
to understand all listing patterns (1x bundles, new-style AR, old-style AUTEL).
"""
import requests
import json

JTL_API_KEY = "2842abdb-5ffe-43f1-b156-4f128c8bfd2b"
CHALLENGE_CODE = "Priyam@2026"
APP_VERSION = "2.2.2026"
APP_ID = "electronicxApp/v2"
BASE_URL = "http://127.0.0.1:5883/api/eazybusiness"

headers = {
    "X-ChallengeCode": CHALLENGE_CODE,
    "X-AppId": APP_ID,
    "X-AppVersion": APP_VERSION,
    "Authorization": f"Wawi {JTL_API_KEY}",
    "api-version": "1.0"
}

session = requests.Session()
session.headers.update(headers)


def get_all_items(search_keyword, max_pages=10):
    """Fetch all items matching keyword across pages."""
    all_items = []
    page = 1
    while page <= max_pages:
        url = f"{BASE_URL}/v1/items"
        params = {
            "pageNumber": page,
            "pageSize": 100,
            "searchKeyWord": search_keyword
        }
        resp = session.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("Items", [])
        all_items.extend(items)
        total = data.get("TotalItems", 0)
        print(f"  Page {page}: got {len(items)} items (total: {total})")
        if len(all_items) >= total or not items:
            break
        page += 1
    return all_items


def analyze_items(items):
    """Print summary of each item for analysis."""
    for item in items:
        name = item.get("Name", "")
        sku = item.get("SKU", "")
        item_id = item.get("Id", "")
        is_active = item.get("IsActive", False)
        parent_id = item.get("ParentItemId", 0)
        channels = item.get("ActiveSalesChannels", [])
        categories = [c.get("Name", "") for c in item.get("Categories", [])]
        
        # Detect pattern
        is_old = "AUTEL" in sku or "Stück" in name or "kompatibel" in name.lower()
        is_new = sku.startswith("AR") or "RDKS Sensoren" in name
        
        # Detect qty
        qty = "?"
        if "4x" in name or "4 Stück" in name:
            qty = "4x"
        elif "1x" in name or "1 Stück" in name:
            qty = "1x"
        
        # Detect color
        color = "?"
        name_lower = name.lower()
        if "silber" in name_lower:
            color = "Silber"
        elif "schwarz" in name_lower:
            color = "Schwarz"
        elif "gummi" in name_lower:
            color = "Gummi"
        elif "gunmetal" in name_lower:
            color = "Gunmetal"
        
        # Check channels
        has_ebay = any(ch.startswith("5-30") for ch in channels)
        has_amazon = any(ch.startswith("6-5") for ch in channels)
        
        print(f"\n  ID={item_id} | SKU={sku}")
        print(f"  Name: {name[:100]}")
        print(f"  Pattern: {'OLD' if is_old else 'NEW' if is_new else 'UNKNOWN'} | Qty={qty} | Color={color}")
        print(f"  Active={is_active} | ParentId={parent_id} | eBay={has_ebay} | Amazon={has_amazon}")
        print(f"  Channels: {channels}")
        print(f"  Categories: {categories[:2]}")


# Search for various patterns
searches = [
    "Abarth 595",                # Known model with old+new listings  
    "1x Reifendrucksensoren",    # Find 1x bundle naming pattern
    "Einzelsensor",              # Alternative 1x name?
    "AR339",                     # New-style SKU prefix
]

all_results = {}
for kw in searches:
    print(f"\n{'='*60}")
    print(f"Searching: '{kw}'")
    print(f"{'='*60}")
    items = get_all_items(kw, max_pages=3)
    all_results[kw] = items
    if items:
        analyze_items(items[:15])  # Show first 15
    else:
        print("  No results found.")

# Save all results
with open("responses/all_sample_data.json", "w", encoding="utf-8") as f:
    json.dump(all_results, f, indent=2, ensure_ascii=False)
    
print(f"\n\nSaved all results to responses/all_sample_data.json")
print(f"Total items per search: {', '.join(f'{k}: {len(v)}' for k, v in all_results.items())}")
