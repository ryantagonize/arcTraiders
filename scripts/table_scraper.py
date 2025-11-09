#!/usr/bin/env python3
# scripts/table_scraper.py

import json
import sys
import argparse
from pathlib import Path
from typing import Optional, List, Dict

import requests
from bs4 import BeautifulSoup

URL = "https://arcraiders.wiki/wiki/Blueprints"

# Map messy/variant headers -> clean snake_case keys
HEADER_MAP = {
    "blueprint": "blueprint_name",
    "blueprint name": "blueprint_name",
    "name": "blueprint_name",
    "workshop": "workshop",
    "crafting recipe": "crafting_recipe",
    "loot": "loot",
    "harvester event": "harvester_event",
    "quest reward": "quest_reward",
    "trials reward": "trials_reward",
}

def fetch_html(url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; ArcScraper/1.0)"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.text

def find_table(soup: BeautifulSoup):
    # Prefer a wikitable whose header contains "Blueprint"
    t = soup.select_one('table.wikitable:has(th:contains("Blueprint"))')
    return t or soup.select_one("table.wikitable")

def clean_key(header: str) -> str:
    h = " ".join(header.split()).strip().lower()
    return HEADER_MAP.get(h, h.replace(" ", "_"))

def cell_text(td) -> str:
    # Preserve line breaks as spaces; strip refs/footnotes text
    return td.get_text(" ", strip=True)

def cell_link(td) -> Optional[str]:
    a = td.find("a", href=True)
    if not a:
        return None
    href = a["href"]
    if href.startswith("/"):
        return f"https://arcraiders.wiki{href}"
    return href

def parse_table(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    table = find_table(soup)
    if not table:
        raise RuntimeError("Blueprints table not found")

    rows = table.select("tr")
    if not rows:
        raise RuntimeError("Blueprints table has no rows")

    # Headers
    raw_headers = [th.get_text(" ", strip=True) for th in rows[0].select("th")]
    headers = [clean_key(h) for h in raw_headers]
    n = len(headers)

    out: List[Dict] = []
    for tr in rows[1:]:
        tds = tr.select("td")
        if not tds:
            continue
        cells = [cell_text(td) for td in tds]
        # normalize length to header count
        if len(cells) < n:
            cells += [""] * (n - len(cells))
        elif len(cells) > n:
            cells = cells[:n]

        record = dict(zip(headers, cells))

        # Try to capture a link for the blueprint (first column usually)
        if headers and headers[0] in record:
            link = cell_link(tds[0])
            if link:
                record["blueprint_url"] = link

        out.append(record)

    return out

def main():
    ap = argparse.ArgumentParser(description="Scrape ARC Raiders Blueprints table into JSON objects.")
    ap.add_argument("--url", default=URL, help="Source URL")
    ap.add_argument("-o", "--output", default="blueprints_full.json", help="Output JSON path")
    args = ap.parse_args()

    try:
        html = fetch_html(args.url)
        entries = parse_table(html)

        # Write list[dict]
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(entries, f, ensure_ascii=False, indent=2)

        print(f"Wrote {len(entries)} rows to {out_path}")
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
