import requests
from bs4 import BeautifulSoup
import json
import time

URL = "https://arcraiders.wiki/wiki/Blueprints"
OUTPUT_FILE = "blueprint_names.json"

def fetch_html(url):
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.text

def parse_table(html):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"class": "wikitable sortable jquery-tablesorter"})
    rows = table.find_all("tr")
    header_cells = [th.get_text(strip=True) for th in rows[0].find_all("th")]

    result = []
    for row in rows[1:]:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if not cells or len(cells) < len(header_cells):
            continue
        obj = dict(zip(header_cells, cells))
        result.append(obj)
    return result

def extract_blueprint_names(entries):
    # If all you need is the “Blueprint Name” column for your normalization list:
    return [entry["Blueprint Name"] for entry in entries]

def save_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    html = fetch_html(URL)
    entries = parse_table(html)
    names = extract_blueprint_names(entries)
    save_json(names, OUTPUT_FILE)
    print(f"Saved {len(names)} blueprint names to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
