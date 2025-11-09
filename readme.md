# ARC Traiders Bot

ARC Traiders is a lightweight Discord bot designed to help small player communities in *ARC Raiders* organize and track non-commercial item trades.  
It provides a transparent, easy way to offer, accept, and complete trades while maintaining a simple shared record of a communities collective inventory.

---

## Project Goal

The purpose of ARC Traiders is to facilitate fair, non-commercial item exchange within Discord servers by:

- Simplifying how players post offers, accept, and complete trades.  
- Keeping active and completed trades visible and logged for everyone.  
- Using Google Sheets for lightweight transparency and recordkeeping.  
- Enriching trade data with blueprint metadata scraped directly from the [ARC Raiders Wiki](https://arcraiders.wiki/wiki/Blueprints).

This project is entirely non-commercial and community-driven.  
It is not affiliated with or endorsed by Embark Studios or any commercial entity.
Use of this bot for commercial activity is STRICTLY FORBIDDEN.

---

## How It Works

### Discord Bot

| Command | Description |
|----------|--------------|
| `/offer <item>` | Create a new trade offer. |
| `/accept <query>` | Accept an offer using fuzzy matching for blueprint and player name (for example, `bluedevilfan's anvil`). |
| `/complete <offer>` | Mark a trade as completed. |
| `/last [int]` / `/last` | Show recent completed trades. |

All trade data is mirrored in a Google Sheet with two tabs:  
`ActiveTrades` and `CompletedTrades`.

### Data Scraper

- `scripts/table_scraper.py` fetches the Blueprints table from the ARC Raiders Wiki.  
- Outputs normalized JSON in `data/blueprints_full.json`.
- These files feed the bot’s fuzzy-matching system so item names are recognized consistently.

### Persistence

- Trades are stored in Google Sheets using a service-account key.  
- Local JSON files maintain canonical blueprint references for normalization.

---

## Setup

### Requirements

- Python 3.10 or newer  
- Discord bot token  
- Google Sheets service account credentials

### Installation

```bash
git clone https://github.com/<ryantagonize>/arcTraiders.git
cd arcTraiders
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
