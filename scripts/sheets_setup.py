#!/usr/bin/env python3
"""
Initialize ArcTraiders trade worksheets.

What this script does:
1) Ensures two sheets (tabs): ActiveTrades, CompletedTrades.
2) Ensures headers are present in strict order (rewrites if missing/mismatched).
3) Applies classic setup (no attempt to create real Google Sheets Tables):
   - Freeze header row
   - Basic filter over the table
   - Data validation for `status` column
   - Plain-text formats for IDs and timestamps
   - Bold header row + auto-resize columns

Run:
  export GOOGLE_SA_JSON_PATH="./service_account.json"
  export GOOGLE_SHEET_ID="<your spreadsheet id>"
  python scripts/sheets_setup.py
"""

import os
import sys
from typing import Tuple, List

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
SPREADSHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "ArcTraiders Trade Ledger").strip()
SA_PATH = os.getenv("GOOGLE_SA_JSON_PATH", "./service_account.json")

ACTIVE_TAB = "ActiveTrades"
COMPLETED_TAB = "CompletedTrades"

HEADERS: List[str] = [
    "offer_id", "status", "item_raw", "item_norm",
    "offerer_id", "offerer_name", "accepter_id", "accepter_name",
    "created_ts", "accepted_ts", "completed_ts",
    "notes", "guild_id", "channel_id",
]

STATUS_ALLOWED = ["OPEN", "ACCEPTED", "COMPLETED", "CANCELLED"]

# Column index helpers (0-based for API requests)
STATUS_COL = 1
TIMESTAMP_COLS = [8, 9, 10]             # created_ts, accepted_ts, completed_ts
ID_COLS = [0, 4, 6, 12, 13]             # offer_id, offerer_id, accepter_id, guild_id, channel_id


def _sheets_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SA_PATH, scopes=scopes)
    return build("sheets", "v4", credentials=creds)


def _get_spreadsheet_id(service) -> str:
    """Prefer explicit ID; otherwise open by name (not implemented)."""
    if SPREADSHEET_ID:
        return SPREADSHEET_ID
    print("ERROR: Set GOOGLE_SHEET_ID env var (preferred).", file=sys.stderr)
    sys.exit(2)


def _get_sheet_map(service, spreadsheet_id: str) -> dict:
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = meta.get("sheets", [])
    return {s["properties"]["title"]: s["properties"]["sheetId"] for s in sheets}


def _add_sheet_if_missing(service, spreadsheet_id: str, title: str, cols: int) -> int:
    sheet_map = _get_sheet_map(service, spreadsheet_id)
    if title in sheet_map:
        return sheet_map[title]
    reqs = [{
        "addSheet": {
            "properties": {"title": title, "gridProperties": {"rowCount": 1000, "columnCount": cols}}
        }
    }]
    resp = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": reqs}
    ).execute()
    sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]
    return sheet_id


def _read_row_1(service, spreadsheet_id: str, title: str) -> List[str]:
    rng = f"{title}!1:1"
    val = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = val.get("values", [])
    return values[0] if values else []


def _write_headers_strict(service, spreadsheet_id: str, title: str) -> None:
    row1 = _read_row_1(service, spreadsheet_id, title)
    if row1 != HEADERS:
        rng = f"{title}!A1:{_col_a1(len(HEADERS))}1"
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="RAW",
            body={"values": [HEADERS]},
        ).execute()


def _col_a1(n: int) -> str:
    # 1->A, 2->B ...
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _freeze_header(service, spreadsheet_id: str, sheet_id: int):
    reqs = [{"updateSheetProperties": {
        "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
        "fields": "gridProperties.frozenRowCount"
    }}]
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()


def _basic_filter(service, spreadsheet_id: str, sheet_id: int, col_count: int):
    reqs = [{
        "setBasicFilter": {
            "filter": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 10000,
                    "startColumnIndex": 0,
                    "endColumnIndex": col_count
                }
            }
        }
    }]
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()


def _text_format_cols(service, spreadsheet_id: str, sheet_id: int, columns: List[int]):
    reqs = []
    for c in columns:
        reqs.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,          # skip header
                    "endRowIndex": 10000,
                    "startColumnIndex": c,
                    "endColumnIndex": c + 1
                },
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "TEXT"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        })
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()


def _status_validation(service, spreadsheet_id: str, sheet_id: int):
    reqs = [{
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": 10000,
                "startColumnIndex": STATUS_COL,
                "endColumnIndex": STATUS_COL + 1
            },
            "cell": {
                "dataValidation": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": v} for v in STATUS_ALLOWED]
                    },
                    "strict": True,
                    "showCustomUi": True
                }
            },
            "fields": "dataValidation"
        }
    }]
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()


def _bold_header_and_autosize(service, spreadsheet_id: str, sheet_id: int, col_count: int):
    reqs = [
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": 0, "endColumnIndex": col_count},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold"
            }
        },
        {
            "autoResizeDimensions": {
                "dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS",
                               "startIndex": 0, "endIndex": col_count}
            }
        }
    ]
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()


def ensure_trade_sheets() -> Tuple[int, int]:
    """
    Idempotent initializer (classic setup only; no addTable):
      - ensures ActiveTrades and CompletedTrades sheets exist
      - enforces strict header order
      - applies header freeze, filter, validation, text formats, bold, auto-resize
    Returns: (active_sheet_id, completed_sheet_id)
    """
    if not SPREADSHEET_ID and not SPREADSHEET_NAME:
        print("Set GOOGLE_SHEET_ID or GOOGLE_SHEET_NAME.", file=sys.stderr)
        sys.exit(2)

    service = _sheets_service()
    spreadsheet_id = _get_spreadsheet_id(service)

    col_count = len(HEADERS)
    active_id = _add_sheet_if_missing(service, spreadsheet_id, ACTIVE_TAB, col_count)
    completed_id = _add_sheet_if_missing(service, spreadsheet_id, COMPLETED_TAB, col_count)

    _write_headers_strict(service, spreadsheet_id, ACTIVE_TAB)
    _write_headers_strict(service, spreadsheet_id, COMPLETED_TAB)

    for sheet_id in (active_id, completed_id):
        _freeze_header(service, spreadsheet_id, sheet_id)
        _basic_filter(service, spreadsheet_id, sheet_id, col_count)
        _status_validation(service, spreadsheet_id, sheet_id)
        _text_format_cols(service, spreadsheet_id, sheet_id, list(set(ID_COLS + TIMESTAMP_COLS)))
        _bold_header_and_autosize(service, spreadsheet_id, sheet_id, col_count)

    print("Initialized sheets (classic setup):")
    print(f" - {ACTIVE_TAB} (sheetId={active_id})")
    print(f" - {COMPLETED_TAB} (sheetId={completed_id})")
    return active_id, completed_id


if __name__ == "__main__":
    ensure_trade_sheets()
