#!/usr/bin/env python3
"""
ArcTraiders trading ledger (standalone, no Discord integration).

Backends:
- SheetsBackend: persists to Google Sheets (API v4)
- MemoryBackend: in-memory store for local/offline testing

Env:
  ARC_BACKEND=memory|sheets      (default: sheets)
  GOOGLE_SA_JSON_PATH=./service_account.json
  GOOGLE_SHEET_ID=<spreadsheet id>

Tabs:
  ActiveTrades, CompletedTrades
"""

import os
import uuid
import datetime as dt
from typing import List, Dict, Any, Optional, Tuple

# ----------------------------
# Canonical schema / constants
# ----------------------------

HEADERS: List[str] = [
    "offer_id", "status", "item_raw", "item_norm",
    "offerer_id", "offerer_name", "accepter_id", "accepter_name",
    "created_ts", "accepted_ts", "completed_ts",
    "notes", "guild_id", "channel_id",
]
ACTIVE_TAB = "ActiveTrades"
COMPLETED_TAB = "CompletedTrades"

STATUS_OPEN = "OPEN"
STATUS_ACCEPTED = "ACCEPTED"
STATUS_COMPLETED = "COMPLETED"
STATUS_CANCELLED = "CANCELLED"

STATUS_ALLOWED = [STATUS_OPEN, STATUS_ACCEPTED, STATUS_COMPLETED, STATUS_CANCELLED]

STATUS_COL = 1  # 0-based index
TIMESTAMP_COLS = [8, 9, 10]             # created_ts, accepted_ts, completed_ts
ID_COLS = [0, 4, 6, 12, 13]             # offer_id, offerer_id, accepter_id, guild_id, channel_id

# ----------------------------
# Utilities
# ----------------------------

def utcnow() -> str:
    return dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"

def col_a1(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

# ----------------------------
# Backend interface
# ----------------------------

class Backend:
    def ensure_initialized(self) -> None: ...
    def append_active(self, row: List[Any]) -> None: ...
    def update_active_cell(self, row_idx_1: int, col_idx_0: int, value: Any) -> None: ...
    def read_active_row(self, row_idx_1: int) -> Dict[str, Any]: ...
    def find_active_row_index(self, offer_id: str) -> Optional[int]: ...
    def read_active_all(self) -> List[Dict[str, Any]]: ...
    def append_completed(self, row: List[Any]) -> None: ...
    # --- cleanup support ---
    def read_active_rows_with_indices(self) -> List[Tuple[int, List[Any]]]: ...
    def append_completed_rows(self, rows: List[List[Any]]) -> None: ...
    def delete_active_rows(self, row_indices_1based: List[int]) -> None: ...
    # --- completed reading ---
    def read_completed_all(self) -> List[Dict[str, Any]]: ...

# ----------------------------
# Memory backend (offline use)
# ----------------------------

class MemoryBackend(Backend):
    def __init__(self):
        self.active: List[List[Any]] = [HEADERS[:]]   # header row at index 0
        self.completed: List[List[Any]] = [HEADERS[:]]

    def ensure_initialized(self) -> None:
        pass

    def append_active(self, row: List[Any]) -> None:
        self.active.append(row)

    def update_active_cell(self, row_idx_1: int, col_idx_0: int, value: Any) -> None:
        self.active[row_idx_1 - 1][col_idx_0] = value

    def read_active_row(self, row_idx_1: int) -> Dict[str, Any]:
        row = self.active[row_idx_1 - 1]
        return dict(zip(HEADERS, row))

    def find_active_row_index(self, offer_id: str) -> Optional[int]:
        for i, row in enumerate(self.active, start=1):
            if i == 1:
                continue
            if row and row[0] == offer_id:
                return i
        return None

    def read_active_all(self) -> List[Dict[str, Any]]:
        rows = self.active[1:]
        return [dict(zip(HEADERS, r)) for r in rows]

    def append_completed(self, row: List[Any]) -> None:
        self.completed.append(row)

    # --- cleanup support ---

    def read_active_rows_with_indices(self) -> List[Tuple[int, List[Any]]]:
        out: List[Tuple[int, List[Any]]] = []
        for i in range(2, len(self.active) + 1):
            out.append((i, self.active[i - 1][:]))
        return out

    def append_completed_rows(self, rows: List[List[Any]]) -> None:
        for r in rows:
            self.completed.append(r[:])

    def delete_active_rows(self, row_indices_1based: List[int]) -> None:
        for idx in sorted(row_indices_1based, reverse=True):
            del self.active[idx - 1]

    # --- completed reading ---

    def read_completed_all(self) -> List[Dict[str, Any]]:
        rows = self.completed[1:]
        return [dict(zip(HEADERS, r)) for r in rows]

# ----------------------------
# Google Sheets backend
# ----------------------------

class SheetsBackend(Backend):
    def __init__(self):
        self.sa_path = os.getenv("GOOGLE_SA_JSON_PATH", "./service_account.json")
        self.spreadsheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
        if not self.spreadsheet_id:
            raise RuntimeError("GOOGLE_SHEET_ID must be set for Sheets backend.")
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(self.sa_path, scopes=scopes)
        self.service = build("sheets", "v4", credentials=creds)

    # --- Setup helpers ---

    def _get_sheet_map(self) -> Dict[str, int]:
        meta = self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
        return {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets", [])}

    def _add_sheet_if_missing(self, title: str, cols: int) -> int:
        sheet_map = self._get_sheet_map()
        if title in sheet_map:
            return sheet_map[title]
        reqs = [{"addSheet": {"properties": {"title": title, "gridProperties": {"rowCount": 1000, "columnCount": cols}}}}]
        resp = self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id, body={"requests": reqs}
        ).execute()
        return resp["replies"][0]["addSheet"]["properties"]["sheetId"]

    def _read_row_1(self, title: str) -> List[str]:
        rng = f"{title}!1:1"
        val = self.service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range=rng).execute()
        values = val.get("values", [])
        return values[0] if values else []

    def _write_headers_strict(self, title: str) -> None:
        row1 = self._read_row_1(title)
        if row1 != HEADERS:
            rng = f"{title}!A1:{col_a1(len(HEADERS))}1"
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=rng,
                valueInputOption="RAW",
                body={"values": [HEADERS]},
            ).execute()

    def _freeze_header(self, sheet_id: int):
        reqs = [{"updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount"
        }}]
        self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": reqs}).execute()

    def _basic_filter(self, sheet_id: int, col_count: int):
        reqs = [{"setBasicFilter": {"filter": {
            "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 10000, "startColumnIndex": 0, "endColumnIndex": col_count}
        }}}]
        self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": reqs}).execute()

    def _text_format_cols(self, sheet_id: int, columns: List[int]):
        reqs = []
        for c in columns:
            reqs.append({
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 10000,
                              "startColumnIndex": c, "endColumnIndex": c + 1},
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "TEXT"}}},
                    "fields": "userEnteredFormat.numberFormat"
                }
            })
        self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": reqs}).execute()

    def _status_validation(self, sheet_id: int):
        reqs = [{
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 10000,
                          "startColumnIndex": STATUS_COL, "endColumnIndex": STATUS_COL + 1},
                "cell": {"dataValidation": {"condition": {
                    "type": "ONE_OF_LIST", "values": [{"userEnteredValue": v} for v in STATUS_ALLOWED]
                }, "strict": True, "showCustomUi": True}},
                "fields": "dataValidation"
            }
        }]
        self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": reqs}).execute()

    def _bold_header_and_autosize(self, sheet_id: int, col_count: int):
        reqs = [
            {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                                      "startColumnIndex": 0, "endColumnIndex": col_count},
                            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                            "fields": "userEnteredFormat.textFormat.bold"}},
            {"autoResizeDimensions": {"dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS",
                                                     "startIndex": 0, "endIndex": col_count}}}
        ]
        self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": reqs}).execute()

    # --- Backend interface ---

    def ensure_initialized(self) -> None:
        col_count = len(HEADERS)
        active_id = self._add_sheet_if_missing(ACTIVE_TAB, col_count)
        completed_id = self._add_sheet_if_missing(COMPLETED_TAB, col_count)
        self._write_headers_strict(ACTIVE_TAB)
        self._write_headers_strict(COMPLETED_TAB)
        for sid in (active_id, completed_id):
            self._freeze_header(sid)
            self._basic_filter(sid, col_count)
            self._status_validation(sid)
            self._text_format_cols(sid, list(set(ID_COLS + TIMESTAMP_COLS)))
            self._bold_header_and_autosize(sid, col_count)

    def append_active(self, row: List[Any]) -> None:
        rng = f"{ACTIVE_TAB}!A:A"
        self.service.spreadsheets().values().append(
            spreadsheetId=self.spreadsheet_id,
            range=rng, valueInputOption="RAW", insertDataOption="INSERT_ROWS",
            body={"values": [row]}
        ).execute()

    def update_active_cell(self, row_idx_1: int, col_idx_0: int, value: Any) -> None:
        cell = f"{ACTIVE_TAB}!{col_a1(col_idx_0 + 1)}{row_idx_1}"
        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id, range=cell, valueInputOption="RAW",
            body={"values": [[value]]}
        ).execute()

    def read_active_row(self, row_idx_1: int) -> Dict[str, Any]:
        rng = f"{ACTIVE_TAB}!A{row_idx_1}:{col_a1(len(HEADERS))}{row_idx_1}"
        val = self.service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range=rng).execute()
        row = val.get("values", [[]])[0]
        return dict(zip(HEADERS, row))

    def find_active_row_index(self, offer_id: str) -> Optional[int]:
        rng = f"{ACTIVE_TAB}!A:A"
        val = self.service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range=rng).execute()
        values = val.get("values", [])
        for i, row in enumerate(values, start=1):
            if row and row[0] == offer_id:
                return i
        return None

    def read_active_all(self) -> List[Dict[str, Any]]:
        rng = f"{ACTIVE_TAB}!A2:{col_a1(len(HEADERS))}"
        val = self.service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range=rng).execute()
        rows = val.get("values", [])
        return [dict(zip(HEADERS, r)) for r in rows]

    def append_completed(self, row: List[Any]) -> None:
        rng = f"{COMPLETED_TAB}!A:A"
        self.service.spreadsheets().values().append(
            spreadsheetId=self.spreadsheet_id,
            range=rng, valueInputOption="RAW", insertDataOption="INSERT_ROWS",
            body={"values": [row]}
        ).execute()

    # --- cleanup support ---

    def read_active_rows_with_indices(self) -> List[Tuple[int, List[Any]]]:
        rng = f"{ACTIVE_TAB}!A2:{col_a1(len(HEADERS))}"
        val = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id, range=rng
        ).execute()
        rows = val.get("values", [])
        out: List[Tuple[int, List[Any]]] = []
        for offset, r in enumerate(rows, start=2):
            out.append((offset, r + [""] * (len(HEADERS) - len(r))))
        return out

    def append_completed_rows(self, rows: List[List[Any]]) -> None:
        if not rows:
            return
        rng = f"{COMPLETED_TAB}!A:A"
        self.service.spreadsheets().values().append(
            spreadsheetId=self.spreadsheet_id,
            range=rng,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows},
        ).execute()

    def delete_active_rows(self, row_indices_1based: List[int]) -> None:
        if not row_indices_1based:
            return
        requests = []
        sheet_id = self._get_sheet_map()[ACTIVE_TAB]
        for idx in sorted(row_indices_1based, reverse=True):
            requests.append({
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": idx - 1,
                        "endIndex": idx
                    }
                }
            })
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={"requests": requests}
        ).execute()

    # --- completed reading ---

    def read_completed_all(self) -> List[Dict[str, Any]]:
        rng = f"{COMPLETED_TAB}!A2:{col_a1(len(HEADERS))}"
        val = self.service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range=rng).execute()
        rows = val.get("values", [])
        return [dict(zip(HEADERS, r)) for r in rows]

# ----------------------------
# TradeLedger orchestrator
# ----------------------------

class TradeLedger:
    def __init__(self, backend: Optional[Backend] = None):
        if backend is not None:
            self.backend = backend
        else:
            mode = os.getenv("ARC_BACKEND", "sheets").lower()
            self.backend = MemoryBackend() if mode == "memory" else SheetsBackend()
        self.backend.ensure_initialized()

    # --- temporary housekeeping hook ---
    def _sweep_active_to_completed(self) -> None:
        """
        Move any COMPLETED and CANCELLED rows that still linger in ActiveTrades to CompletedTrades.
        Safe, idempotent. Called before/after actions until formal flow is added.
        """
        try:
            self.cleanup(include_cancelled=True)
        except Exception:
            pass

    def offer(self, offerer_id: str, offerer_name: str, item_raw: str,
              qty: int = 1, notes: str = "", guild_id: str = "", channel_id: str = "") -> str:
        self._sweep_active_to_completed()
        if qty <= 0:
            raise ValueError("qty must be positive")
        offer_id = str(uuid.uuid4())[:8]
        now = utcnow()
        row = [
            offer_id, STATUS_OPEN, item_raw, item_raw,
            str(offerer_id), str(offerer_name), "", "",
            now, "", "",
            notes, str(guild_id), str(channel_id),
        ]
        self.backend.append_active(row)
        self._sweep_active_to_completed()
        return offer_id

    def accept(self, offer_id: str, accepter_id: str, accepter_name: str) -> bool:
        self._sweep_active_to_completed()
        idx = self.backend.find_active_row_index(offer_id)
        if not idx:
            return False
        row = self.backend.read_active_row(idx)
        if row.get("status") != STATUS_OPEN:
            return False
        now = utcnow()
        self.backend.update_active_cell(idx, HEADERS.index("status"), STATUS_ACCEPTED)
        self.backend.update_active_cell(idx, HEADERS.index("accepter_id"), str(accepter_id))
        self.backend.update_active_cell(idx, HEADERS.index("accepter_name"), str(accepter_name))
        self.backend.update_active_cell(idx, HEADERS.index("accepted_ts"), now)
        self._sweep_active_to_completed()
        return True

    def complete(self, offer_id: str) -> bool:
        self._sweep_active_to_completed()
        idx = self.backend.find_active_row_index(offer_id)
        if not idx:
            return False
        row = self.backend.read_active_row(idx)
        if row.get("status") not in (STATUS_ACCEPTED, STATUS_OPEN):
            return False
        now = utcnow()
        self.backend.update_active_cell(idx, HEADERS.index("status"), STATUS_COMPLETED)
        self.backend.update_active_cell(idx, HEADERS.index("completed_ts"), now)
        row["status"] = STATUS_COMPLETED
        row["completed_ts"] = now
        self.backend.append_completed([row.get(h, "") for h in HEADERS])
        self._sweep_active_to_completed()
        return True

    def last(self, n: int = 5) -> List[Dict[str, Any]]:
        """
        Backward-compatible: return the last N 'in-progress' (ActiveTrades) by created_ts.
        Prefer using recent() for split views.
        """
        view = self.recent(n_active=n, n_completed=0)
        return view["in_progress"]

    def recent(self, n_active: int = 5, n_completed: int = 5) -> Dict[str, List[Dict[str, Any]]]:
        """
        Return a split view:
          - in_progress: OPEN/ACCEPTED from ActiveTrades, sorted by created_ts desc
          - completed:   COMPLETED/CANCELLED from CompletedTrades (and any lingering in ActiveTrades),
                        sorted by completed_ts desc

        The method also sweeps to ensure ActiveTrades is tidy before reading.
        """
        self._sweep_active_to_completed()

        # In-progress from Active
        active_rows = self.backend.read_active_all()
        in_progress = [r for r in active_rows if (r.get("status") in (STATUS_OPEN, STATUS_ACCEPTED))]
        in_progress.sort(key=lambda r: r.get("created_ts", ""), reverse=True)
        if n_active >= 0:
            in_progress = in_progress[:n_active]

        # Completed primarily from Completed tab
        completed_rows = self.backend.read_completed_all()
        # Guard: if anything still in Active is completed/cancelled, fold it in (should be rare post-sweep)
        lingering_done = [r for r in active_rows if (r.get("status") in (STATUS_COMPLETED, STATUS_CANCELLED))]
        completed_all = completed_rows + lingering_done
        # Prefer completed_ts, fallback to accepted_ts/created_ts chain
        def completed_key(r: Dict[str, Any]) -> str:
            return r.get("completed_ts") or r.get("accepted_ts") or r.get("created_ts") or ""
        completed_all.sort(key=completed_key, reverse=True)
        if n_completed >= 0:
            completed_all = completed_all[:n_completed]

        return {"in_progress": in_progress, "completed": completed_all}

    def cleanup(self, include_cancelled: bool = False) -> Dict[str, int]:
        """
        Move COMPLETED (and optionally CANCELLED) rows from ActiveTrades to CompletedTrades,
        then delete them from ActiveTrades.

        Returns: {"moved": X, "deleted": X, "skipped": Y}
        """
        target_statuses = {STATUS_COMPLETED}
        if include_cancelled:
            target_statuses.add(STATUS_CANCELLED)

        rows_with_idx = self.backend.read_active_rows_with_indices()
        to_move: List[List[Any]] = []
        to_delete: List[int] = []
        skipped = 0

        for idx_1, row in rows_with_idx:
            row = (row + [""] * (len(HEADERS) - len(row)))[:len(HEADERS)]
            rec = dict(zip(HEADERS, row))
            status = rec.get("status", "").upper()
            if status in target_statuses:
                to_move.append([rec.get(h, "") for h in HEADERS])
                to_delete.append(idx_1)
            else:
                skipped += 1

        self.backend.append_completed_rows(to_move)
        self.backend.delete_active_rows(to_delete)

        return {"moved": len(to_move), "deleted": len(to_delete), "skipped": skipped}

# ----------------------------
# CLI demo / cleanup
# ----------------------------

def _demo():
    ledger = TradeLedger()  # uses ARC_BACKEND
    trade_id = ledger.offer("123", "Doug", "Atlas Chassis", qty=1, notes="any rare part")
    print("Offered:", trade_id)
    ledger.accept(trade_id, "456", "Ava")
    ledger.complete(trade_id)

    view = ledger.recent(n_active=3, n_completed=3)
    print("In-Progress:")
    for r in view["in_progress"]:
        print(r.get("offer_id"), r.get("status"), r.get("item_raw"), r.get("created_ts"))
    print("Completed:")
    for r in view["completed"]:
        print(r.get("offer_id"), r.get("status"), r.get("item_raw"), r.get("completed_ts") or r.get("accepted_ts") or r.get("created_ts"))

def _cleanup_cli():
    ledger = TradeLedger()
    stats = ledger.cleanup(include_cancelled=True)
    print(f"Cleanup: moved={stats['moved']} deleted={stats['deleted']} skipped={stats['skipped']}")

if __name__ == "__main__":
    # ARC_ACTION=cleanup -> run cleanup; otherwise run demo
    if os.getenv("ARC_ACTION", "").lower() == "cleanup":
        _cleanup_cli()
    else:
        _demo()
