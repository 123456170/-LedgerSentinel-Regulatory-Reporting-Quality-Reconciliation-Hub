"""
LedgerSentinel — Regulatory Reporting Data Quality & Reconciliation Hub (local MVP).

Single-file Streamlit application. Synthetic demo data only. Decision support:
every financial output is explainable, traceable and requires human approval.
No API keys. Works offline after install. SQLite persistence + hash-chained audit.
On first launch it seeds a realistic dataset, starts a live ingestion simulation
and renders a downloadable demo film (MP4, locally via ffmpeg) inside the app.

Windows PowerShell — exact commands:
  python -m venv .venv
  .venv\\Scripts\\Activate.ps1
  pip install -r requirements.txt
  streamlit run app.py

Reset to a clean demo: sidebar "Reset demo data", or:
  Remove-Item ledgersentinel_data\\hub.db*
"""
from __future__ import annotations

import io
import json
import math
import random
import shutil
import sqlite3
import hashlib
import zipfile
import importlib.util
from collections import deque
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from html import escape as esc
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# ──────────────────────────────────────────────────────────────────────────────
# Constants & palette
# ──────────────────────────────────────────────────────────────────────────────
APP_NAME = "LedgerSentinel"
APP_TAG = "Regulatory Reporting Quality & Reconciliation Hub"
APP_VER = "1.3.1-mvp"
SCHEMA_VER = 1
ORG = "Meridian Payments Ltd (SYNTHETIC)"
PERIOD = "2026-10"
P_START, P_END = date(2026, 10, 1), date(2026, 10, 31)
DUE = datetime(2026, 11, 14, 17, 0, 0)
SEED = 20261112

DATA_DIR = Path("ledgersentinel_data")
UP_DIR = DATA_DIR / "uploads"
EV_DIR = DATA_DIR / "evidence"
EX_DIR = DATA_DIR / "exports"
FILM_DIR = DATA_DIR / "demo"
BK_DIR = DATA_DIR / "backups"
DB_PATH = DATA_DIR / "hub.db"
for d in (DATA_DIR, UP_DIR, EV_DIR, EX_DIR, FILM_DIR, BK_DIR):
    d.mkdir(parents=True, exist_ok=True)

C = {
    "ink": "#0B1220", "ink2": "#0F172A", "surface": "#131C2E", "overlay": "#182338",
    "line": "#243147", "text": "#E6EDF7", "muted": "#93A4BD",
    "teal": "#2DD4BF", "blue": "#60A5FA", "violet": "#A78BFA", "amber": "#F5B93E",
    "red": "#F87171", "green": "#34D399", "pink": "#F472B6", "cyan": "#22D3EE",
}
SEV_COLOR = {"high": C["red"], "medium": C["amber"], "low": C["blue"]}
STATUS_COLOR = {
    "matched": C["green"], "break_amount": C["red"], "missing_posting": C["red"],
    "timing": C["amber"], "accepted": C["violet"],
    "Open": C["red"], "Investigating": C["amber"], "Resolved": C["blue"],
    "Verified": C["green"], "Closed": C["muted"], "Overridden": C["violet"],
    "Prepared": C["amber"], "Approved": C["green"], "Rejected": C["red"],
}
SYM = {"USD": "$", "EUR": "€", "GBP": "£"}
ACCOUNTS_SEED = [
    ("ACC-1001", "USD Settlement Operating", "USD", "settlement"),
    ("ACC-1002", "USD Merchant Payouts", "USD", "settlement"),
    ("ACC-2001", "EUR Card Acquiring", "EUR", "payments"),
    ("ACC-2002", "EUR Loan Repayments", "EUR", "lending"),
    ("ACC-3001", "GBP Treasury Current", "GBP", "treasury"),
    ("ACC-3002", "GBP Client Money", "GBP", "safeguarding"),
]
CATEGORIES = ["payment", "repayment", "fee", "refund", "transfer"]
CHANNELS = ["api", "batch", "manual", "card"]
EXPECTED_COLS = ["txn_id", "account_id", "currency", "amount", "value_date",
                 "booked_at", "category", "channel", "country"]

ROLES = {
    "admin":       {"name": "Amara Okafor",  "title": "Administrator", "icon": "🛠️",
                    "perms": {"import", "correct", "assign", "resolve", "verify", "override",
                              "approve", "prepare", "export", "rules", "admin", "restore", "prune"}},
    "analyst":     {"name": "Diego Ramírez", "title": "Analyst / Operator", "icon": "📥",
                    "perms": {"import", "correct", "assign", "resolve", "prepare", "export"}},
    "reviewer":    {"name": "Priya Nair",    "title": "Reviewer / Approver", "icon": "🔎",
                    "perms": {"assign", "resolve", "verify", "override", "approve", "export"}},
    "stakeholder": {"name": "Sam Chen",      "title": "Read-only Stakeholder", "icon": "👁️",
                    "perms": set()},
}

DEFAULT_RULES = {
    "tolerance_abs_cents": 50,
    "tolerance_rel": 0.001,
    "timing_grace_days": 2,
    "amount_cap_cents": 25_000_000,
    "anomaly_k": 3.5,
    "anomaly_window": 7,
}

DATA_DICT = [
    ("txn_id", "text", "—", "Unique transaction key", "SMP-/SIM-/TXN- prefix", "source file", "internal", "RQ-001/004 unique, required"),
    ("account_id", "text", "—", "Normalized account reference", "must exist in accounts", "reference catalog", "internal", "RQ-006 referential integrity"),
    ("currency", "text", "ISO-4217", "Transaction currency", "USD | EUR | GBP", "source file", "internal", "RQ-002/006 must match account"),
    ("amount", "decimal", "major units", "Positive gross amount", "0 < amount <= 250,000", "source file", "confidential", "RQ-003 range; stored as int cents"),
    ("value_date", "date", "YYYY-MM-DD", "Economic value date", f"{P_START} … {P_END}", "source file", "internal", "RQ-005 inside reporting period"),
    ("booked_at", "datetime", "ISO-8601", "Ledger booking timestamp", ">= value_date", "source file", "internal", "RC-102 timing grace 2 days"),
    ("category", "text", "—", "Business classification", " | ".join(CATEGORIES), "source file", "internal", "RQ-007 enum"),
    ("channel", "text", "—", "Origination channel", " | ".join(CHANNELS), "source file", "internal", "RQ-007 enum"),
    ("country", "text", "ISO-3166 a2", "Counterparty jurisdiction", "2 letters", "source file", "internal", "RQ-008 format"),
]

# FK-safe creation order: parents before children.
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS accounts(account_id TEXT PRIMARY KEY, name TEXT, currency TEXT, kind TEXT);
CREATE TABLE IF NOT EXISTS imports(
  id TEXT PRIMARY KEY, filename TEXT, sha256 TEXT, source_system TEXT, actor TEXT,
  rows_total INTEGER, rows_valid INTEGER, rows_rejected INTEGER, rows_skipped INTEGER,
  status TEXT, notes TEXT, created_at TEXT, schema_version INTEGER);
CREATE TABLE IF NOT EXISTS records(
  txn_id TEXT PRIMARY KEY, import_id TEXT REFERENCES imports(id), account_id TEXT,
  currency TEXT, amount_cents INTEGER, value_date TEXT, booked_at TEXT, category TEXT,
  channel TEXT, country TEXT, status TEXT, created_at TEXT, updated_at TEXT, schema_version INTEGER);
CREATE TABLE IF NOT EXISTS import_errors(
  id INTEGER PRIMARY KEY AUTOINCREMENT, import_id TEXT REFERENCES imports(id),
  row_num INTEGER, rule_id TEXT, col TEXT, message TEXT, fix TEXT, raw_json TEXT);
CREATE TABLE IF NOT EXISTS ledger(
  entry_id TEXT PRIMARY KEY, account_id TEXT, currency TEXT, value_date TEXT,
  amount_cents INTEGER, booked_at TEXT, batch_ref TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS exceptions(
  id TEXT PRIMARY KEY, kind TEXT, severity TEXT, rule_id TEXT, entity_type TEXT, entity_id TEXT,
  title TEXT, detail_json TEXT, status TEXT, assignee TEXT, resolution TEXT, override_reason TEXT,
  fingerprint TEXT, opened_at TEXT, updated_at TEXT, closed_by TEXT);
CREATE TABLE IF NOT EXISTS adjustments(
  id TEXT PRIMARY KEY, exception_id TEXT REFERENCES exceptions(id), account_id TEXT,
  currency TEXT, value_date TEXT, amount_cents INTEGER, reason TEXT, actor TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS audit(
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, actor TEXT, role TEXT, action TEXT,
  entity_type TEXT, entity_id TEXT, detail_json TEXT, prev_hash TEXT, hash TEXT);
CREATE TABLE IF NOT EXISTS lineage(
  id INTEGER PRIMARY KEY AUTOINCREMENT, entity_type TEXT, entity_id TEXT, source_type TEXT,
  source_id TEXT, source_file TEXT, source_row INTEGER, transform_version TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS rule_versions(
  version TEXT PRIMARY KEY, payload_json TEXT, note TEXT, activated_at TEXT, activated_by TEXT, active INTEGER);
CREATE TABLE IF NOT EXISTS exports(
  id TEXT PRIMARY KEY, period TEXT, status TEXT, prepared_by TEXT, approved_by TEXT, reason TEXT,
  path TEXT, pack_fp TEXT, created_at TEXT, decided_at TEXT);
CREATE TABLE IF NOT EXISTS evidence(
  id TEXT PRIMARY KEY, exception_id TEXT REFERENCES exceptions(id), filename TEXT,
  sha256 TEXT, path TEXT, actor TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS sim_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, tick INTEGER, account_id TEXT, currency TEXT,
  amount_cents INTEGER, ok INTEGER, anomaly TEXT, latency_ms REAL);
CREATE INDEX IF NOT EXISTS idx_rec_acct ON records(account_id, currency, value_date);
CREATE INDEX IF NOT EXISTS idx_led_acct ON ledger(account_id, currency, value_date);
CREATE INDEX IF NOT EXISTS idx_exc_status ON exceptions(status, severity);
"""

# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────
class HubError(Exception):
    """Domain / permission error surfaced safely to the UI."""

def now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def cj(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)

def sha_b(b: bytes | str) -> str:
    """SHA-256 hex digest; accepts text or bytes (text is UTF-8 encoded)."""
    if isinstance(b, str):
        b = b.encode("utf-8")
    return hashlib.sha256(b).hexdigest()

def sha_f(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()

def hid(prefix: str) -> str:
    import uuid
    return f"{prefix}-{uuid.uuid4().hex[:10].upper()}"

def to_cents(v) -> int:
    d = Decimal(str(v).replace(",", "").replace(" ", "").strip())
    if not d.is_finite():
        raise ValueError("non-finite amount")
    return int((d * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

def money(c: int, ccy: str = "USD") -> str:
    return f"{'-' if c < 0 else ''}{SYM.get(ccy, '')}{abs(c) / 100:,.2f}"

def pct(x: float) -> str:
    return f"{100 * x:.1f}%"

def parse_date(s: str) -> date:
    return date.fromisoformat(str(s).strip()[:10])

def parse_dt(s: str) -> datetime:
    return datetime.fromisoformat(str(s).strip().replace("Z", "+00:00")).replace(tzinfo=None)

def time_left() -> timedelta:
    """Never-negative countdown to the filing deadline (timedelta, not int)."""
    return max(timedelta(0), DUE - datetime.now())

# ──────────────────────────────────────────────────────────────────────────────
# Database layer
# ──────────────────────────────────────────────────────────────────────────────
def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA busy_timeout=8000")
    return con

def ensure_schema() -> None:
    """Idempotent: safe on a brand-new, empty, or partially-created database."""
    with closing(db()) as con:
        con.executescript(SCHEMA_SQL)
        con.commit()

def q(sql: str, p: tuple = ()) -> list[sqlite3.Row]:
    with closing(db()) as con:
        return con.execute(sql, p).fetchall()

def qdf(sql: str, p: tuple = ()) -> pd.DataFrame:
    rows = q(sql, p)
    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()

def x(sql: str, p: tuple = ()) -> None:
    with closing(db()) as con:
        con.execute(sql, p)
        con.commit()

def xm(sql: str, rows: list[tuple]) -> None:
    if not rows:
        return
    with closing(db()) as con:
        con.executemany(sql, rows)
        con.commit()

def meta_get(k: str, default=None):
    r = q("SELECT v FROM meta WHERE k=?", (k,))
    return json.loads(r[0]["v"]) if r else default

def meta_set(k: str, v) -> None:
    x("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, cj(v)))

# ──────────────────────────────────────────────────────────────────────────────
# Audit — append-only, hash-chained
# ──────────────────────────────────────────────────────────────────────────────
def chain_hash(prev: str, payload: str) -> str:
    return hashlib.sha256((prev + payload).encode("utf-8")).hexdigest()

def audit(action: str, etype: str, eid: str, detail: dict | None = None,
          actor_name: str | None = None, role_key: str | None = None) -> None:
    a, r = actor()
    actor_name = actor_name or a
    role_key = role_key or r
    ts = now_iso()
    payload = cj({"ts": ts, "actor": actor_name, "role": role_key, "action": action,
                  "entity_type": etype, "entity_id": eid, "detail": detail or {}})
    last = q("SELECT hash FROM audit ORDER BY id DESC LIMIT 1")
    prev = last[0]["hash"] if last else "GENESIS"
    x("INSERT INTO audit(ts,actor,role,action,entity_type,entity_id,detail_json,prev_hash,hash) "
      "VALUES(?,?,?,?,?,?,?,?,?)",
      (ts, actor_name, role_key, action, etype, eid, cj(detail or {}), prev, chain_hash(prev, payload)))

def chain_verify() -> tuple[bool, str, int]:
    rows = q("SELECT * FROM audit ORDER BY id ASC")
    prev = "GENESIS"
    for r in rows:
        payload = cj({"ts": r["ts"], "actor": r["actor"], "role": r["role"], "action": r["action"],
                      "entity_type": r["entity_type"], "entity_id": r["entity_id"],
                      "detail": json.loads(r["detail_json"])})
        if r["prev_hash"] != prev or chain_hash(prev, payload) != r["hash"]:
            return False, f"Chain broken at audit event #{r['id']}", len(rows)
        prev = r["hash"]
    return True, f"Verified {len(rows)} events · head {prev[:12]}…", len(rows)

# ──────────────────────────────────────────────────────────────────────────────
# Roles & permissions
# ──────────────────────────────────────────────────────────────────────────────
def actor() -> tuple[str, str]:
    rk = st.session_state.get("role", "analyst")
    return ROLES[rk]["name"], rk

def has(perm: str, role: str | None = None) -> bool:
    rk = role or st.session_state.get("role", "analyst")
    return perm in ROLES[rk]["perms"]

def require(perm: str) -> None:
    _, rk = actor()
    if not has(perm, rk):
        raise HubError(f"Permission denied: role '{ROLES[rk]['title']}' cannot perform '{perm}'. "
                       f"Switch persona in the sidebar.")

# ──────────────────────────────────────────────────────────────────────────────
# Rule catalog & versioning
# ──────────────────────────────────────────────────────────────────────────────
def rules() -> tuple[dict, str]:
    r = q("SELECT version, payload_json FROM rule_versions WHERE active=1")
    if not r:
        return dict(DEFAULT_RULES), "2026.11.1"
    return json.loads(r[0]["payload_json"]), r[0]["version"]

def rules_activate(payload: dict, note: str) -> str:
    require("rules")
    a, _ = actor()
    n = len(q("SELECT version FROM rule_versions")) + 1
    ver = f"2026.11.{n}"
    x("UPDATE rule_versions SET active=0")
    x("INSERT INTO rule_versions(version,payload_json,note,activated_at,activated_by,active) "
      "VALUES(?,?,?,?,?,1)", (ver, cj(payload), note, now_iso(), a))
    audit("rules_activated", "rule_version", ver, {"payload": payload, "note": note})
    return ver

def rules_rollback(version: str) -> None:
    require("rules")
    r = q("SELECT payload_json FROM rule_versions WHERE version=?", (version,))
    if not r:
        raise HubError(f"Unknown rule version {version}")
    x("UPDATE rule_versions SET active=0")
    x("UPDATE rule_versions SET active=1 WHERE version=?", (version,))
    audit("rules_rollback", "rule_version", version, {"restored_payload": json.loads(r[0]["payload_json"])})

# ──────────────────────────────────────────────────────────────────────────────
# Validation engine
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class Issue:
    rule_id: str
    col: str
    message: str
    fix: str
    severity: str = "medium"

def accounts_map() -> dict[str, str]:
    return {r["account_id"]: r["currency"] for r in q("SELECT account_id, currency FROM accounts")}

def validate_row(raw: dict, acct_ccy: dict[str, str], seen: set[str], rp: dict) -> tuple[dict | None, list[Issue]]:
    issues: list[Issue] = []
    for c in EXPECTED_COLS:
        if str(raw.get(c, "")).strip() == "":
            issues.append(Issue("RQ-001", c, f"Required field '{c}' is missing.",
                                f"Populate {c} from the source system extract.", "high"))
    if issues:
        return None, issues
    txn = str(raw["txn_id"]).strip()
    acct = str(raw["account_id"]).strip()
    ccy = str(raw["currency"]).strip().upper()
    if txn in seen:
        issues.append(Issue("RQ-004", "txn_id", f"Duplicate transaction key '{txn}'.",
                            "De-duplicate at source; re-import skips existing keys safely.", "high"))
    if acct not in acct_ccy:
        issues.append(Issue("RQ-006", "account_id", f"Unknown account '{acct}'.",
                            "Add account to the reference catalog or correct the key.", "high"))
    if ccy not in SYM:
        issues.append(Issue("RQ-002", "currency", f"Unsupported currency '{ccy}'.",
                            "Use ISO-4217 code USD, EUR or GBP.", "high"))
    elif acct in acct_ccy and acct_ccy[acct] != ccy:
        issues.append(Issue("RQ-006", "currency", f"Currency {ccy} contradicts account currency {acct_ccy[acct]}.",
                            "Verify account mapping; contradictory data is quarantined.", "high"))
    cents = None
    try:
        cents = to_cents(raw["amount"])
        if cents <= 0:
            issues.append(Issue("RQ-003", "amount", "Amount must be positive (refunds use category=refund).",
                                "Send gross positive amount with the refund category.", "high"))
        elif cents > rp["amount_cap_cents"]:
            issues.append(Issue("RQ-003", "amount",
                                f"Amount {money(cents, ccy)} exceeds cap {money(rp['amount_cap_cents'], ccy)}.",
                                "Split settlement batch or raise cap via governed rule change.", "high"))
    except Exception:
        issues.append(Issue("RQ-003", "amount", f"Amount {raw['amount']!r} is not a valid decimal.",
                            "Use plain decimal, e.g. 1234.56 (commas tolerated).", "high"))
    vd = None
    try:
        vd = parse_date(raw["value_date"])
        if not (P_START <= vd <= P_END):
            issues.append(Issue("RQ-005", "value_date", f"Value date {vd} outside period {PERIOD}.",
                                f"Route to the correct reporting period ({PERIOD}).", "medium"))
    except Exception:
        issues.append(Issue("RQ-005", "value_date", f"Invalid date '{raw['value_date']}'.",
                            "Use ISO format YYYY-MM-DD.", "high"))
    try:
        bk = parse_dt(raw["booked_at"])
        if vd and bk.date() < vd:
            issues.append(Issue("RQ-005", "booked_at", "booked_at precedes value_date.",
                                "Check source clock / timezone; booking cannot precede value.", "medium"))
    except Exception:
        issues.append(Issue("RQ-005", "booked_at", f"Invalid timestamp '{raw['booked_at']}'.",
                            "Use ISO-8601, e.g. 2026-10-14T09:00:00.", "high"))
    if str(raw["category"]).strip().lower() not in CATEGORIES:
        issues.append(Issue("RQ-007", "category", f"Unknown category '{raw['category']}'.",
                            f"Allowed: {', '.join(CATEGORIES)}.", "medium"))
    if str(raw["channel"]).strip().lower() not in CHANNELS:
        issues.append(Issue("RQ-007", "channel", f"Unknown channel '{raw['channel']}'.",
                            f"Allowed: {', '.join(CHANNELS)}.", "medium"))
    ctry = str(raw["country"]).strip().upper()
    if len(ctry) != 2 or not ctry.isalpha():
        issues.append(Issue("RQ-008", "country", f"Country '{raw['country']}' is not ISO-3166 alpha-2.",
                            "Use two letters, e.g. US, DE, GB.", "low"))
    if issues:
        return None, issues
    seen.add(txn)
    clean = {"txn_id": txn, "account_id": acct, "currency": ccy, "amount_cents": cents,
             "value_date": vd.isoformat(), "booked_at": parse_dt(raw["booked_at"]).isoformat(),
             "category": str(raw["category"]).lower(), "channel": str(raw["channel"]).lower(),
             "country": ctry}
    return clean, []

def validate_frame(df: pd.DataFrame, existing_ids: set[str], rp: dict) -> tuple[list[dict], list[dict], list[str]]:
    valid, errors, skipped = [], [], []
    acct_ccy = accounts_map()
    seen: set[str] = set()
    for i, raw in enumerate(df.to_dict("records")):
        txn = str(raw.get("txn_id", "")).strip()
        if txn and txn in existing_ids:
            skipped.append(txn)
            continue
        clean, issues = validate_row(raw, acct_ccy, seen, rp)
        if clean:
            clean["_row"] = i + 2
            valid.append(clean)
        for iss in issues:
            errors.append({"row_num": i + 2, "rule_id": iss.rule_id, "col": iss.col,
                           "message": iss.message, "fix": iss.fix, "severity": iss.severity,
                           "raw_json": cj(raw)})
    return valid, errors, skipped

# ──────────────────────────────────────────────────────────────────────────────
# Import service
# ──────────────────────────────────────────────────────────────────────────────
def parse_upload(name: str, data: bytes) -> pd.DataFrame:
    if len(data) == 0:
        raise HubError("File is empty — nothing to import.")
    if len(data) > 10 * 1024 * 1024:
        raise HubError("File exceeds the 10 MB MVP upload limit.")
    ext = Path(name).suffix.lower()
    try:
        if ext == ".csv":
            df = pd.read_csv(io.BytesIO(data), dtype=str, keep_default_na=False)
        elif ext == ".xlsx":
            df = pd.read_excel(io.BytesIO(data), dtype=str, engine="openpyxl").fillna("")
        else:
            raise HubError("Unsupported file type. Upload .csv or .xlsx.")
    except HubError:
        raise
    except Exception as e:
        raise HubError(f"Could not parse file: {e}")
    missing = [c for c in EXPECTED_COLS if c not in df.columns]
    extra = [c for c in df.columns if c not in EXPECTED_COLS]
    if missing:
        raise HubError(f"Schema mismatch — missing columns: {', '.join(missing)}. "
                       f"Unexpected columns: {', '.join(extra) or 'none'}. See the data dictionary below.")
    return df[EXPECTED_COLS]

def stage_preview(name: str, data: bytes) -> dict:
    df = parse_upload(name, data)
    rp, _ = rules()
    existing = {r["txn_id"] for r in q("SELECT txn_id FROM records")}
    valid, errors, skipped = validate_frame(df, existing, rp)
    sha = sha_b(data)
    return {"filename": name, "bytes": data, "sha": sha, "df": df,
            "valid": valid, "errors": errors, "skipped": skipped,
            "dup_file": bool(q("SELECT id FROM imports WHERE sha256=?", (sha,)))}

def commit_import(preview: dict, source_system: str) -> dict:
    require("import")
    a, _ = actor()
    imp_id = hid("IMP")
    stored = UP_DIR / f"{preview['sha']}{Path(preview['filename']).suffix.lower()}"
    if not stored.exists():
        stored.write_bytes(preview["bytes"])
    ts = now_iso()
    n_total = len(preview["df"])
    x("INSERT INTO imports(id,filename,sha256,source_system,actor,rows_total,rows_valid,rows_rejected,"
      "rows_skipped,status,notes,created_at,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
      (imp_id, preview["filename"], preview["sha"], source_system, a, n_total,
       len(preview["valid"]), len({e['row_num'] for e in preview['errors']}),
       len(preview["skipped"]), "committed",
       "duplicate checksum of earlier import" if preview["dup_file"] else "", ts, SCHEMA_VER))
    xm("INSERT OR IGNORE INTO records(txn_id,import_id,account_id,currency,amount_cents,value_date,"
       "booked_at,category,channel,country,status,created_at,updated_at,schema_version) "
       "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
       [(r["txn_id"], imp_id, r["account_id"], r["currency"], r["amount_cents"], r["value_date"],
         r["booked_at"], r["category"], r["channel"], r["country"], "validated", ts, ts, SCHEMA_VER)
        for r in preview["valid"]])
    xm("INSERT INTO import_errors(import_id,row_num,rule_id,col,message,fix,raw_json) VALUES(?,?,?,?,?,?,?)",
       [(imp_id, e["row_num"], e["rule_id"], e["col"], e["message"], e["fix"], e["raw_json"])
        for e in preview["errors"]])
    xm("INSERT INTO lineage(entity_type,entity_id,source_type,source_id,source_file,source_row,"
       "transform_version,created_at) VALUES(?,?,?,?,?,?,?,?)",
       [("record", r["txn_id"], "file_row", imp_id, preview["filename"], r["_row"], "tx-v1", ts)
        for r in preview["valid"]])
    audit("import_committed", "import", imp_id,
          {"file": preview["filename"], "sha256": preview["sha"], "valid": len(preview["valid"]),
           "rejected_rows": len({e['row_num'] for e in preview['errors']}), "skipped": len(preview["skipped"]),
           "source": source_system})
    return {"import_id": imp_id, "valid": len(preview["valid"]),
            "rejected": len({e['row_num'] for e in preview['errors']}), "skipped": len(preview["skipped"])}

# ──────────────────────────────────────────────────────────────────────────────
# Sample files
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def sample_valid_csv() -> bytes:
    rng = random.Random(7)
    rows = []
    for i in range(24):
        acct, _, ccy, _ = ACCOUNTS_SEED[i % len(ACCOUNTS_SEED)]
        d = P_START + timedelta(days=rng.randint(0, 30))
        rows.append({"txn_id": f"SMP-{i+1:04d}", "account_id": acct, "currency": ccy,
                     "amount": f"{rng.uniform(20, 4200):.2f}", "value_date": d.isoformat(),
                     "booked_at": (d + timedelta(days=1)).isoformat() + "T09:00:00",
                     "category": rng.choice(CATEGORIES), "channel": rng.choice(CHANNELS),
                     "country": rng.choice(["US", "DE", "GB", "FR", "IE"])})
    return pd.DataFrame(rows)[EXPECTED_COLS].to_csv(index=False).encode()

@st.cache_data(show_spinner=False)
def sample_broken_csv() -> bytes:
    base = {"txn_id": "BRK-0001", "account_id": "ACC-1001", "currency": "USD", "amount": "120.00",
            "value_date": "2026-10-10", "booked_at": "2026-10-11T09:00:00",
            "category": "payment", "channel": "api", "country": "US"}
    rows, defects = [], [
        {}, {"txn_id": "BRK-0001"}, {"currency": "XX"}, {"account_id": "ACC-9999"},
        {"currency": "EUR"}, {"amount": "abc"}, {"amount": "9999999.00"}, {"amount": "-5.00"},
        {"value_date": "2026-10-32"}, {"value_date": "2026-09-15"}, {"booked_at": "2026-10-01T00:00:00"},
        {"category": "crypto"}, {"country": "USA"}, {"txn_id": ""},
    ]
    for i, d in enumerate(defects):
        r = dict(base); r["txn_id"] = f"BRK-{i+1:04d}"; r.update(d); rows.append(r)
    return pd.DataFrame(rows)[EXPECTED_COLS].to_csv(index=False).encode()

@st.cache_data(show_spinner=False)
def sample_xlsx() -> bytes:
    df = pd.read_csv(io.BytesIO(sample_valid_csv()), dtype=str)
    df["txn_id"] = df["txn_id"].str.replace("SMP", "XLS")
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()

# ──────────────────────────────────────────────────────────────────────────────
# Reconciliation engine
# ──────────────────────────────────────────────────────────────────────────────
RECON_COLS = ["account_id", "currency", "value_date", "src_cents", "led_cents", "adj_cents",
              "var_cents", "tol_cents", "n_src", "n_led", "late_days", "status", "key"]

def load_frames() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    src = qdf("SELECT account_id,currency,value_date,amount_cents,booked_at FROM records")
    led = qdf("SELECT account_id,currency,value_date,amount_cents,booked_at,batch_ref FROM ledger")
    adj = qdf("SELECT account_id,currency,value_date,amount_cents FROM adjustments")
    return src, led, adj

def reconcile_core(src: pd.DataFrame, led: pd.DataFrame, adj: pd.DataFrame, rp: dict) -> pd.DataFrame:
    keys = ["account_id", "currency", "value_date"]
    def agg(df, cnt_name):
        if df.empty:
            return pd.DataFrame(columns=keys + ["cents", cnt_name, "max_booked"])
        g = df.groupby(keys).agg(cents=("amount_cents", "sum"), n=("amount_cents", "size"),
                                 max_booked=("booked_at", "max")).reset_index()
        return g.rename(columns={"n": cnt_name})
    s = agg(src, "n_src")
    l = agg(led, "n_led")
    a = (adj.groupby(keys).amount_cents.sum().reset_index().rename(columns={"amount_cents": "adj"})
         if not adj.empty else pd.DataFrame(columns=keys + ["adj"]))
    m = pd.merge(s, l, on=keys, how="outer", suffixes=("_s", "_l")).merge(a, on=keys, how="left")
    for col in ("cents_s", "cents_l", "adj", "n_src", "n_led"):
        m[col] = m[col].fillna(0).astype("int64")
    out = []
    grace = int(rp["timing_grace_days"])
    for _, r in m.iterrows():
        src_c, led_c, adj_c = int(r.cents_s), int(r.cents_l), int(r.adj)
        var = src_c - (led_c + adj_c)
        tol = max(int(rp["tolerance_abs_cents"]), math.ceil(rp["tolerance_rel"] * abs(src_c)))
        late_days = 0
        mb = r.get("max_booked_l")
        if isinstance(mb, str) and mb:
            try:
                late_days = max(0, (parse_dt(mb).date() - parse_date(r.value_date)).days)
            except Exception:
                late_days = 0
        if r.n_led == 0 and src_c != 0:
            status = "missing_posting"
        elif abs(var) > tol:
            status = "break_amount"
        elif late_days > grace:
            status = "timing"
        else:
            status = "matched"
        out.append({"account_id": r.account_id, "currency": r.currency, "value_date": r.value_date,
                    "src_cents": src_c, "led_cents": led_c, "adj_cents": adj_c, "var_cents": var,
                    "tol_cents": tol, "n_src": int(r.n_src), "n_led": int(r.n_led),
                    "late_days": late_days, "status": status,
                    "key": f"{r.account_id}|{r.currency}|{r.value_date}"})
    if not out:
        return pd.DataFrame(columns=RECON_COLS)
    return pd.DataFrame(out).sort_values(keys).reset_index(drop=True)

def accepted_keys() -> set[str]:
    rows = q("SELECT entity_id FROM exceptions WHERE status='Overridden' AND entity_type='recon_pair'")
    return {r["entity_id"] for r in rows}

def recon_all() -> tuple[pd.DataFrame, str, dict, str]:
    rp, ver = rules()
    src, led, adj = load_frames()
    pairs = reconcile_core(src, led, adj, rp)
    acc = accepted_keys()
    pairs.loc[(pairs.status != "matched") & pairs.key.isin(acc), "status"] = "accepted"
    fp = sha_b(cj({"s": pairs[["key", "src_cents"]].values.tolist(),
                   "l": pairs[["key", "led_cents"]].values.tolist(),
                   "a": pairs[["key", "adj_cents"]].values.tolist(), "rules": rp}).encode())[:16]
    return pairs, fp, rp, ver

def pair_explain(r: pd.Series, rp: dict, ver: str) -> dict:
    tol_formula = (f"tol = max(abs_floor, ceil(rel × |source|)) = max({money(rp['tolerance_abs_cents'], r.currency)}, "
                   f"ceil({rp['tolerance_rel']} × {money(abs(int(r.src_cents)), r.currency)})) = {money(int(r.tol_cents), r.currency)}")
    factors = []
    if r.status == "missing_posting":
        factors.append(f"No ledger batch found for {int(r.n_src)} source transactions — posting file likely missing.")
    if r.status == "break_amount":
        factors.append(f"Ledger differs by {money(int(r.var_cents), r.currency)} — check transposition, FX rounding or partial batch.")
    if r.status == "timing":
        factors.append(f"Ledger booked {int(r.late_days)} days after value date (grace {rp['timing_grace_days']}d) — late settlement file.")
    if r.status == "accepted":
        factors.append("Break formally accepted via reviewer override — see exception audit trail.")
    if r.adj_cents:
        factors.append(f"Correction adjustment {money(int(r.adj_cents), r.currency)} applied (append-only, evidenced).")
    return {"formula": "variance = source_total − (ledger_total + adjustments); matched iff |variance| <= tol AND timing within grace",
            "inputs": {"source": money(int(r.src_cents), r.currency), "ledger": money(int(r.led_cents), r.currency),
                       "adjustments": money(int(r.adj_cents), r.currency), "variance": money(int(r.var_cents), r.currency)},
            "tolerance": tol_formula, "rule_version": ver, "rules": ["RC-101", "RC-102"],
            "verdict": r.status, "factors": factors or ["Totals agree within tolerance and booking grace."],
            "warnings": [] if r.n_src else ["Ledger-only group: no source transactions found."],
            "uncertainty": "Deterministic arithmetic on imported data — no model uncertainty. Data-quality risk sits in source completeness."}

# ──────────────────────────────────────────────────────────────────────────────
# Anomaly baseline
# ──────────────────────────────────────────────────────────────────────────────
def robust_flags(series: pd.Series, window: int, k: float) -> tuple[pd.Series, pd.Series]:
    med = series.rolling(window, min_periods=3).median()
    mad = (series - med).abs().rolling(window, min_periods=3).median()
    denom = 1.4826 * mad + 1e-9
    z = (series - med) / denom
    return (z.abs() > k), z

def baseline_eval() -> tuple[pd.DataFrame, dict]:
    rp, _ = rules()
    src, _, _ = load_frames()
    truth = set(tuple(t) for t in meta_get("spike_truth", []))
    rows, tp, fp_, fn, tn = [], 0, 0, 0, 0
    for acct in sorted(src.account_id.unique()):
        d = (src[src.account_id == acct].groupby("value_date").amount_cents.sum()
             .reindex([(P_START + timedelta(days=i)).isoformat() for i in range(31)], fill_value=0))
        flags, z = robust_flags(d, rp["anomaly_window"], rp["anomaly_k"])
        hold = d.index[-10:]
        for day in hold:
            f = bool(flags.loc[day]); t = (acct, day) in truth
            tp += f and t; fp_ += f and not t; fn += (not f) and t; tn += (not f) and (not t)
            if f or t:
                rows.append({"account": acct, "date": day, "flagged": f, "injected_truth": t,
                             "robust_z": round(float(z.loc[day]) if pd.notna(z.loc[day]) else 0.0, 2)})
    prec = tp / (tp + fp_) if tp + fp_ else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    far = fp_ / (fp_ + tn) if fp_ + tn else 0.0
    return pd.DataFrame(rows), {"precision": prec, "recall": rec, "f1": f1, "false_alarm_rate": far,
                                "holdout": "last 10 days per account (time split)", "n_eval": 6 * 10}

# ──────────────────────────────────────────────────────────────────────────────
# Exceptions
# ──────────────────────────────────────────────────────────────────────────────
EXC_FLOW = {"Open": ["Investigating", "Overridden"], "Investigating": ["Resolved", "Overridden"],
            "Resolved": ["Verified", "Investigating"], "Verified": ["Closed"],
            "Closed": ["Open"], "Overridden": ["Open"]}
TARGET_PERM = {"Investigating": "assign", "Resolved": "resolve", "Verified": "verify",
               "Closed": "verify", "Overridden": "override", "Open": "override"}
RECON_KINDS = {"missing_posting", "amount_mismatch", "timing"}
CLEARED = {"Verified", "Closed", "Overridden"}

def exc_open(kind: str, severity: str, rule_id: str, etype: str, eid: str, title: str,
             detail: dict, fingerprint: str, opened_at: str | None = None,
             actor_name: str | None = None) -> str:
    if actor_name is None:
        require("resolve")
    dup = q("SELECT id FROM exceptions WHERE fingerprint=? AND status NOT IN ('Closed')", (fingerprint,))
    if dup:
        return dup[0]["id"]
    exc_id = hid("EXC")
    ts = opened_at or now_iso()
    x("INSERT INTO exceptions(id,kind,severity,rule_id,entity_type,entity_id,title,detail_json,status,"
      "assignee,resolution,override_reason,fingerprint,opened_at,updated_at,closed_by) "
      "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
      (exc_id, kind, severity, rule_id, etype, eid, title, cj(detail), "Open", "", "", "",
       fingerprint, ts, ts, ""))
    audit("exception_opened", "exception", exc_id,
          {"kind": kind, "severity": severity, "title": title, "entity": eid},
          actor_name=actor_name, role_key="admin" if actor_name == "system" else None)
    return exc_id

def exc_transition(exc_id: str, target: str, note: str = "") -> None:
    r = q("SELECT * FROM exceptions WHERE id=?", (exc_id,))
    if not r:
        raise HubError("Exception not found.")
    e = dict(r[0])
    if target not in EXC_FLOW.get(e["status"], []):
        raise HubError(f"Illegal transition {e['status']} -> {target}.")
    require(TARGET_PERM[target])
    a, _ = actor()
    if target == "Overridden" and len(note.strip()) < 20:
        raise HubError("Override requires a reason of at least 20 characters (audit evidence).")
    if target == "Resolved":
        if len(note.strip()) < 10:
            raise HubError("Resolution note must be at least 10 characters.")
        if e["kind"] in {"missing_posting", "amount_mismatch"}:
            if not q("SELECT id FROM adjustments WHERE exception_id=?", (exc_id,)):
                raise HubError("This break needs a correction adjustment (or a reviewer override) before resolution.")
    if target == "Verified" and e["kind"] in RECON_KINDS:
        pairs, _, _, _ = recon_all()
        pr = pairs[pairs.key == e["entity_id"]]
        if pr.empty or pr.iloc[0].status not in ("matched", "accepted"):
            raise HubError("Verification blocked: the reconciliation pair is still breaking. Apply the correction first.")
    closed_by = a if target in ("Verified", "Closed", "Overridden") else e["closed_by"]
    x("UPDATE exceptions SET status=?, updated_at=?, resolution=CASE WHEN ?!='' THEN ? ELSE resolution END, "
      "override_reason=CASE WHEN ?='Overridden' THEN ? ELSE override_reason END, closed_by=? WHERE id=?",
      (target, now_iso(), note, note, target, note, closed_by, exc_id))
    audit(f"exception_{target.lower()}", "exception", exc_id, {"from": e["status"], "note": note})

def add_adjustment(exc_id: str, amount_cents: int, reason: str) -> str:
    require("correct")
    a, _ = actor()
    r = q("SELECT * FROM exceptions WHERE id=?", (exc_id,))
    if not r:
        raise HubError("Exception not found.")
    e = dict(r[0])
    if e["kind"] not in {"missing_posting", "amount_mismatch"}:
        raise HubError("Adjustments apply to amount / missing-posting breaks only.")
    if len(reason.strip()) < 10:
        raise HubError("Correction reason must be at least 10 characters.")
    acct, ccy, vd = e["entity_id"].split("|")
    adj_id = hid("ADJ")
    x("INSERT INTO adjustments(id,exception_id,account_id,currency,value_date,amount_cents,reason,actor,created_at) "
      "VALUES(?,?,?,?,?,?,?,?,?)", (adj_id, exc_id, acct, ccy, vd, amount_cents, reason, a, now_iso()))
    x("INSERT INTO lineage(entity_type,entity_id,source_type,source_id,source_file,source_row,transform_version,created_at) "
      "VALUES('adjustment',?,'exception',?,'manual-correction',0,'adj-v1',?)", (adj_id, exc_id, now_iso()))
    audit("adjustment_posted", "adjustment", adj_id,
          {"exception": exc_id, "pair": e["entity_id"], "amount_cents": amount_cents, "reason": reason})
    if e["status"] in ("Open", "Investigating"):
        x("UPDATE exceptions SET status='Resolved', resolution=?, updated_at=? WHERE id=?",
            (f"Correction {adj_id} posted: {reason}", now_iso(), exc_id))
        audit("exception_resolved", "exception", exc_id, {"via_adjustment": adj_id})
    return adj_id

def fasttrack_demo() -> int:
    require("admin")
    n = 0
    pairs, _, _, _ = recon_all()
    for r in q("SELECT * FROM exceptions WHERE status NOT IN ('Verified','Closed','Overridden') ORDER BY opened_at"):
        e = dict(r)
        try:
            if e["status"] == "Open":
                exc_transition(e["id"], "Investigating", "Demo fast-track triage")
            if e["kind"] in {"missing_posting", "amount_mismatch"}:
                pr = pairs[pairs.key == e["entity_id"]]
                var = int(pr.iloc[0].var_cents) if not pr.empty else 0
                if var != 0:
                    add_adjustment(e["id"], var, "Demo fast-track: align ledger to evidenced source total")
                cur = q("SELECT status FROM exceptions WHERE id=?", (e["id"],))[0]["status"]
                if cur == "Resolved":
                    exc_transition(e["id"], "Verified", "Demo fast-track verification")
                    exc_transition(e["id"], "Closed", "Demo fast-track closure")
            elif e["kind"] == "timing":
                exc_transition(e["id"], "Overridden",
                               "Demo fast-track: timing accepted — funds confirmed settled T+5, evidence on file")
            else:
                exc_transition(e["id"], "Resolved", "Demo fast-track: quarantined / evidenced and documented")
                exc_transition(e["id"], "Verified", "Demo fast-track verification")
                exc_transition(e["id"], "Closed", "Demo fast-track closure")
            n += 1
        except HubError:
            continue
    audit("demo_fasttrack", "exception_queue", "ALL", {"resolved": n})
    return n

# ──────────────────────────────────────────────────────────────────────────────
# Submission packs
# ──────────────────────────────────────────────────────────────────────────────
def quality_score(pairs: pd.DataFrame) -> float:
    imp = q("SELECT COALESCE(SUM(rows_valid),0) v, COALESCE(SUM(rows_rejected),0) r FROM imports")
    v, rj = imp[0]["v"], imp[0]["r"]
    valid_rate = v / (v + rj) if v + rj else 1.0
    cov = float((pairs.status.isin(["matched", "accepted"])).mean()) if len(pairs) else 1.0
    excs = q("SELECT status FROM exceptions")
    res = sum(e["status"] in CLEARED for e in excs) / len(excs) if excs else 1.0
    return round(100 * (0.35 * valid_rate + 0.45 * cov + 0.20 * res), 1)

def readiness(pairs: pd.DataFrame) -> list[dict]:
    cov = float((pairs.status.isin(["matched", "accepted"])).mean()) if len(pairs) else 1.0
    excs = q("SELECT severity,status FROM exceptions")
    high_open = sum(e["severity"] == "high" and e["status"] not in CLEARED for e in excs)
    any_open = sum(e["status"] not in CLEARED for e in excs)
    checks = [
        {"id": "G1", "label": f"Reconciliation coverage >= 99.5% (now {pct(cov)})", "ok": cov >= 0.995,
         "detail": "Every source-to-ledger pair matched, corrected or formally accepted."},
        {"id": "G2", "label": f"No open high-severity exceptions (now {high_open})", "ok": high_open == 0,
         "detail": "High-severity breaks must be Verified, Closed or Overridden."},
        {"id": "G3", "label": f"Exception queue fully dispositioned (open: {any_open})", "ok": any_open == 0,
         "detail": "All exceptions carry a resolution, verification or override with evidence."},
    ]
    last = q("SELECT * FROM exports ORDER BY created_at DESC LIMIT 1")
    if last:
        _, fp, _, _ = recon_all()
        checks.append({"id": "G4", "label": "No data drift since preparation", "ok": fp == last[0]["pack_fp"],
                       "detail": f"Pack fingerprint {last[0]['pack_fp']} vs current {fp}."})
        checks.append({"id": "G5", "label": "Reviewer approval recorded (separation of duties)",
                       "ok": last[0]["status"] == "Approved",
                       "detail": f"Prepared by {last[0]['prepared_by']}; approver must differ."})
    return checks

def build_manifest(files: dict[str, bytes], pack_fp: str, ver: str) -> dict:
    return {"pack_fingerprint": pack_fp, "rules_version": ver, "schema_version": SCHEMA_VER,
            "generated_at": now_iso(), "period": PERIOD,
            "files": {name: {"sha256": sha_b(b), "bytes": len(b)} for name, b in files.items()},
            "notice": "SYNTHETIC DEMO DATA — decision support only; requires authorized human approval."}

def pack_prepare() -> str:
    require("prepare")
    a, _ = actor()
    pairs, fp, rp, ver = recon_all()
    gates = readiness(pairs)[:3]
    failed = [g["label"] for g in gates if not g["ok"]]
    if failed:
        raise HubError("Readiness gates failed: " + "; ".join(failed))
    pack_id = hid("PACK")
    disp = pairs.copy()
    for c in ["src_cents", "led_cents", "adj_cents", "var_cents", "tol_cents"]:
        disp[c] = disp[c] / 100
    summary = {"organization": ORG, "period": PERIOD, "prepared_by": a, "generated_at": now_iso(),
               "rules_version": ver, "records": int(q("SELECT COUNT(*) n FROM records")[0]["n"]),
               "pairs": len(pairs), "coverage": round(float(pairs.status.isin(['matched', 'accepted']).mean()), 4),
               "quality_score": quality_score(pairs), "pack_fingerprint": fp,
               "status_counts": pairs.status.value_counts().to_dict(),
               "disclaimer": "Synthetic demonstration dataset. Decision support — not regulatory advice. "
                             "Submission requires authorized human approval."}
    exc_df = qdf("SELECT id,kind,severity,status,title,resolution,override_reason,opened_at,updated_at FROM exceptions")
    aud_df = qdf("SELECT id,ts,actor,role,action,entity_type,entity_id,hash FROM audit ORDER BY id DESC LIMIT 500")
    dd_df = pd.DataFrame(DATA_DICT, columns=["field", "type", "unit", "meaning", "allowed", "source", "sensitivity", "rule"])
    files = {"summary.json": cj(summary).encode(),
             "reconciliation_totals.csv": disp.to_csv(index=False).encode(),
             "exceptions.csv": (exc_df.to_csv(index=False) if not exc_df.empty else "id\n").encode(),
             "audit_extract.csv": aud_df.to_csv(index=False).encode(),
             "data_dictionary.csv": dd_df.to_csv(index=False).encode(),
             "README.txt": (f"{APP_NAME} submission preparation pack\n{ORG}\nPeriod {PERIOD}\n\n"
                            "SYNTHETIC DATA — for evaluation only.\nDecision support: verify all figures and obtain "
                            "authorized human approval before any regulatory submission.\n").encode()}
    manifest = build_manifest(files, fp, ver)
    files["manifest.json"] = cj(manifest).encode()
    path = EX_DIR / f"submission_pack_{PERIOD}_{pack_id}.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for name, b in files.items():
            z.writestr(name, b)
    x("INSERT INTO exports(id,period,status,prepared_by,approved_by,reason,path,pack_fp,created_at,decided_at) "
      "VALUES(?,?,?,?,?,?,?,?,?,?)", (pack_id, PERIOD, "Prepared", a, "", "", str(path), fp, now_iso(), ""))
    audit("pack_prepared", "export", pack_id, {"fingerprint": fp, "files": list(files), "sha256": sha_f(path)})
    return pack_id

def pack_decide(pack_id: str, approve: bool, reason: str) -> None:
    require("approve")
    a, _ = actor()
    r = q("SELECT * FROM exports WHERE id=?", (pack_id,))
    if not r:
        raise HubError("Pack not found.")
    p = dict(r[0])
    if p["prepared_by"] == a:
        raise HubError("Separation of duties: the preparer cannot approve their own pack. Switch to a Reviewer persona.")
    if len(reason.strip()) < 10:
        raise HubError("Decision reason must be at least 10 characters.")
    _, fp, _, _ = recon_all()
    if approve and fp != p["pack_fp"]:
        raise HubError("Data drift detected since preparation — re-prepare the pack before approval.")
    status = "Approved" if approve else "Rejected"
    x("UPDATE exports SET status=?, approved_by=?, reason=?, decided_at=? WHERE id=?",
      (status, a, reason, now_iso(), pack_id))
    audit(f"pack_{status.lower()}", "export", pack_id, {"reason": reason, "approver": a})

def pack_bytes(pack_row: dict) -> bytes:
    path = Path(pack_row["path"])
    data = path.read_bytes()
    if pack_row["status"] == "Approved":
        buf = io.BytesIO(data)
        with zipfile.ZipFile(buf, "a", zipfile.ZIP_DEFLATED) as z:
            z.writestr("approval.json", cj({"approved_by": pack_row["approved_by"],
                                            "decided_at": pack_row["decided_at"],
                                            "reason": pack_row["reason"], "pack": pack_row["id"]}))
        data = buf.getvalue()
    return data

# ──────────────────────────────────────────────────────────────────────────────
# Seed
# ──────────────────────────────────────────────────────────────────────────────
def seed_if_empty() -> bool:
    ensure_schema()
    if meta_get("seeded"):
        return False
    rng = random.Random(SEED)
    with closing(db()) as con:
        con.execute("PRAGMA foreign_keys=OFF")
        for t in ("sim_events", "evidence", "exports", "rule_versions", "lineage", "audit",
                  "adjustments", "exceptions", "import_errors", "records", "ledger",
                  "imports", "accounts", "meta"):
            con.execute(f"DELETE FROM {t}")
        try:
            con.execute("DELETE FROM sqlite_sequence")
        except sqlite3.OperationalError:
            pass
        con.commit()
        con.execute("PRAGMA foreign_keys=ON")
    xm("INSERT OR IGNORE INTO accounts(account_id,name,currency,kind) VALUES(?,?,?,?)", ACCOUNTS_SEED)
    x("INSERT INTO rule_versions(version,payload_json,note,activated_at,activated_by,active) "
      "VALUES(?,?,?,?,?,1)", ("2026.11.1", cj(DEFAULT_RULES), "Initial approved rule set", now_iso(), "system"))
    src_rows, spikes = [], [("ACC-1001", "2026-10-09"), ("ACC-2001", "2026-10-17"), ("ACC-3001", "2026-10-24")]
    biz_days = [P_START + timedelta(days=i) for i in range(31)
                if (P_START + timedelta(days=i)).weekday() < 5]
    imp_a, imp_b = "IMP-SEED-CORE", "IMP-SEED-GW"
    for imp_id, fn, sysname in [(imp_a, "corebank_2026-10.csv", "CoreBank CSV"),
                                (imp_b, "gateway_2026-10.xlsx", "Payments Gateway XLSX")]:
        x("INSERT INTO imports(id,filename,sha256,source_system,actor,rows_total,rows_valid,rows_rejected,"
          "rows_skipped,status,notes,created_at,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
          (imp_id, fn, sha_b(fn.encode()), sysname, "system", 0, 0, 0, 0, "committed",
           "Synthetic seed extract" + (" — arrived 2026-11-03 (late)" if imp_id == imp_b else ""),
           now_iso(), SCHEMA_VER))
    n = 0
    for acct, _, ccy, _ in ACCOUNTS_SEED:
        imp_id = imp_a if acct.startswith(("ACC-1", "ACC-3")) else imp_b
        for d in biz_days:
            for _ in range(rng.randint(2, 5)):
                n += 1
                amt = int(rng.lognormvariate(math.log(90000), 0.9))
                amt = max(1500, min(amt, 24_000_000))
                src_rows.append((f"TXN-{n:05d}", imp_id, acct, ccy, amt, d.isoformat(),
                                 (d + timedelta(days=1)).isoformat() + "T09:00:00",
                                 rng.choice(CATEGORIES), rng.choice(CHANNELS),
                                 rng.choice(["US", "DE", "GB", "FR", "IE", "NL"])))
    for acct, day in spikes:
        n += 1
        ccy = dict((a[0], a[2]) for a in ACCOUNTS_SEED)[acct]
        src_rows.append((f"TXN-{n:05d}", imp_a, acct, ccy, 21_500_000, day,
                         (date.fromisoformat(day) + timedelta(days=1)).isoformat() + "T09:00:00",
                         "transfer", "batch", "GB"))
    xm("INSERT INTO records(txn_id,import_id,account_id,currency,amount_cents,value_date,booked_at,"
       "category,channel,country,status,created_at,updated_at,schema_version) "
       "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
       [r + ("validated", now_iso(), now_iso(), SCHEMA_VER) for r in src_rows])
    xm("INSERT INTO lineage(entity_type,entity_id,source_type,source_id,source_file,source_row,transform_version,created_at) "
       "VALUES('record',?,'synthetic_seed',?,'seed-generator',?,'tx-v1',?)",
       [(r[0], r[1], i + 2, now_iso()) for i, r in enumerate(src_rows)])
    groups: dict[tuple, dict] = {}
    for r in src_rows:
        k = (r[2], r[3], r[5])
        g = groups.setdefault(k, {"cents": 0, "n": 0})
        g["cents"] += r[4]; g["n"] += 1
    led_rows, missing_total = [], 0
    for (acct, ccy, vd), g in sorted(groups.items()):
        amt, booked = g["cents"], (date.fromisoformat(vd) + timedelta(days=1)).isoformat() + "T17:00:00"
        if acct == "ACC-1002" and ccy == "USD" and vd == "2026-10-14":
            missing_total = amt
            continue
        if acct == "ACC-2001" and vd == "2026-10-21":
            amt += 31055
        if acct == "ACC-3001" and vd == "2026-10-27":
            booked = "2026-11-02T17:00:00"
        if acct == "ACC-1001" and vd in ("2026-10-06", "2026-10-13", "2026-10-20", "2026-10-28"):
            amt += rng.choice([-18, -9, 11, 16])
        led_rows.append((hid("LED"), acct, ccy, vd, amt, booked, f"BAT-{acct}-{vd[5:7]}{vd[8:10]}", now_iso()))
    xm("INSERT INTO ledger(entry_id,account_id,currency,value_date,amount_cents,booked_at,batch_ref,created_at) "
       "VALUES(?,?,?,?,?,?,?,?)", led_rows)
    rej = [(imp_b, 118, "RQ-004", "txn_id", "Duplicate transaction key 'TXN-00042'.",
            "De-duplicate at source; safe re-import skips existing keys.", cj({"txn_id": "TXN-00042"})),
           (imp_b, 233, "RQ-003", "amount", "Amount $9,999,999.00 exceeds cap $250,000.00.",
            "Split settlement batch or raise cap via governed rule change.", cj({"amount": "9999999.00"}))]
    xm("INSERT INTO import_errors(import_id,row_num,rule_id,col,message,fix,raw_json) VALUES(?,?,?,?,?,?,?)", rej)
    for imp_id in (imp_a, imp_b):
        nv = sum(1 for r in src_rows if r[1] == imp_id)
        nr = 2 if imp_id == imp_b else 0
        x("UPDATE imports SET rows_total=?, rows_valid=?, rows_rejected=? WHERE id=?", (nv + nr, nv, nr, imp_id))
    meta_set("spike_truth", spikes)
    meta_set("seed_fingerprint", sha_b(cj([r[:6] for r in src_rows]).encode())[:16])
    audit("database_seeded", "database", "hub", {"records": len(src_rows), "ledger_entries": len(led_rows),
                                                 "synthetic": True}, actor_name="system", role_key="admin")
    def pair_key(a, c, d): return f"{a}|{c}|{d}"
    exc_open("missing_posting", "high", "RC-101", "recon_pair", pair_key("ACC-1002", "USD", "2026-10-14"),
             "Missing ledger posting — USD Merchant Payouts, Oct 14",
             {"variance_cents": missing_total, "expected_batch": "BAT-ACC-1002-1014",
              "hint": f"Ledger batch absent; source total {money(missing_total, 'USD')} unmatched."},
             "recon:" + pair_key("ACC-1002", "USD", "2026-10-14") + ":missing_posting",
             "2026-11-05T08:12:00Z", actor_name="system")
    exc_open("amount_mismatch", "high", "RC-101", "recon_pair", pair_key("ACC-2001", "EUR", "2026-10-21"),
             "Amount mismatch — EUR Card Acquiring, Oct 21 (-EUR 310.55)",
             {"variance_cents": -31055, "hint": "Classic digit transposition (310.55 vs 301.55 pattern)."},
             "recon:" + pair_key("ACC-2001", "EUR", "2026-10-21") + ":amount_mismatch",
             "2026-11-06T10:40:00Z", actor_name="system")
    e3 = exc_open("timing", "medium", "RC-102", "recon_pair", pair_key("ACC-3001", "GBP", "2026-10-27"),
                  "Late ledger booking — GBP Treasury, Oct 27 (T+6)",
                  {"late_days": 6, "grace_days": 2, "hint": "Totals agree; settlement file arrived late."},
                  "recon:" + pair_key("ACC-3001", "GBP", "2026-10-27") + ":timing",
                  "2026-11-07T09:05:00Z", actor_name="system")
    exc_open("late_source", "medium", "RQ-005", "import", imp_b,
             "Late source file — Payments Gateway October extract",
             {"arrived": "2026-11-03", "expected": "2026-11-01",
              "hint": "SLA breach; confirm completeness with the gateway team."},
             "late_source:" + imp_b, "2026-11-08T14:22:00Z", actor_name="system")
    exc_open("validation", "high", "RQ-003", "import_row", f"{imp_b}:233",
             "Out-of-range amount quarantined ($9,999,999.00)",
             {"rule": "RQ-003", "cap_cents": DEFAULT_RULES["amount_cap_cents"],
              "hint": "Row excluded from validated records; corrected re-import required."},
             "validation:" + imp_b + ":233", "2026-11-08T15:01:00Z", actor_name="system")
    exc_open("validation", "medium", "RQ-004", "import_row", f"{imp_b}:118",
             "Duplicate transaction key quarantined (TXN-00042)",
             {"rule": "RQ-004", "hint": "Safe re-import strategy skips existing keys — no overwrite."},
             "validation:" + imp_b + ":118", "2026-11-09T08:45:00Z", actor_name="system")
    src_df, _, _ = load_frames()
    d = (src_df[src_df.account_id == "ACC-1001"].groupby("value_date").amount_cents.sum()
         .reindex([(P_START + timedelta(days=i)).isoformat() for i in range(31)], fill_value=0))
    rp, _ = rules()
    flags, z = robust_flags(d, rp["anomaly_window"], rp["anomaly_k"])
    spike_day = "2026-10-09"
    exc_open("anomaly", "medium", "AN-201", "daily_total", f"ACC-1001|{spike_day}",
             "Volume anomaly — USD Settlement Operating, Oct 09",
             {"robust_z": round(float(z.loc[spike_day]), 2), "threshold_k": rp["anomaly_k"],
              "window_days": rp["anomaly_window"], "day_total_cents": int(d.loc[spike_day]),
              "window_median_cents": float(d.rolling(rp["anomaly_window"]).median().loc[spike_day]),
              "method": "rolling median +/- k*1.4826*MAD (deterministic baseline, not a trained model)",
              "hint": "Large transfer batch; confirm treasury intent before sign-off."},
             "anomaly:ACC-1001:" + spike_day, "2026-11-10T11:30:00Z", actor_name="system")
    x("UPDATE exceptions SET status='Investigating', assignee='Priya Nair', updated_at=? WHERE id=?",
      (now_iso(), e3))
    audit("exception_assigned", "exception", e3, {"assignee": "Priya Nair"}, actor_name="system", role_key="admin")
    meta_set("seeded", True)
    return True

# ──────────────────────────────────────────────────────────────────────────────
# Live simulation
# ──────────────────────────────────────────────────────────────────────────────
def init_state() -> None:
    ss = st.session_state
    ss.setdefault("role", "analyst")
    ss.setdefault("page", "🛰️ Command Center")
    ss.setdefault("live", True)
    ss.setdefault("sim_interval", 3)
    ss.setdefault("sim_batch", 8)
    ss.setdefault("sim_anom", 8)
    ss.setdefault("sim_seed", 42)
    ss.setdefault("sim_ccy", "All")
    ss.setdefault("feed", deque(maxlen=240))
    ss.setdefault("staged", [])
    ss.setdefault("tick", 0)
    ss.setdefault("inject_next", False)
    ss.setdefault("sim_ok", 0)
    ss.setdefault("sim_bad", 0)
    ss.setdefault("saved_views", {})
    ss.setdefault("test_results", None)
    ss.setdefault("clear_upload", False)

def sim_tick() -> None:
    ss = st.session_state
    ss.tick += 1
    rng = random.Random(ss.sim_seed * 1_000_003 + ss.tick)
    rp, _ = rules()
    acct_ccy = accounts_map()
    pool = [a for a in ACCOUNTS_SEED if ss.sim_ccy == "All" or a[2] == ss.sim_ccy]
    batch_rows = []
    for i in range(int(ss.sim_batch)):
        acct, _, ccy, _ = pool[rng.randrange(len(pool))]
        vd = P_START + timedelta(days=rng.randint(0, 30))
        amt = max(1500, min(int(rng.lognormvariate(math.log(80000), 0.8)), 20_000_000))
        anomaly = ""
        if ss.inject_next or rng.random() < ss.sim_anom / 100:
            anomaly = rng.choice(["amount", "missing", "late", "corrupt"])
            ss.inject_next = False
        raw = {"txn_id": f"SIM-{ss.sim_seed}-{ss.tick}-{i}", "account_id": acct, "currency": ccy,
               "amount": f"{amt / 100:.2f}", "value_date": vd.isoformat(),
               "booked_at": (vd + timedelta(days=1)).isoformat() + "T09:00:00",
               "category": rng.choice(CATEGORIES), "channel": "api",
               "country": rng.choice(["US", "DE", "GB"])}
        latency = max(8.0, rng.gauss(46, 12))
        if anomaly == "corrupt":
            raw["currency"] = "XX"
            clean, issues = validate_row(raw, acct_ccy, set(), rp)
            ss.sim_bad += 1
            ss.feed.append({"tick": ss.tick, "ok": False, "acct": acct, "ccy": ccy, "amt": amt,
                            "anomaly": anomaly, "rule": issues[0].rule_id if issues else "RQ-002",
                            "latency": latency})
            batch_rows.append((now_iso(), ss.tick, acct, ccy, amt, 0, anomaly, latency))
        else:
            clean, _ = validate_row(raw, acct_ccy, set(), rp)
            if clean:
                clean["_anomaly"] = anomaly
                ss.staged.append(clean)
                ss.sim_ok += 1
                ss.feed.append({"tick": ss.tick, "ok": True, "acct": acct, "ccy": ccy, "amt": amt,
                                "anomaly": anomaly, "rule": "", "latency": latency})
                batch_rows.append((now_iso(), ss.tick, acct, ccy, amt, 1, anomaly, latency))
    xm("INSERT INTO sim_events(ts,tick,account_id,currency,amount_cents,ok,anomaly,latency_ms) "
       "VALUES(?,?,?,?,?,?,?,?)", batch_rows)
    x("DELETE FROM sim_events WHERE id NOT IN (SELECT id FROM sim_events ORDER BY id DESC LIMIT 600)")

def sim_commit() -> dict:
    require("import")
    ss = st.session_state
    if not ss.staged:
        raise HubError("Nothing staged — let the live feed run for a few ticks first.")
    a, _ = actor()
    rows = ss.staged[:]
    imp_id = hid("IMP")
    ts = now_iso()
    fn = f"live_simulator_seed{ss.sim_seed}_tick{ss.tick}.csv"
    clean_rows = [{"txn_id": r["txn_id"], "account_id": r["account_id"], "currency": r["currency"],
                   "amount": f"{r['amount_cents'] / 100:.2f}", "value_date": r["value_date"],
                   "booked_at": r["booked_at"], "category": r["category"], "channel": r["channel"],
                   "country": r["country"]} for r in rows]
    data = pd.DataFrame(clean_rows)[EXPECTED_COLS].to_csv(index=False).encode()
    sha = sha_b(data)
    (UP_DIR / f"{sha}.csv").write_bytes(data)
    x("INSERT INTO imports(id,filename,sha256,source_system,actor,rows_total,rows_valid,rows_rejected,"
      "rows_skipped,status,notes,created_at,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
      (imp_id, fn, sha, "Live Simulator (replay)", a, len(rows), len(rows), 0, 0, "committed",
       "Committed from live simulation staging buffer", ts, SCHEMA_VER))
    xm("INSERT OR IGNORE INTO records(txn_id,import_id,account_id,currency,amount_cents,value_date,booked_at,"
       "category,channel,country,status,created_at,updated_at,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
       [(r["txn_id"], imp_id, r["account_id"], r["currency"], r["amount_cents"], r["value_date"],
         r["booked_at"], r["category"], r["channel"], r["country"], "validated", ts, ts, SCHEMA_VER) for r in rows])
    xm("INSERT INTO lineage(entity_type,entity_id,source_type,source_id,source_file,source_row,transform_version,created_at) "
       "VALUES('record',?,'simulation',?,?,?,'tx-v1',?)",
       [(r["txn_id"], imp_id, fn, i + 2, ts) for i, r in enumerate(rows)])
    led_new, breaks = [], 0
    for r in rows:
        an = r.get("_anomaly", "")
        if an == "missing":
            breaks += 1
            continue
        amt = r["amount_cents"]
        booked = r["booked_at"]
        if an == "amount":
            amt = int(amt * 1.03) + 137
            breaks += 1
        if an == "late":
            booked = (parse_date(r["value_date"]) + timedelta(days=6)).isoformat() + "T17:00:00"
            breaks += 1
        led_new.append((hid("LED"), r["account_id"], r["currency"], r["value_date"], amt, booked,
                        f"SIM-{imp_id[-6:]}", ts))
    xm("INSERT INTO ledger(entry_id,account_id,currency,value_date,amount_cents,booked_at,batch_ref,created_at) "
       "VALUES(?,?,?,?,?,?,?,?)", led_new)
    audit("simulation_committed", "import", imp_id,
          {"records": len(rows), "ledger_entries": len(led_new), "engineered_breaks": breaks,
           "seed": ss.sim_seed, "tick": ss.tick})
    ss.staged = []
    return {"import_id": imp_id, "records": len(rows), "breaks": breaks}

# ──────────────────────────────────────────────────────────────────────────────
# Demo film
# ──────────────────────────────────────────────────────────────────────────────
FILM_MP4 = FILM_DIR / "ledgersentinel_demo.mp4"
FILM_GIF = FILM_DIR / "ledgersentinel_demo.gif"

def film_ready() -> Path | None:
    if FILM_MP4.exists():
        return FILM_MP4
    if FILM_GIF.exists():
        return FILM_GIF
    return None

def render_film(duration: int = 14, fps: int = 12, res: str = "800×450",
                scenes: tuple = (1, 2, 3, 4), prog=None) -> Path:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import imageio.v2 as iio

    w, h = (int(v) for v in res.split("×"))
    total = duration * fps
    pairs, fp, _, ver = recon_all()
    src, led, adj = load_frames()
    var_by_acct = pairs.groupby("account_id").var_cents.sum() / 100
    if var_by_acct.empty:
        var_by_acct = pd.Series(0.0, index=[a[0] for a in ACCOUNTS_SEED])
    approved = q("SELECT id FROM exports WHERE status='Approved' LIMIT 1")
    stamp = f"APPROVED · PACK {fp.upper()}" if approved else f"PACK FINGERPRINT {fp.upper()}"
    rng = np.random.default_rng(SEED)
    ticks = 60
    ok_stream = rng.integers(4, 12, ticks).cumsum()
    bad_stream = (rng.random(ticks) < 0.12).cumsum()
    burndown = np.maximum(0, 7 - np.linspace(0, 7, 40) + rng.normal(0, 0.15, 40).cumsum() * 0.15)

    plt.rcParams.update({"font.family": "DejaVu Sans", "text.color": C["text"],
                         "axes.edgecolor": C["line"], "axes.labelcolor": C["muted"],
                         "xtick.color": C["muted"], "ytick.color": C["muted"]})

    def new_fig():
        return plt.figure(figsize=(w / 100, h / 100), dpi=100, facecolor=C["ink"])

    def chrome(ax, title, sub):
        ax.set_facecolor(C["ink2"])
        for s in ax.spines.values():
            s.set_color(C["line"])
        ax.grid(True, color=C["line"], alpha=0.35, linewidth=0.6)
        ax.set_title(title, color=C["text"], fontsize=13, fontweight="bold", loc="left", pad=10)
        ax.text(0, 1.06, sub, transform=ax.transAxes, color=C["muted"], fontsize=8)

    def footer(f, scene_name, p):
        f.text(0.03, 0.035, f"◆ {APP_NAME.upper()}  ·  {APP_TAG}", color=C["teal"], fontsize=8, fontweight="bold")
        f.text(0.97, 0.035, "SYNTHETIC DEMO · DECISION SUPPORT", color=C["muted"], fontsize=7, ha="right")
        f.text(0.97, 0.955, scene_name, color=C["violet"], fontsize=8, ha="right", fontweight="bold")
        bar = plt.Rectangle((0.03, 0.012), 0.94 * p, 0.006, transform=f.transFigure, color=C["teal"], alpha=0.9)
        f.add_artist(bar)

    scene_frames = {s: max(1, total // len(scenes)) for s in scenes}
    writer, gif_frames, out_path = None, [], FILM_MP4
    try:
        writer = iio.get_writer(FILM_MP4, fps=fps, codec="libx264", quality=7,
                                pixelformat="yuv420p", macro_block_size=1,
                                ffmpeg_params=["-movflags", "+faststart"])
    except Exception:
        out_path = FILM_GIF

    idx = 0
    try:
        for s in scenes:
            nf = scene_frames[s]
            for j in range(nf):
                t = j / max(1, nf - 1)
                ease = 1 - (1 - t) ** 3
                f = new_fig()
                if s == 1:
                    ax = f.add_axes([0.07, 0.14, 0.86, 0.68]); ax.axis("off")
                    ax.text(0, 0.86, f"◆ {APP_NAME}", color=C["teal"], fontsize=26, fontweight="bold")
                    ax.text(0, 0.72, APP_TAG, color=C["text"], fontsize=12)
                    ax.text(0, 0.60, f"{ORG}  ·  Period {PERIOD}", color=C["muted"], fontsize=9)
                    n_rec = int(q("SELECT COUNT(*) n FROM records")[0]["n"])
                    for k, (lab, val, col) in enumerate([
                            ("VALIDATED RECORDS", int(n_rec * ease), C["teal"]),
                            ("RECON PAIRS", int(len(pairs) * ease), C["blue"]),
                            ("BREAKS DETECTED", int((pairs.status != 'matched').sum() * ease), C["red"]),
                            ("QUALITY SCORE", quality_score(pairs) * ease, C["green"])]):
                        x0 = 0.0 + k * 0.25
                        ax.text(x0, 0.34, f"{val:,.0f}" if k < 3 else f"{val:.1f}", color=col,
                                fontsize=21, fontweight="bold")
                        ax.text(x0, 0.24, lab, color=C["muted"], fontsize=7.5)
                    xs = np.linspace(0, 1, max(2, int(40 * ease)))
                    ax.plot(xs, 0.08 + 0.06 * np.sin(xs * 9) * ease, color=C["violet"], lw=2)
                    name = "01 · SEEDED & LIVE"
                elif s == 2:
                    ax = f.add_axes([0.09, 0.16, 0.85, 0.62]); chrome(ax, "Live ingestion monitor", "validated vs flagged · replay simulation")
                    npts = max(2, int(ticks * ease))
                    ax.fill_between(range(npts), ok_stream[:npts], color=C["teal"], alpha=0.35)
                    ax.plot(range(npts), ok_stream[:npts], color=C["teal"], lw=2, label="validated")
                    ax.fill_between(range(npts), bad_stream[:npts], color=C["red"], alpha=0.45)
                    ax.plot(range(npts), bad_stream[:npts], color=C["red"], lw=2, label="flagged")
                    ax.scatter([npts - 1], [ok_stream[npts - 1]], color=C["teal"], s=60, zorder=5)
                    ax.legend(loc="upper left", facecolor=C["overlay"], edgecolor=C["line"], fontsize=8)
                    ax.set_xlim(0, ticks); ax.set_ylim(0, ok_stream[-1] * 1.1)
                    name = "02 · INGEST & VALIDATE"
                elif s == 3:
                    ax = f.add_axes([0.10, 0.16, 0.84, 0.60]); chrome(ax, "Source → ledger reconciliation", f"rules {ver} · tolerance ±0.50 / 0.1%")
                    vals = var_by_acct.values.copy()
                    shown = vals * (1 - min(1.0, max(0.0, (t - 0.5) * 2)))
                    colors = [C["red"] if abs(v) > 0.5 and t < 0.9 else C["green"] for v in shown]
                    ax.bar(range(len(shown)), shown, color=colors, alpha=0.9, width=0.62)
                    ax.axhline(0, color=C["muted"], lw=1)
                    ax.axhspan(-0.5, 0.5, color=C["green"], alpha=0.12)
                    ax.set_xticks(range(len(shown))); ax.set_xticklabels(var_by_acct.index, fontsize=7, rotation=18)
                    big = int(np.argmax(np.abs(vals)))
                    if t < 0.55:
                        ax.annotate(f"BREAK {vals[big]:+,.2f}", (big, vals[big]),
                                    textcoords="offset points", xytext=(0, 14), ha="center",
                                    color=C["amber"], fontsize=9, fontweight="bold",
                                    arrowprops=dict(arrowstyle="->", color=C["amber"]))
                    else:
                        ax.text(0.5, 0.9, "CORRECTIONS POSTED · EVIDENCED", transform=ax.transAxes,
                                ha="center", color=C["green"], fontsize=10, fontweight="bold", alpha=min(1, (t - 0.5) * 3))
                    name = "03 · RECONCILE & CORRECT"
                else:
                    ax = f.add_axes([0.09, 0.16, 0.85, 0.60]); chrome(ax, "Governance & submission pack", "hash-chained audit · separation of duties")
                    npts = max(2, int(len(burndown) * ease))
                    ax.plot(range(npts), burndown[:npts], color=C["amber"], lw=2.5, marker="o", ms=3)
                    ax.set_ylabel("open exceptions", fontsize=8); ax.set_ylim(-0.5, 8)
                    ax.set_xlim(0, len(burndown))
                    if t > 0.6:
                        a = min(1.0, (t - 0.6) * 3)
                        ax.text(0.5, 0.55, "✔ " + stamp.split(" · ")[0], transform=ax.transAxes, ha="center",
                                color=C["green"], fontsize=17, fontweight="bold", alpha=a,
                                bbox=dict(boxstyle="round,pad=0.5", facecolor=C["overlay"], edgecolor=C["green"], alpha=a))
                        ax.text(0.5, 0.38, stamp.split(" · ")[-1] + f"  ·  rules {ver}", transform=ax.transAxes,
                                ha="center", color=C["muted"], fontsize=8, family="monospace", alpha=a)
                    name = "04 · REVIEW & EXPORT"
                footer(f, name, idx / total)
                f.canvas.draw()
                frame = np.asarray(f.canvas.buffer_rgba())[..., :3].copy()
                plt.close(f)
                if writer is not None:
                    writer.append_data(frame)
                else:
                    if idx % 2 == 0:
                        gif_frames.append(frame[::2, ::2])
                idx += 1
                if prog and idx % 6 == 0:
                    prog(idx / total, f"Rendering scene {name} — frame {idx}/{total}")
        if writer is not None:
            writer.close(); writer = None
        else:
            iio.mimsave(FILM_GIF, gif_frames, duration=int(1000 / fps) * 2, loop=0)
    finally:
        if writer is not None:
            try: writer.close()
            except Exception: pass
        plt.close("all")
    meta_set("film_params", {"duration": duration, "fps": fps, "res": res, "scenes": list(scenes)})
    audit("demo_film_rendered", "demo_film", out_path.name,
          {"duration": duration, "fps": fps, "resolution": res, "format": out_path.suffix})
    if prog:
        prog(1.0, "Film ready.")
    return out_path

# ──────────────────────────────────────────────────────────────────────────────
# Self-tests
# ──────────────────────────────────────────────────────────────────────────────
def _assert(cond, msg=""):
    if not cond:
        raise AssertionError(msg or "condition failed")

def self_tests() -> list[dict]:
    rp, ver = rules()
    results = []
    def run(name, fn):
        t0 = datetime.utcnow()
        try:
            fn(); ok, msg = True, "pass"
        except AssertionError as e:
            ok, msg = False, f"assert: {e}"
        except Exception as e:
            ok, msg = False, f"{type(e).__name__}: {e}"
        results.append({"test": name, "result": "PASS" if ok else "FAIL", "detail": msg,
                        "ms": round((datetime.utcnow() - t0).total_seconds() * 1000, 1)})
    am = accounts_map()
    good = {"txn_id": "T-1", "account_id": "ACC-1001", "currency": "USD", "amount": "19.99",
            "value_date": "2026-10-10", "booked_at": "2026-10-11T09:00:00",
            "category": "payment", "channel": "api", "country": "US"}
    run("cents: decimal rounding half-up", lambda: (
        _assert(to_cents("19.99") == 1999), _assert(to_cents("0.1") + to_cents("0.2") == 30),
        _assert(to_cents("1,234.565") == 123457)))
    run("validation: clean row accepted", lambda: _assert(validate_row(good, am, set(), rp)[0] is not None))
    run("validation: duplicate key RQ-004", lambda: _assert(
        any(i.rule_id == "RQ-004" for i in validate_row(good, am, {"T-1"}, rp)[1])))
    def _bad(mut, rule):
        _, iss = validate_row({**good, **mut}, am, set(), rp)
        _assert(any(i.rule_id == rule for i in iss), f"{rule} not raised")
    run("validation: currency RQ-002", lambda: _bad({"currency": "XX"}, "RQ-002"))
    run("validation: referential RQ-006", lambda: _bad({"account_id": "ACC-9999"}, "RQ-006"))
    run("validation: contradictory ccy RQ-006", lambda: _bad({"currency": "EUR"}, "RQ-006"))
    run("validation: range RQ-003", lambda: _bad({"amount": "9999999"}, "RQ-003"))
    run("validation: malformed amount RQ-003", lambda: _bad({"amount": "abc"}, "RQ-003"))
    run("validation: date window RQ-005", lambda: _bad({"value_date": "2026-10-32"}, "RQ-005"))
    run("re-import: existing keys skipped, never overwritten", lambda: _assert(
        validate_frame(pd.DataFrame([good]), {"T-1"}, rp)[2] == ["T-1"]))
    def _hand():
        s = pd.DataFrame([{"account_id": "A", "currency": "USD", "value_date": "2026-10-01",
                           "amount_cents": c, "booked_at": "2026-10-02T09:00:00"} for c in (10000, 5000)])
        l = pd.DataFrame([{"account_id": "A", "currency": "USD", "value_date": "2026-10-01",
                           "amount_cents": 14960, "booked_at": "2026-10-02T09:00:00", "batch_ref": "B"}])
        e = pd.DataFrame(columns=["account_id", "currency", "value_date", "amount_cents"])
        p = reconcile_core(s, l, e, rp).iloc[0]
        _assert(p.var_cents == 40 and p.status == "matched", "0.40 within 0.50 must match")
        l.loc[0, "amount_cents"] = 14940
        p2 = reconcile_core(s, l, e, rp).iloc[0]
        _assert(p2.var_cents == 60 and p2.status == "break_amount", "0.60 beyond 0.50 must break")
    run("recon: hand-checkable tolerance boundary", _hand)
    run("recon: fingerprint reproducible", lambda: _assert(recon_all()[1] == recon_all()[1]))
    def _chain():
        p1 = cj({"a": 1}); h1 = chain_hash("GENESIS", p1)
        h2 = chain_hash(h1, cj({"a": 2}))
        _assert(chain_hash(h1, cj({"a": 3})) != h2, "tamper not detected")
        ok, _, n = chain_verify()
        _assert(ok and n > 0, "live audit chain invalid")
    run("audit: hash chain + tamper detection", _chain)
    run("security: stakeholder cannot approve (server-side)", lambda: (
        _assert(not has("approve", "stakeholder")), _assert(not has("import", "stakeholder")),
        _assert(has("approve", "reviewer")), _assert(not has("prepare", "reviewer"))))
    def _anom():
        s = pd.Series([10.0] * 8 + [100.0])
        f, z = robust_flags(s, 7, 3.5)
        _assert(bool(f.iloc[-1]) and abs(z.iloc[-1]) > 3.5, "spike not flagged")
    run("anomaly: robust-z flags known spike", _anom)
    run("manifest: sha256 integrity", lambda: _assert(
        build_manifest({"a.csv": b"x,y\n1,2\n"}, "fp", ver)["files"]["a.csv"]["sha256"] == sha_b(b"x,y\n1,2\n")))
    run("hash helper: accepts text and bytes", lambda: _assert(sha_b("abc") == sha_b(b"abc")))
    run("countdown: timedelta clamp never negative", lambda: _assert(
        isinstance(time_left(), timedelta) and time_left() >= timedelta(0)))
    run("deps: ffmpeg film backend available", lambda: _assert(
        importlib.util.find_spec("imageio_ffmpeg") is not None, "GIF fallback will be used"))
    return results

# ──────────────────────────────────────────────────────────────────────────────
# UI foundation
# ──────────────────────────────────────────────────────────────────────────────
CSS = """
<style>
:root{--ink:#0B1220;--ink2:#0F172A;--surface:#131C2E;--overlay:#182338;--line:#243147;
--text:#E6EDF7;--muted:#93A4BD;--teal:#2DD4BF;--blue:#60A5FA;--violet:#A78BFA;--amber:#F5B93E;
--red:#F87171;--green:#34D399;--pink:#F472B6;--cyan:#22D3EE;}
html,body,[data-testid="stAppViewContainer"]{background:var(--ink);color:var(--text);}
[data-testid="stAppViewContainer"]{background:
 radial-gradient(1100px 520px at 8% -8%, rgba(45,212,191,.10), transparent 60%),
 radial-gradient(900px 480px at 96% 4%, rgba(167,139,250,.10), transparent 60%),
 radial-gradient(700px 700px at 55% 115%, rgba(96,165,250,.07), transparent 60%), var(--ink);}
[data-testid="stHeader"]{background:transparent;}
#MainMenu,footer{visibility:hidden;}
.block-container{padding-top:1.2rem;padding-bottom:3rem;max-width:1500px;}
*{scrollbar-width:thin;scrollbar-color:#2DD4BF66 #0F172A;}
::-webkit-scrollbar{width:10px;height:10px;}
::-webkit-scrollbar-track{background:#0F172A;border-radius:8px;}
::-webkit-scrollbar-thumb{background:linear-gradient(180deg,#2DD4BF88,#60A5FA88);border-radius:8px;border:2px solid #0F172A;}
::-webkit-scrollbar-thumb:hover{background:linear-gradient(180deg,#2DD4BF,#60A5FA);}
[data-testid="stSidebar"]{background:linear-gradient(180deg,#0F172A,#0B1220);border-right:1px solid var(--line);}
[data-testid="stSidebar"] *{color:var(--text);}
h1,h2,h3{color:var(--text);letter-spacing:.2px;}
h1{font-size:1.55rem;} h2{font-size:1.18rem;} h3{font-size:1.02rem;}
a{color:var(--cyan);}
.hub-top{display:flex;align-items:center;gap:14px;flex-wrap:wrap;padding:14px 18px;margin-bottom:14px;
 background:linear-gradient(90deg,rgba(45,212,191,.10),rgba(96,165,250,.06) 45%,rgba(167,139,250,.10));
 border:1px solid var(--line);border-radius:14px;}
.hub-brand{font-weight:800;font-size:1.15rem;color:var(--teal);letter-spacing:.5px;}
.hub-sub{color:var(--muted);font-size:.8rem;}
.chip{display:inline-flex;align-items:center;gap:6px;padding:3px 10px;border-radius:999px;font-size:.72rem;
 font-weight:700;border:1px solid var(--line);background:var(--overlay);color:var(--text);}
.chip.teal{color:var(--teal);border-color:#2DD4BF55;} .chip.amber{color:var(--amber);border-color:#F5B93E55;}
.chip.red{color:var(--red);border-color:#F8717155;} .chip.green{color:var(--green);border-color:#34D39955;}
.chip.violet{color:var(--violet);border-color:#A78BFA55;} .chip.blue{color:var(--blue);border-color:#60A5FA55;}
.dot{width:8px;height:8px;border-radius:50%;background:var(--green);animation:pulse 1.4s infinite;}
.dot.off{background:var(--muted);animation:none;}
@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(52,211,153,.55)}70%{box-shadow:0 0 0 9px rgba(52,211,153,0)}100%{box-shadow:0 0 0 0 rgba(52,211,153,0)}}
.kgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin:6px 0 14px;}
.kcard{background:linear-gradient(160deg,var(--surface),var(--ink2));border:1px solid var(--line);
 border-radius:14px;padding:14px 16px;position:relative;overflow:hidden;animation:fadeUp .5s ease both;}
.kcard:nth-child(2){animation-delay:.05s}.kcard:nth-child(3){animation-delay:.1s}
.kcard:nth-child(4){animation-delay:.15s}.kcard:nth-child(5){animation-delay:.2s}.kcard:nth-child(6){animation-delay:.25s}
.kcard:before{content:"";position:absolute;inset:0 auto 0 0;width:4px;background:var(--accent,var(--teal));}
.klabel{color:var(--muted);font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.8px;}
.kval{font-family:"Cascadia Code",Consolas,monospace;font-size:1.5rem;font-weight:700;margin-top:4px;color:var(--text);}
.kdelta{font-size:.72rem;margin-top:3px;color:var(--muted);}
@keyframes fadeUp{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}
.panel{background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:14px 16px;margin:8px 0;}
.panel h4{margin:0 0 8px;color:var(--teal);font-size:.85rem;text-transform:uppercase;letter-spacing:.8px;}
.mono{font-family:"Cascadia Code",Consolas,monospace;font-size:.8rem;}
.feedrow{display:flex;justify-content:space-between;gap:8px;padding:6px 10px;border-bottom:1px dashed var(--line);
 font-size:.78rem;animation:fadeUp .3s ease both;}
.pill{padding:2px 9px;border-radius:999px;font-size:.68rem;font-weight:800;letter-spacing:.4px;}
.stButton>button{background:var(--overlay);color:var(--text);border:1px solid var(--line);border-radius:10px;
 font-weight:600;transition:all .15s ease;}
.stButton>button:hover{border-color:var(--teal);color:var(--teal);transform:translateY(-1px);}
.stButton>button[kind="primary"]{background:linear-gradient(90deg,#0EA5A0,#2563EB);border:none;color:white;}
.stButton>button[kind="primary"]:hover{filter:brightness(1.15);color:white;}
.stDownloadButton>button{background:var(--overlay);border:1px solid #2DD4BF55;color:var(--teal);border-radius:10px;font-weight:700;}
.stDownloadButton>button:hover{background:#2DD4BF22;}
[data-testid="stDataFrame"]{border:1px solid var(--line);border-radius:12px;}
[data-testid="stExpander"]{border:1px solid var(--line);border-radius:12px;background:var(--ink2);}
.stTabs [data-baseweb="tab-list"]{gap:6px;border-bottom:1px solid var(--line);}
.stTabs [data-baseweb="tab"]{background:var(--ink2);border-radius:10px 10px 0 0;color:var(--muted);font-weight:600;}
.stTabs [aria-selected="true"]{background:var(--overlay);color:var(--teal);}
[data-testid="stSidebar"] .stRadio label{padding:4px 6px;border-radius:8px;}
[data-testid="stSidebar"] .stRadio label:hover{background:#2DD4BF14;}
[data-testid="stMetricValue"]{font-family:Consolas,monospace;}
.stAlert{border-radius:12px;border:1px solid var(--line);}
hr{border-color:var(--line);}
.small{font-size:.75rem;color:var(--muted);}
.lifecycle{display:flex;gap:6px;flex-wrap:wrap;margin:6px 0;}
.lc{padding:3px 10px;border-radius:8px;font-size:.7rem;font-weight:700;background:var(--overlay);
 border:1px solid var(--line);color:var(--muted);}
.lc.on{color:var(--teal);border-color:#2DD4BF66;background:#2DD4BF14;}
</style>
"""

def wide(fn, *a, **k):
    """Version-tolerant full-width rendering across Streamlit releases."""
    try:
        return fn(*a, width="stretch", **k)
    except TypeError:
        try:
            return fn(*a, use_container_width=True, **k)
        except TypeError:
            return fn(*a, **k)

def chart(fig: go.Figure, key: str | None = None):
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font=dict(color=C["text"], size=11), margin=dict(l=10, r=10, t=34, b=10),
                      legend=dict(orientation="h", y=1.12, x=0, bgcolor="rgba(0,0,0,0)"),
                      uirevision="keep", hoverlabel=dict(bgcolor=C["overlay"], font_color=C["text"]))
    fig.update_xaxes(gridcolor=C["line"], zerolinecolor=C["line"])
    fig.update_yaxes(gridcolor=C["line"], zerolinecolor=C["line"])
    wide(st.plotly_chart, fig, key=key, config={"displayModeBar": False})

def dtable(df: pd.DataFrame, height: int = 320, key: str | None = None, selectable: bool = False):
    kw = dict(hide_index=True, height=height)
    if key: kw["key"] = key
    if selectable: kw.update(on_select="rerun", selection_mode="single-row")
    return wide(st.dataframe, df, **kw)

def sel(label: str, options, key: str, **kw):
    opts = list(options)
    if not opts:
        return None
    if st.session_state.get(key) not in opts:
        st.session_state[key] = opts[0]
    return st.selectbox(label, opts, key=key, **kw)

def pill_html(text: str, color: str) -> str:
    return (f"<span class='pill' style='color:{color};background:{color}1c;"
            f"border:1px solid {color}55'>{esc(str(text))}</span>")

def kpi(label: str, value: str, delta: str, accent: str):
    st.markdown(f"<div class='kcard' style='--accent:{accent}'><div class='klabel'>{esc(label)}</div>"
                f"<div class='kval'>{value}</div><div class='kdelta'>{delta}</div></div>",
                unsafe_allow_html=True)

def hdr(title: str, sub: str):
    st.markdown(f"## {title}")
    st.caption(sub)

def pills_select(label: str, options: list, default, key: str):
    if hasattr(st, "pills"):
        v = st.pills(label, options, default=default, key=key)
        return v if v is not None else default
    return st.radio(label, options, index=options.index(default) if default in options else 0,
                    horizontal=True, key=key)

def fig_base() -> go.Figure:
    return go.Figure()

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar / topbar
# ──────────────────────────────────────────────────────────────────────────────
PAGES = ["🛰️ Command Center", "📥 Import & Validate", "⚖️ Reconciliation", "🧯 Exceptions & Review",
         "🗂️ Records & Lineage", "📦 Reports & Submission", "🛡️ Audit & Governance", "🎬 Demo Studio"]

def sidebar() -> str:
    with st.sidebar:
        st.markdown("<div class='hub-brand'>◆ LedgerSentinel</div>"
                    "<div class='hub-sub'>Regulatory Reporting Quality & Reconciliation</div>",
                    unsafe_allow_html=True)
        st.markdown("<span class='chip amber'>SYNTHETIC DATA · DEMO</span>", unsafe_allow_html=True)
        st.divider()
        role = st.selectbox("👤 Active persona", list(ROLES), key="role",
                            format_func=lambda k: f"{ROLES[k]['icon']} {ROLES[k]['name']} — {ROLES[k]['title']}")
        st.caption("Permissions: " + (", ".join(sorted(ROLES[role]["perms"])) or "view & download only"))
        page = st.radio("Navigate", PAGES, key="page", label_visibility="collapsed")
        st.divider()
        st.markdown("#### 📡 Live simulation")
        st.toggle("Live feed running", key="live")
        st.slider("Tick interval (s)", 1, 10, key="sim_interval")
        st.slider("Batch size (txns / tick)", 1, 25, key="sim_batch")
        st.slider("Anomaly injection (%)", 0, 30, key="sim_anom")
        st.selectbox("Currency focus", ["All", "USD", "EUR", "GBP"], key="sim_ccy")
        st.number_input("Deterministic seed", 1, 99999, key="sim_seed")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("⚡ Inject anomaly", key="btn_inject"):
                st.session_state.inject_next = True
                st.toast("Anomaly armed — watch the next tick", icon="⚡")
        with c2:
            if st.button("🔄 Refresh now", key="btn_refresh"):
                st.rerun()
        n_staged = len(st.session_state.staged)
        if st.button(f"📌 Commit staged batch ({n_staged})", key="btn_commit", type="primary",
                     disabled=n_staged == 0 or not has("import")):
            try:
                res = sim_commit()
                st.toast(f"Committed {res['records']} records · {res['breaks']} engineered breaks → reconcile them",
                         icon="📌")
                st.rerun()
            except HubError as e:
                st.error(str(e))
        st.caption("Replay mode: simulated txns carry October value dates; committing posts both source records "
                   "and ledger entries — injected anomalies corrupt the ledger side, creating real breaks.")
        st.divider()
        film = film_ready()
        st.markdown(f"<span class='chip {'green' if film else 'amber'}'>"
                    f"{'🎬 Demo film ready' if film else '🎬 Film renders on first run'}</span>",
                    unsafe_allow_html=True)
        if st.button("🧨 Reset demo data", key="btn_reset"):
            for p in (DB_PATH, Path(str(DB_PATH) + "-wal"), Path(str(DB_PATH) + "-shm")):
                if p.exists(): p.unlink()
            for d in (UP_DIR, EV_DIR, EX_DIR):
                shutil.rmtree(d, ignore_errors=True); d.mkdir(parents=True, exist_ok=True)
            for k in ("feed", "staged", "tick", "sim_ok", "sim_bad", "test_results", "preview"):
                st.session_state.pop(k, None)
            st.rerun()
        st.caption(f"{APP_NAME} {APP_VER} · SQLite WAL · offline · no API keys")
    return page

def topbar():
    ss = st.session_state
    left = time_left()
    due_txt = f"{left.days}d {left.seconds // 3600}h to filing due" if left.total_seconds() > 0 else "filing window elapsed (demo)"
    live = ss.get("live", True)
    st.markdown(
        f"<div class='hub-top'><span class='hub-brand'>◆ {APP_NAME}</span>"
        f"<span class='hub-sub'>{APP_TAG}</span>"
        f"<span class='chip teal'>Period {PERIOD}</span>"
        f"<span class='chip amber'>⏳ {due_txt}</span>"
        f"<span class='chip {'green' if live else ''}'><span class='dot {'' if live else 'off'}'></span>"
        f"{'LIVE FEED' if live else 'FEED PAUSED'}</span>"
        f"<span class='chip violet'>👤 {esc(ROLES[ss['role']]['title'])}</span>"
        f"<span class='chip blue'>SYNTHETIC · DECISION SUPPORT</span></div>",
        unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# Page 1 — Command Center
# ──────────────────────────────────────────────────────────────────────────────
def render_live():
    ss = st.session_state
    if ss.live:
        sim_tick()
    feed = list(ss.feed)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Staged (uncommitted)", len(ss.staged))
    c2.metric("Validated live", ss.sim_ok)
    c3.metric("Flagged live", ss.sim_bad)
    lat = [f["latency"] for f in feed] or [0]
    c4.metric("p95 latency", f"{np.percentile(lat, 95):.0f} ms")
    if feed:
        df = pd.DataFrame(feed)
        g = df.groupby("tick").agg(ok=("ok", "sum"), bad=("ok", lambda s: (~s).sum())).reset_index()
        f = fig_base()
        f.add_trace(go.Scatter(x=g.tick, y=g.ok, name="validated / tick", mode="lines",
                               line=dict(color=C["teal"], width=2), fill="tozeroy",
                               fillcolor="rgba(45,212,191,.18)"))
        f.add_trace(go.Scatter(x=g.tick, y=g.bad, name="flagged / tick", mode="lines+markers",
                               line=dict(color=C["red"], width=2),
                               marker=dict(size=6, color=C["red"])))
        f.update_layout(height=230, title="Live ingestion — validated vs flagged",
                        xaxis_title="tick", yaxis_title="events")
        chart(f, key="live_chart")
        st.markdown("<div class='panel'><h4>Event stream (latest 7)</h4>", unsafe_allow_html=True)
        for ev in feed[-7:][::-1]:
            status = pill_html("VALIDATED", C["green"]) if ev["ok"] else pill_html(f"FLAGGED · {ev['rule']}", C["red"])
            an = f" <span class='chip violet'>anomaly:{ev['anomaly']}</span>" if ev["anomaly"] else ""
            st.markdown(f"<div class='feedrow'><span class='mono'>t{ev['tick']} · {esc(ev['acct'])} · "
                        f"{money(ev['amt'], ev['ccy'])}</span><span>{status}{an} "
                        f"<span class='small'>{ev['latency']:.0f} ms</span></span></div>",
                        unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.info("Feed warming up — first tick arrives shortly.")
    if not ss.live:
        st.caption("⏸ Feed paused. Toggle 'Live feed running' in the sidebar to resume.")

def page_overview():
    hdr("🛰️ Command Center", f"{ORG} · reporting period {PERIOD} · seeded automatically, simulation running")
    pairs, fp, rp, ver = recon_all()
    cov = float(pairs.status.isin(["matched", "accepted"]).mean()) if len(pairs) else 1.0
    excs = qdf("SELECT * FROM exceptions")
    open_high = int(((excs.severity == "high") & (~excs.status.isin(CLEARED))).sum()) if not excs.empty else 0
    open_all = int((~excs.status.isin(CLEARED)).sum()) if not excs.empty else 0
    n_rec = int(q("SELECT COUNT(*) n FROM records")[0]["n"])
    left = time_left()
    st.markdown("<div class='kgrid'>", unsafe_allow_html=True)
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    with k1: kpi("Validated records", f"{n_rec:,}", "source of truth · immutable", C["teal"])
    with k2: kpi("Recon coverage", pct(cov), f"target >= 99.5% · rules {ver}", C["blue"])
    with k3: kpi("Open exceptions", str(open_all), f"{open_high} high severity", C["red"] if open_high else C["green"])
    with k4: kpi("Quality score", f"{quality_score(pairs):.1f}", "0.35·valid + 0.45·coverage + 0.20·resolved", C["violet"])
    with k5: kpi("Pack fingerprint", fp[:10] + "…", "reproducible · drift-checked", C["cyan"])
    with k6: kpi("Filing countdown", f"{left.days}d {left.seconds // 3600}h", DUE.strftime("%b %d, %H:%M"), C["amber"])
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("### 📡 Live ingestion monitor")
    if hasattr(st, "fragment"):
        st.fragment(run_every=st.session_state.sim_interval if st.session_state.live else None)(render_live)()
    else:
        render_live()
        st.button("Refresh feed", on_click=st.rerun)

    c1, c2 = st.columns([3, 2])
    with c1:
        src, led, adj = load_frames()
        ds = src.groupby("value_date").amount_cents.sum() / 100
        dl = led.groupby("value_date").amount_cents.sum() / 100
        da = adj.groupby("value_date").amount_cents.sum() / 100 if not adj.empty else pd.Series(dtype=float)
        dl = dl.add(da, fill_value=0)
        f = fig_base()
        f.add_trace(go.Scatter(x=ds.index, y=ds.values, name="source totals", mode="lines",
                               line=dict(color=C["teal"], width=2.5)))
        f.add_trace(go.Scatter(x=dl.index, y=dl.values, name="ledger + adjustments", mode="lines",
                               line=dict(color=C["blue"], width=2, dash="dash")))
        brk = pairs[pairs.status.isin(["break_amount", "missing_posting"])]
        if not brk.empty:
            f.add_trace(go.Scatter(x=brk.value_date, y=brk.src_cents / 100, name="breaks",
                                   mode="markers", marker=dict(color=C["red"], size=11, symbol="x"),
                                   hovertemplate="%{x}<br>variance %{customdata}<extra></extra>",
                                   customdata=[money(int(v), c) for v, c in zip(brk.var_cents, brk.currency)]))
        f.update_layout(height=300, title="Daily totals — source vs ledger (all currencies, major units)")
        chart(f, key="ov_daily")
    with c2:
        counts = excs.status.value_counts() if not excs.empty else pd.Series(dtype=int)
        f = fig_base()
        f.add_trace(go.Pie(labels=counts.index.tolist(), values=counts.values.tolist(), hole=0.62,
                           marker=dict(colors=[STATUS_COLOR.get(s, C["muted"]) for s in counts.index]),
                           textinfo="label+value"))
        f.update_layout(height=300, title="Exception lifecycle", showlegend=False,
                        annotations=[dict(text=f"{open_all}<br>open", showarrow=False,
                                          font=dict(size=18, color=C["text"]))])
        chart(f, key="ov_donut")

    c3, c4 = st.columns([2, 3])
    with c3:
        f = fig_base()
        f.add_trace(go.Indicator(mode="gauge+number", value=round(cov * 100, 2),
                                 number=dict(suffix="%", font=dict(color=C["text"])),
                                 gauge=dict(axis=dict(range=[90, 100], tickcolor=C["muted"]),
                                            bar=dict(color=C["teal"]), bgcolor="rgba(0,0,0,0)",
                                            bordercolor=C["line"],
                                            steps=[dict(range=[90, 99.5], color="rgba(248,113,113,.15)"),
                                                   dict(range=[99.5, 100], color="rgba(52,211,153,.18)")],
                                            threshold=dict(line=dict(color=C["amber"], width=3), value=99.5))))
        f.update_layout(height=260, title="Coverage vs 99.5% gate")
        chart(f, key="ov_gauge")
    with c4:
        matched_recs = int(pairs.loc[pairs.status.isin(["matched", "accepted"]), "n_src"].sum())
        break_recs = int(pairs.loc[~pairs.status.isin(["matched", "accepted"]), "n_src"].sum())
        rejected = int(q("SELECT COALESCE(SUM(rows_rejected),0) n FROM imports")[0]["n"])
        approved = bool(q("SELECT id FROM exports WHERE status='Approved'"))
        labels = ["Source files", "Validated", "Quarantined", "Recon matched", "Recon breaks",
                  "Exceptions", "Resolved", "Approved pack"]
        vals = [n_rec + rejected, n_rec, rejected, matched_recs, break_recs,
                len(excs), int(excs.status.isin(CLEARED).sum()) if not excs.empty else 0,
                n_rec if approved else 0]
        s_idx = [0, 0, 1, 1, 3, 4, 5, 6]
        t_idx = [1, 2, 3, 4, 5, 6, 7, 7]
        link_cols = ["rgba(45,212,191,.35)", "rgba(248,113,113,.35)", "rgba(96,165,250,.35)",
                     "rgba(248,113,113,.35)", "rgba(245,185,62,.4)", "rgba(167,139,250,.4)",
                     "rgba(52,211,153,.4)", "rgba(34,211,238,.4)"]
        f = fig_base()
        f.add_trace(go.Sankey(
            node=dict(label=[f"{l}<br>{v:,}" for l, v in zip(labels, vals)],
                      color=[C["teal"], C["blue"], C["red"], C["green"], C["red"], C["amber"], C["violet"], C["cyan"]],
                      pad=16, thickness=14, line=dict(color=C["line"], width=1)),
            link=dict(source=s_idx, target=t_idx, value=[max(v, 1) for v in
                      [n_rec, rejected, matched_recs, break_recs, break_recs, len(excs),
                       int(excs.status.isin(CLEARED).sum()) if not excs.empty else 0, n_rec if approved else 1]],
                      color=link_cols)))
        f.update_layout(height=280, title="Workflow volumes — actors: Analyst → engine → Reviewer → pack")
        chart(f, key="ov_sankey")

    with st.expander("🎬 Watch the auto-generated demo film", expanded=False):
        film = film_ready()
        if film:
            if film.suffix == ".mp4":
                st.video(str(film))
            else:
                st.image(str(film))
            st.caption("Rendered locally (matplotlib frames → FFmpeg). Regenerate with custom scenes in **Demo Studio**.")
        else:
            st.info("Film is rendering below on first launch…")
    if not film_ready():
        with st.status("🎬 Rendering the live demo film locally (first run only, no API)…", expanded=True) as s:
            pbar = st.progress(0.0)
            def prog(v, msg):
                pbar.progress(min(1.0, v), text=msg)
            try:
                out = render_film(prog=prog)
                s.update(label=f"🎬 Demo film ready — {out.name}", state="complete")
                if out.suffix == ".mp4":
                    st.video(str(out))
                else:
                    st.image(str(out))
            except Exception as e:
                s.update(label="Film rendering unavailable", state="error")
                st.warning(f"Film rendering failed gracefully: {e}. The live dashboard remains fully functional; "
                           f"retry from Demo Studio.")

# ──────────────────────────────────────────────────────────────────────────────
# Page 2 — Import & Validate
# ──────────────────────────────────────────────────────────────────────────────
def page_import():
    hdr("📥 Import & Validate", "CSV / XLSX → schema check → row-level validation → preview → safe commit. "
        "Originals preserved with SHA-256; validated data is never silently overwritten.")
    c1, c2, c3 = st.columns(3)
    c1.download_button("⬇️ Valid sample CSV", sample_valid_csv(), "sample_valid.csv", "text/csv", key="dl_ok")
    c2.download_button("⬇️ Broken sample CSV", sample_broken_csv(), "sample_broken.csv", "text/csv", key="dl_bad")
    c3.download_button("⬇️ Sample XLSX", sample_xlsx(), "sample.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_xlsx")
    with st.expander("📚 Data dictionary & schema contract", expanded=False):
        dtable(pd.DataFrame(DATA_DICT, columns=["field", "type", "unit", "meaning", "allowed values",
                                                "source", "sensitivity", "validation rule"]), height=340)
        st.caption("Amounts stored as integer cents (Decimal ROUND_HALF_UP). Dates ISO-8601. "
                   "Currencies ISO-4217. All provenance recorded in lineage with transform version tx-v1.")

    # Clear the uploader BEFORE it is instantiated (safe across Streamlit versions).
    if st.session_state.get("clear_upload"):
        st.session_state.pop("up_file", None)
        st.session_state.clear_upload = False
    up = st.file_uploader("Upload source extract (.csv / .xlsx, <=10 MB)", type=["csv", "xlsx"], key="up_file")
    b1, b2 = st.columns(2)
    with b1:
        if st.button("🧪 Run broken-sample demo (rejection path)", key="btn_broken"):
            try:
                st.session_state.preview = stage_preview("gateway_broken_sample.csv", sample_broken_csv())
                st.toast("Broken sample staged — inspect the actionable row errors", icon="🧪")
            except HubError as e:
                st.error(str(e))
    with b2:
        if st.button("✅ Run valid-sample demo", key="btn_valid"):
            try:
                st.session_state.preview = stage_preview("corebank_sample.csv", sample_valid_csv())
                st.toast("Valid sample staged — commit it to the ledger workflow", icon="✅")
            except HubError as e:
                st.error(str(e))
    if up is not None:
        try:
            st.session_state.preview = stage_preview(up.name, up.getvalue())
        except HubError as e:
            st.error(str(e))
            st.session_state.pop("preview", None)

    pv = st.session_state.get("preview")
    if pv:
        st.markdown(f"<div class='panel'><h4>Preview — {esc(pv['filename'])}</h4>"
                    f"<span class='mono small'>sha256 {pv['sha'][:24]}…</span></div>", unsafe_allow_html=True)
        if pv["dup_file"]:
            st.warning("⚠️ Identical checksum already imported. Committing is safe: existing transaction keys are "
                       "skipped, never overwritten.")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Rows in file", len(pv["df"]))
        m2.metric("Valid", len(pv["valid"]))
        m3.metric("Rejected rows", len({e["row_num"] for e in pv["errors"]}))
        m4.metric("Skipped (already exist)", len(pv["skipped"]))
        t1, t2 = st.tabs(["🔎 Preview rows (first 50)", f"🚫 Row errors ({len(pv['errors'])})"])
        with t1:
            dtable(pv["df"].head(50), height=300)
        with t2:
            if pv["errors"]:
                edf = pd.DataFrame(pv["errors"])[["row_num", "rule_id", "col", "severity", "message", "fix"]]
                dtable(edf, height=300)
                st.download_button("⬇️ Download error report (CSV)", edf.to_csv(index=False).encode(),
                                   f"errors_{Path(pv['filename']).stem}.csv", "text/csv", key="dl_err")
            else:
                st.success("No row-level errors — file fully conforms to the schema contract.")
        src = st.selectbox("Source system", ["CoreBank CSV", "Payments Gateway XLSX", "Treasury Manual", "Other"],
                           key="imp_src")
        cc1, cc2 = st.columns([1, 1])
        with cc1:
            if st.button("📌 Commit import", type="primary", key="btn_commit_imp",
                         disabled=not has("import") or not pv["valid"]):
                try:
                    res = commit_import(pv, src)
                    st.toast(f"Import {res['import_id']}: {res['valid']} committed · {res['rejected']} rejected · "
                             f"{res['skipped']} skipped", icon="📌")
                    st.session_state.pop("preview", None)
                    st.session_state.clear_upload = True
                    st.rerun()
                except HubError as e:
                    st.error(str(e))
        with cc2:
            if st.button("🗑️ Discard preview", key="btn_discard"):
                st.session_state.pop("preview", None)
                st.session_state.clear_upload = True
                st.rerun()
        if not has("import"):
            st.info("Current persona cannot commit imports — switch to Analyst or Administrator.")

    st.markdown("### 📜 Import history (immutable originals)")
    imps = qdf("SELECT * FROM imports ORDER BY created_at DESC")
    if imps.empty:
        st.info("No imports yet.")
        return
    dtable(imps[["id", "filename", "source_system", "actor", "rows_total", "rows_valid",
                 "rows_rejected", "rows_skipped", "status", "created_at"]], height=260)
    sel_id = sel("Inspect import", imps.id.tolist(), "imp_sel")
    row = imps[imps.id == sel_id].iloc[0]
    c1, c2 = st.columns(2)
    stored = list(UP_DIR.glob(f"{row.sha256}.*"))
    with c1:
        if stored:
            st.download_button("⬇️ Original file (checksum-verified)", stored[0].read_bytes(),
                               row.filename, key="dl_orig")
            st.caption(f"checksum {'✅ matches' if sha_f(stored[0]) == row.sha256 else '❌ MISMATCH'}")
        else:
            st.caption("Seed extract — original generated deterministically (no stored upload).")
    errs = qdf("SELECT row_num,rule_id,col,message,fix FROM import_errors WHERE import_id=?", (sel_id,))
    with c2:
        if not errs.empty:
            st.download_button(f"⬇️ Error report ({len(errs)} issues)", errs.to_csv(index=False).encode(),
                               f"errors_{sel_id}.csv", "text/csv", key="dl_hist_err")
    if not errs.empty:
        dtable(errs, height=200)

# ──────────────────────────────────────────────────────────────────────────────
# Page 3 — Reconciliation
# ──────────────────────────────────────────────────────────────────────────────
def page_recon():
    hdr("⚖️ Reconciliation", "Deterministic source-to-ledger matching per account · currency · value date. "
        "Every verdict is explainable, fingerprinted and versioned.")
    pairs, fp, rp, ver = recon_all()
    c1, c2, c3 = st.columns([1, 1, 2])
    ccy_f = c1.selectbox("Currency", ["All", "USD", "EUR", "GBP"], key="rc_ccy")
    acct_f = sel("Account", ["All"] + sorted(pairs.account_id.unique().tolist()), "rc_acct")
    status_f = pills_select("Status filter", ["All", "breaks", "matched", "accepted"], "All", key="rc_status")
    view = pairs.copy()
    if ccy_f != "All": view = view[view.currency == ccy_f]
    if acct_f != "All": view = view[view.account_id == acct_f]
    if status_f == "breaks": view = view[view.status.isin(["break_amount", "missing_posting", "timing"])]
    elif status_f != "All": view = view[view.status == status_f]

    for ccy in ["USD", "EUR", "GBP"]:
        sub = pairs[pairs.currency == ccy]
        if sub.empty: continue
        st.markdown(f"<span class='chip teal'>{ccy} source {money(int(sub.src_cents.sum()), ccy)}</span> "
                    f"<span class='chip blue'>ledger {money(int(sub.led_cents.sum() + sub.adj_cents.sum()), ccy)}</span> "
                    f"<span class='chip {'green' if sub.var_cents.sum() == 0 else 'red'}'>net variance "
                    f"{money(int(sub.var_cents.sum()), ccy)}</span>", unsafe_allow_html=True)
    st.markdown(f"<div class='small'>Rules {ver} · tolerance = max({money(rp['tolerance_abs_cents'])}, "
                f"{rp['tolerance_rel']*100:.1f}% of |source|) · grace {rp['timing_grace_days']} days · "
                f"fingerprint <span class='mono'>{fp}</span></div>", unsafe_allow_html=True)

    disp = view.copy()
    for c, lbl in [("src_cents", "source"), ("led_cents", "ledger"), ("adj_cents", "adjustments"),
                   ("var_cents", "variance"), ("tol_cents", "tolerance")]:
        disp[lbl] = [money(int(v), cc) for v, cc in zip(disp[c], disp.currency)]
    disp = disp[["account_id", "currency", "value_date", "source", "ledger", "adjustments",
                 "variance", "tolerance", "n_src", "late_days", "status"]]
    ev = dtable(disp, height=320, key="rc_table", selectable=True)
    sel_rows = ev.selection.rows if ev and hasattr(ev, "selection") else []
    if sel_rows:
        r = view.iloc[sel_rows[0]]
        ex = pair_explain(r, rp, ver)
        color = STATUS_COLOR.get(r.status, C["muted"])
        st.markdown(f"<div class='panel'><h4>Explanation — {esc(r.key)}</h4>"
                    f"{pill_html(r.status.upper(), color)}<br><br>"
                    f"<b>Formula:</b> <span class='mono'>{esc(ex['formula'])}</span><br>"
                    f"<b>Inputs:</b> source {ex['inputs']['source']} · ledger {ex['inputs']['ledger']} · "
                    f"adjustments {ex['inputs']['adjustments']} → <b>variance {ex['inputs']['variance']}</b><br>"
                    f"<b>Tolerance:</b> <span class='mono'>{esc(ex['tolerance'])}</span><br>"
                    f"<b>Factors:</b><ul>" + "".join(f"<li>{esc(f)}</li>" for f in ex["factors"]) + "</ul>"
                    f"<b>Uncertainty:</b> {esc(ex['uncertainty'])}<br>"
                    f"<span class='small'>Rules {esc(ex['rule_version'])} · {', '.join(ex['rules'])} · "
                    f"reviewer status via exception queue</span></div>", unsafe_allow_html=True)
        b1, b2, b3 = st.columns(3)
        with b1:
            if r.status in ("break_amount", "missing_posting", "timing"):
                if st.button("🧯 Open / locate exception", key="rc_open_exc",
                             disabled=not (has("resolve") or has("correct"))):
                    try:
                        kind = {"break_amount": "amount_mismatch", "missing_posting": "missing_posting",
                                "timing": "timing"}[r.status]
                        eid = exc_open(kind, "high" if kind != "timing" else "medium",
                                       "RC-102" if kind == "timing" else "RC-101", "recon_pair", r.key,
                                       f"{kind.replace('_', ' ').title()} — {r.account_id}, {r.value_date}",
                                       {"variance_cents": int(r.var_cents), "late_days": int(r.late_days)},
                                       f"recon:{r.key}:{kind}")
                        st.toast(f"Exception {eid} ready in the review queue", icon="🧯")
                    except HubError as e:
                        st.error(str(e))
        with b2:
            if st.button("🔁 Re-run & verify fingerprint", key="rc_fp"):
                _, fp2, _, _ = recon_all()
                if fp2 == fp:
                    st.toast(f"Reproducible ✅ fingerprint {fp2}", icon="🔁")
                else:
                    st.error("Fingerprint changed between runs — investigate data drift.")
        with b3:
            if r.status != "matched" and has("override"):
                reason = st.text_input("Override reason (>=20 chars)", key="rc_ov_reason",
                                       placeholder="e.g. Timing accepted — settlement confirmed T+5 by treasury")
                if st.button("🟣 Accept with override", key="rc_ov_btn"):
                    try:
                        kind = {"break_amount": "amount_mismatch", "missing_posting": "missing_posting",
                                "timing": "timing"}.get(r.status, "amount_mismatch")
                        eid = exc_open(kind, "high", "RC-101", "recon_pair", r.key,
                                       f"Override — {r.key}", {"variance_cents": int(r.var_cents)},
                                       f"recon:{r.key}:{kind}")
                        cur = q("SELECT status FROM exceptions WHERE id=?", (eid,))[0]["status"]
                        if cur in ("Open", "Resolved"):
                            exc_transition(eid, "Investigating", "Override triage")
                            cur = "Investigating"
                        if cur != "Overridden":
                            exc_transition(eid, "Overridden", reason)
                        st.toast("Pair formally accepted — audit recorded", icon="🟣")
                        st.rerun()
                    except HubError as e:
                        st.error(str(e))
        with st.expander("Raw explanation JSON (traceable output)"):
            st.json(ex)

    c1, c2 = st.columns([3, 2])
    with c1:
        top = pairs[pairs.var_cents != 0].copy()
        top["abs"] = top.var_cents.abs()
        top = top.sort_values("abs", ascending=False).head(12)
        if not top.empty:
            f = fig_base()
            f.add_trace(go.Bar(x=top.key, y=top.var_cents / 100,
                               marker=dict(color=[STATUS_COLOR.get(s, C["muted"]) for s in top.status]),
                               hovertemplate="%{x}<br>variance %{y:,.2f}<extra></extra>"))
            f.add_hline(y=0, line_color=C["muted"])
            f.update_layout(height=300, title="Largest variances (major units) — red breaks, amber timing, violet accepted")
            chart(f, key="rc_bars")
        else:
            st.success("All pairs net to zero variance.")
    with c2:
        with st.expander("🧮 Hand-checkable example (same engine)", expanded=True):
            st.markdown("""
| line | source | ledger |
|---|---|---|
| txn A | 100.00 | |
| txn B | 50.00 | |
| batch | **150.00** | 149.60 |

`variance = 150.00 − 149.60 = 0.40 <= tol 0.50` → **MATCHED**
If ledger were `149.40`: `variance 0.60 > 0.50` → **BREAK**
The self-test suite asserts exactly these two outcomes.
""")
    with st.expander("⚙️ Governed rule change (Administrator)"):
        rp2, ver2 = rules()
        n1 = st.number_input("Absolute tolerance (cents)", 0, 10000, int(rp2["tolerance_abs_cents"]), key="rp_abs")
        n2 = st.number_input("Relative tolerance (fraction)", 0.0, 0.05, float(rp2["tolerance_rel"]),
                             format="%.4f", key="rp_rel")
        n3 = st.number_input("Timing grace (days)", 0, 10, int(rp2["timing_grace_days"]), key="rp_grace")
        note = st.text_input("Change justification", key="rp_note", placeholder="e.g. Treasury-approved FX rounding policy")
        if st.button("📜 Activate new rule version", key="rp_go", disabled=not has("rules")):
            try:
                if len(note.strip()) < 10:
                    raise HubError("Justification must be >=10 characters (governance evidence).")
                nv = rules_activate({**rp2, "tolerance_abs_cents": int(n1), "tolerance_rel": float(n2),
                                     "timing_grace_days": int(n3)}, note)
                st.toast(f"Rule version {nv} activated — reconciliation re-versioned", icon="📜")
                st.rerun()
            except HubError as e:
                st.error(str(e))

# ──────────────────────────────────────────────────────────────────────────────
# Page 4 — Exceptions & Review
# ──────────────────────────────────────────────────────────────────────────────
def page_exceptions():
    hdr("🧯 Exceptions & Review", "Lifecycle with evidence, corrections, verification and overrides. "
        "Every transition is permission-checked and hash-chain audited.")
    st.markdown("<div class='lifecycle'>" + "".join(
        f"<span class='lc {'on' if s in ('Open','Investigating') else ''}'>{s}</span>" for s in
        ["Open", "Investigating", "Resolved", "Verified", "Closed", "Overridden"]) +
        "</div>", unsafe_allow_html=True)
    excs = qdf("SELECT * FROM exceptions ORDER BY CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 "
               "ELSE 2 END, opened_at")
    if excs.empty:
        st.success("Exception queue is empty — nothing to review.")
        return
    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    st_f = sel("Status", ["All"] + sorted(excs.status.unique().tolist()), "ex_st")
    sv_f = sel("Severity", ["All", "high", "medium", "low"], "ex_sv")
    kd_f = sel("Kind", ["All"] + sorted(excs.kind.unique().tolist()), "ex_kd")
    search = c4.text_input("Search title / entity", key="ex_q")
    view = excs.copy()
    if st_f != "All": view = view[view.status == st_f]
    if sv_f != "All": view = view[view.severity == sv_f]
    if kd_f != "All": view = view[view.kind == kd_f]
    if search: view = view[view.title.str.contains(search, case=False) | view.entity_id.str.contains(search, case=False)]
    disp = view[["id", "severity", "kind", "title", "status", "assignee", "opened_at"]].copy()
    dtable(disp, height=260)

    with st.expander("👥 Bulk assignment"):
        valid_ids = view.id.tolist()
        st.session_state.ex_bulk_ids = [i for i in st.session_state.get("ex_bulk_ids", []) if i in valid_ids]
        ids = st.multiselect("Exceptions", valid_ids, key="ex_bulk_ids")
        who = st.selectbox("Assign to", [r["name"] for r in ROLES.values()], key="ex_bulk_who")
        if st.button("Assign selected", key="ex_bulk_go", disabled=not has("assign") or not ids):
            for eid in ids:
                x("UPDATE exceptions SET assignee=?, updated_at=? WHERE id=?", (who, now_iso(), eid))
                audit("exception_assigned", "exception", eid, {"assignee": who})
            st.toast(f"{len(ids)} exceptions assigned to {who}", icon="👥")
            st.rerun()
    with st.expander("⚡ Administrator fast-track (demo): disposition all open exceptions with full audit"):
        st.caption("Executes the same service functions as the manual path — adjustments, verifications and "
                   "overrides are all real, evidenced and audited. Use it to reach the export gate quickly.")
        if st.button("Fast-track demo resolution", key="ex_fast", disabled=not has("admin")):
            try:
                n = fasttrack_demo()
                st.toast(f"{n} exceptions dispositioned — readiness gates re-evaluated", icon="⚡")
                st.rerun()
            except HubError as e:
                st.error(str(e))

    if view.empty:
        st.info("No exceptions match the filters.")
        return
    labels = {f"{r.id} · {r.severity.upper()} · {r.title} · {r.status}": r.id for r in view.itertuples()}
    sel_label = sel("Open exception detail", list(labels), "ex_sel")
    e = dict(q("SELECT * FROM exceptions WHERE id=?", (labels[sel_label],))[0])
    color = STATUS_COLOR.get(e["status"], C["muted"])
    sev_c = SEV_COLOR.get(e["severity"], C["muted"])
    st.markdown(f"<div class='panel'><h4>{esc(e['id'])} — {esc(e['title'])}</h4>"
                f"{pill_html(e['severity'].upper(), sev_c)} {pill_html(e['status'], color)} "
                f"<span class='chip blue'>{esc(e['kind'])}</span> <span class='chip'>{esc(e['rule_id'])}</span><br><br>"
                f"<span class='small'>entity {esc(e['entity_type'])}:{esc(e['entity_id'])} · opened {e['opened_at']} · "
                f"assignee {esc(e['assignee'] or '—')}</span></div>", unsafe_allow_html=True)
    detail = json.loads(e["detail_json"])
    st.json(detail)
    if e["resolution"]:
        st.info(f"Resolution: {e['resolution']}")
    if e["override_reason"]:
        st.markdown(f"🟣 **Override:** {e['override_reason']}")

    adjs = qdf("SELECT * FROM adjustments WHERE exception_id=?", (e["id"],))
    if not adjs.empty:
        st.markdown("**Correction adjustments (append-only):**")
        adjs_disp = adjs.copy()
        adjs_disp["amount"] = [money(int(v), c) for v, c in zip(adjs.amount_cents, adjs.currency)]
        dtable(adjs_disp[["id", "amount", "reason", "actor", "created_at"]], height=140)

    if e["kind"] in {"missing_posting", "amount_mismatch"} and e["status"] not in CLEARED:
        with st.expander("🛠️ Post correction adjustment", expanded=True):
            pairs, _, _, _ = recon_all()
            pr = pairs[pairs.key == e["entity_id"]]
            suggested = int(pr.iloc[0].var_cents) / 100 if not pr.empty else 0.0
            ccy = e["entity_id"].split("|")[1]
            amt = st.number_input(f"Ledger adjustment ({ccy}, suggested = current variance)",
                                  value=float(suggested), step=0.01, format="%.2f", key="adj_amt")
            reason = st.text_input("Correction reason (evidence, >=10 chars)", key="adj_reason",
                                   placeholder="e.g. Re-posted missing batch BAT-ACC-1002-1014 per gateway confirmation")
            if st.button("Post adjustment", key="adj_go", disabled=not has("correct")):
                try:
                    aid = add_adjustment(e["id"], to_cents(amt), reason)
                    st.toast(f"Adjustment {aid} posted — pair re-reconciled", icon="🛠️")
                    st.rerun()
                except HubError as err:
                    st.error(str(err))

    with st.expander("📎 Evidence attachments"):
        ev_up = st.file_uploader("Attach evidence (any file <=10 MB)", key=f"ev_up_{e['id']}")
        if ev_up is not None and st.button("Store evidence", key=f"ev_go_{e['id']}",
                                           disabled=not (has("resolve") or has("correct"))):
            data = ev_up.getvalue()
            if len(data) > 10 * 1024 * 1024:
                st.error("Evidence exceeds 10 MB limit.")
            else:
                sha = sha_b(data)
                path = EV_DIR / f"{sha}_{ev_up.name}"
                if not path.exists(): path.write_bytes(data)
                a, _ = actor()
                x("INSERT INTO evidence(id,exception_id,filename,sha256,path,actor,created_at) VALUES(?,?,?,?,?,?,?)",
                    (hid("EV"), e["id"], ev_up.name, sha, str(path), a, now_iso()))
                audit("evidence_attached", "exception", e["id"], {"file": ev_up.name, "sha256": sha})
                st.toast("Evidence stored with checksum", icon="📎")
                st.rerun()
        evs = qdf("SELECT * FROM evidence WHERE exception_id=?", (e["id"],))
        for r in evs.itertuples():
            p = Path(r.path)
            if p.exists():
                st.download_button(f"📎 {r.filename} ({r.sha256[:10]}…)", p.read_bytes(), r.filename,
                                   key=f"ev_dl_{r.id}")

    st.markdown("**Workflow transitions** (permission-enforced):")
    targets = EXC_FLOW.get(e["status"], [])
    note = st.text_input("Transition note / override reason", key="ex_note",
                         placeholder="Overrides need >=20 chars; resolutions >=10 chars")
    cols = st.columns(max(1, len(targets)))
    for i, tgt in enumerate(targets):
        with cols[i]:
            label = {"Investigating": "🔍 Investigate", "Resolved": "🛠️ Mark resolved",
                     "Verified": "✅ Verify", "Closed": "🔒 Close", "Overridden": "🟣 Override",
                     "Open": "↩️ Reopen (undo)"}[tgt]
            if st.button(label, key=f"ex_tr_{tgt}", disabled=not has(TARGET_PERM[tgt])):
                try:
                    exc_transition(e["id"], tgt, note)
                    st.toast(f"{e['id']} → {tgt}", icon="🔁")
                    st.rerun()
                except HubError as err:
                    st.error(str(err))
    with st.expander("🕒 Audit timeline for this exception"):
        tl = qdf("SELECT ts,actor,action,detail_json FROM audit WHERE entity_id=? ORDER BY id", (e["id"],))
        if not tl.empty:
            dtable(tl, height=200)

# ──────────────────────────────────────────────────────────────────────────────
# Page 5 — Records & Lineage
# ──────────────────────────────────────────────────────────────────────────────
def render_record_detail(txn_id: str):
    r = q("SELECT * FROM records WHERE txn_id=?", (txn_id,))
    if not r:
        st.error("Record not found.")
        return
    rec = dict(r[0])
    pairs, _, _, _ = recon_all()
    key = f"{rec['account_id']}|{rec['currency']}|{rec['value_date']}"
    pr = pairs[pairs.key == key]
    pair_status = pr.iloc[0].status if not pr.empty else "unknown"
    approved = bool(q("SELECT id FROM exports WHERE status='Approved'"))
    stage = ("exported / approved" if approved else
             "matched" if pair_status in ("matched", "accepted") else "in reconciliation (break)")
    st.markdown(f"{pill_html(rec['status'].upper(), C['teal'])} {pill_html(stage.upper(), STATUS_COLOR.get(pair_status, C['blue']))}",
                unsafe_allow_html=True)
    st.markdown(f"**{rec['txn_id']}** · {rec['account_id']} · **{money(rec['amount_cents'], rec['currency'])}** · "
                f"value {rec['value_date']} · booked {rec['booked_at']} · {rec['category']} / {rec['channel']} / {rec['country']}")
    st.markdown("**Lineage chain** (source → file → row → transform):")
    lin = qdf("SELECT * FROM lineage WHERE entity_type='record' AND entity_id=?", (txn_id,))
    if not lin.empty:
        dtable(lin[["source_type", "source_id", "source_file", "source_row", "transform_version", "created_at"]], height=120)
    adjs = qdf("SELECT a.* FROM adjustments a WHERE a.account_id=? AND a.currency=? AND a.value_date=?",
               (rec["account_id"], rec["currency"], rec["value_date"]))
    if not adjs.empty:
        st.markdown("**Related adjustments on this pair:**")
        dtable(adjs[["id", "exception_id", "amount_cents", "reason", "actor"]], height=120)
    aud = qdf("SELECT ts,actor,action FROM audit WHERE entity_id=? ORDER BY id", (txn_id,))
    if not aud.empty:
        st.markdown("**Audit:**")
        dtable(aud, height=140)

def page_records():
    hdr("🗂️ Records & Lineage", "Search, filter, sort, paginate. Full provenance for every derived figure. "
        "Saved views persist for this session.")
    df = qdf("SELECT * FROM records")
    if df.empty:
        st.info("No records yet — import a sample file.")
        return
    c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
    qtext = c1.text_input("Search txn / account", key="rec_q")
    acct = sel("Account", ["All"] + sorted(df.account_id.unique().tolist()), "rec_acct")
    ccy = c3.selectbox("Currency", ["All", "USD", "EUR", "GBP"], key="rec_ccy")
    cat = c4.selectbox("Category", ["All"] + CATEGORIES, key="rec_cat")
    c5, c6, c7 = st.columns([2, 1, 1])
    sort_f = c5.selectbox("Sort by", ["created_at", "value_date", "amount_cents", "txn_id", "account_id"], key="rec_sort")
    sort_d = c6.selectbox("Direction", ["desc", "asc"], key="rec_dir")
    page_sz = c7.selectbox("Page size", [25, 50, 100], key="rec_ps")
    v1, v2, v3 = st.columns([2, 1, 1])
    view_name = v1.text_input("Save current filters as view", key="rec_vname", placeholder="e.g. EUR large repayments")
    with v2:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        if st.button("💾 Save view", key="rec_vsave") and view_name:
            st.session_state.saved_views[view_name] = {"q": qtext, "acct": acct, "ccy": ccy, "cat": cat,
                                                       "sort": sort_f, "dir": sort_d}
            st.toast(f"View '{view_name}' saved", icon="💾")
    with v3:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        if st.session_state.saved_views:
            pick = st.selectbox("Load view", ["—"] + list(st.session_state.saved_views), key="rec_vpick",
                                label_visibility="collapsed")
            if pick != "—":
                v = st.session_state.saved_views[pick]
                for k, val in [("rec_q", v["q"]), ("rec_acct", v["acct"]), ("rec_ccy", v["ccy"]),
                               ("rec_cat", v["cat"]), ("rec_sort", v["sort"]), ("rec_dir", v["dir"])]:
                    st.session_state[k] = val
                st.rerun()
    view = df.copy()
    if qtext: view = view[view.txn_id.str.contains(qtext, case=False) | view.account_id.str.contains(qtext, case=False)]
    if acct != "All": view = view[view.account_id == acct]
    if ccy != "All": view = view[view.currency == ccy]
    if cat != "All": view = view[view.category == cat]
    view = view.sort_values(sort_f, ascending=sort_d == "asc").reset_index(drop=True)
    n_pages = max(1, math.ceil(len(view) / page_sz))
    if st.session_state.get("rec_pg", 1) > n_pages:
        st.session_state.rec_pg = 1
    pg = st.number_input(f"Page (1–{n_pages})", 1, n_pages, 1, key="rec_pg")
    sl = view.iloc[(pg - 1) * page_sz: pg * page_sz].copy()
    sl["amount"] = [money(int(v), c) for v, c in zip(sl.amount_cents, sl.currency)]
    st.caption(f"{len(view):,} records match · showing {len(sl)} · total {len(df):,}")
    dtable(sl[["txn_id", "account_id", "currency", "amount", "value_date", "booked_at",
               "category", "channel", "country", "status", "import_id"]], height=360)
    st.download_button("⬇️ Export filtered records (CSV)", view.to_csv(index=False).encode(),
                       "records_filtered.csv", "text/csv", key="rec_dl")
    txn = sel("Open record drawer", ["—"] + sl.txn_id.tolist(), "rec_detail")
    if txn and txn != "—":
        if hasattr(st, "dialog"):
            try:
                st.dialog("Record detail & lineage", width="large")(render_record_detail)(txn)
            except TypeError:
                st.dialog("Record detail & lineage")(render_record_detail)(txn)
        else:
            with st.expander("Record detail & lineage", expanded=True):
                render_record_detail(txn)

# ──────────────────────────────────────────────────────────────────────────────
# Page 6 — Reports & Submission
# ──────────────────────────────────────────────────────────────────────────────
def page_reports():
    hdr("📦 Reports & Submission", "Readiness gates → prepare pack → independent reviewer approval → export. "
        "Decision support: submission always requires authorized human approval.")
    pairs, fp, rp, ver = recon_all()
    gates = readiness(pairs)
    st.markdown("<div class='panel'><h4>Readiness gates (observable pass / fail)</h4>", unsafe_allow_html=True)
    for g in gates:
        st.markdown(f"{pill_html('PASS' if g['ok'] else 'FAIL', C['green'] if g['ok'] else C['red'])} "
                    f"**{g['id']}** — {esc(g['label'])} <span class='small'>· {esc(g['detail'])}</span>",
                    unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)
    if st.button("📦 Prepare submission pack", type="primary", key="pk_prepare", disabled=not has("prepare")):
        try:
            pid = pack_prepare()
            st.toast(f"Pack {pid} prepared — awaiting independent review", icon="📦")
            st.rerun()
        except HubError as e:
            st.error(str(e))
    packs = qdf("SELECT * FROM exports ORDER BY created_at DESC")
    if packs.empty:
        st.info("No pack yet. Resolve exceptions (Exceptions & Review), then prepare. "
                "Administrators can use the audited fast-track to see the full loop quickly.")
        return
    labels = {f"{r.id} · {r.status} · {r.created_at} · by {r.prepared_by}": r.id for r in packs.itertuples()}
    sel_label = sel("Select pack", list(labels), "pk_sel")
    p = dict(q("SELECT * FROM exports WHERE id=?", (labels[sel_label],))[0])
    color = STATUS_COLOR.get(p["status"], C["muted"])
    st.markdown(f"{pill_html(p['status'], color)} <span class='chip teal'>fingerprint {p['pack_fp']}</span> "
                f"<span class='chip blue'>rules {ver}</span>", unsafe_allow_html=True)
    with zipfile.ZipFile(io.BytesIO(Path(p["path"]).read_bytes())) as z:
        summary = json.loads(z.read("summary.json"))
        manifest = json.loads(z.read("manifest.json"))
        totals = z.read("reconciliation_totals.csv").decode()
        exc_csv = z.read("exceptions.csv").decode()
    t1, t2, t3, t4 = st.tabs(["📄 Summary report", "⚖️ Totals", "🧯 Exceptions", "🔏 Manifest"])
    with t1:
        st.markdown(f"""
**{summary['organization']}** — Reporting period **{summary['period']}**
Prepared by **{summary['prepared_by']}** at {summary['generated_at']} · rules **{summary['rules_version']}**

| metric | value |
|---|---|
| validated records | {summary['records']:,} |
| reconciliation pairs | {summary['pairs']} |
| coverage | {summary['coverage']*100:.2f}% |
| quality score | {summary['quality_score']} |
| pack fingerprint | `{summary['pack_fingerprint']}` |

> ⚠️ {summary['disclaimer']}
""")
        st.download_button("⬇️ Summary (JSON)", cj(summary).encode(), f"summary_{p['id']}.json",
                           "application/json", key="pk_sum_dl")
    with t2:
        dtable(pd.read_csv(io.StringIO(totals)), height=300)
    with t3:
        edf = pd.read_csv(io.StringIO(exc_csv))
        dtable(edf, height=300) if not edf.empty else st.success("No exceptions.")
    with t4:
        st.json(manifest)
        with zipfile.ZipFile(io.BytesIO(Path(p["path"]).read_bytes())) as z:
            ok = all(sha_b(z.read(name)) == meta["sha256"] for name, meta in manifest["files"].items())
        st.markdown(pill_html("MANIFEST HASHES VERIFIED" if ok else "HASH MISMATCH", C["green"] if ok else C["red"]),
                    unsafe_allow_html=True)
    st.download_button("⬇️ Download submission pack (.zip" +
                       (" + approval.json" if p["status"] == "Approved" else "") + ")",
                       pack_bytes(p), Path(p["path"]).name, "application/zip", key="pk_dl")
    if p["status"] == "Prepared":
        with st.expander("🔎 Reviewer decision (separation of duties enforced)", expanded=True):
            reason = st.text_input("Decision reason (>=10 chars)", key="pk_reason",
                                   placeholder="e.g. Verified all corrections against gateway confirmations")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("✅ Approve pack", key="pk_ok", disabled=not has("approve")):
                    try:
                        pack_decide(p["id"], True, reason)
                        st.toast("Pack approved — approval.json will accompany the download", icon="✅")
                        st.rerun()
                    except HubError as e:
                        st.error(str(e))
            with c2:
                if st.button("❌ Reject pack", key="pk_no", disabled=not has("approve")):
                    try:
                        pack_decide(p["id"], False, reason)
                        st.toast("Pack rejected — returned to preparation", icon="❌")
                        st.rerun()
                    except HubError as e:
                        st.error(str(e))
    else:
        st.info(f"Decision by **{p['approved_by']}** at {p['decided_at']}: {p['reason']}")

# ──────────────────────────────────────────────────────────────────────────────
# Page 7 — Audit & Governance
# ──────────────────────────────────────────────────────────────────────────────
def page_governance():
    hdr("🛡️ Audit & Governance", "Hash-chained audit trail, rule versioning, retention, backup/restore, "
        "access control and the product / risk documentation.")
    t1, t2, t3, t4, t5 = st.tabs(["🔗 Audit chain", "📜 Rules", "💾 Backup & health", "🔐 Access & security", "📚 Product docs"])
    with t1:
        aud = qdf("SELECT * FROM audit ORDER BY id DESC")
        c1, c2, c3 = st.columns(3)
        act_f = sel("Action", ["All"] + sorted(aud.action.unique().tolist()), "au_act")
        who_f = sel("Actor", ["All"] + sorted(aud.actor.unique().tolist()), "au_who")
        qq = c3.text_input("Search entity / detail", key="au_q")
        v = aud.copy()
        if act_f != "All": v = v[v.action == act_f]
        if who_f != "All": v = v[v.actor == who_f]
        if qq: v = v[v.entity_id.str.contains(qq, case=False) | v.detail_json.str.contains(qq, case=False)]
        dtable(v[["id", "ts", "actor", "role", "action", "entity_type", "entity_id", "hash"]].head(300), height=320)
        if st.button("🔗 Verify hash chain", key="au_verify"):
            ok, msg, n = chain_verify()
            (st.success if ok else st.error)(msg)
        st.caption("Append-only in the MVP. Enterprise path: periodic signed export + anchored truncation.")
        st.download_button("⬇️ Full audit extract (CSV)", aud.to_csv(index=False).encode(),
                           "audit_extract.csv", "text/csv", key="au_dl")
    with t2:
        rv = qdf("SELECT version,note,activated_at,activated_by,active FROM rule_versions ORDER BY activated_at DESC")
        dtable(rv, height=180)
        cur, ver = rules()
        st.json(cur)
        pick = sel("Rollback to version", rv.version.tolist(), "rv_pick")
        if st.button("↩️ Rollback (audited)", key="rv_go", disabled=not has("rules")):
            try:
                rules_rollback(pick)
                st.toast(f"Rule version {pick} re-activated", icon="↩️")
                st.rerun()
            except HubError as e:
                st.error(str(e))
        st.caption("Propose tolerance/grace changes from the Reconciliation page — every activation is a new version.")
    with t3:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Backup**")
            x("PRAGMA wal_checkpoint(TRUNCATE)")
            st.download_button("⬇️ Download database backup (.db)", DB_PATH.read_bytes(),
                               f"hub_backup_{datetime.utcnow():%Y%m%d_%H%M}.db", "application/octet-stream", key="bk_dl")
            if st.button("📁 Snapshot to backups folder", key="bk_snap"):
                dest = BK_DIR / f"hub_{datetime.utcnow():%Y%m%d_%H%M%S}.db"
                shutil.copy2(DB_PATH, dest)
                audit("backup_created", "database", dest.name, {"sha256": sha_f(dest)})
                st.toast(f"Snapshot saved: {dest.name}", icon="📁")
            st.markdown("**Restore** (validated: table set + PRAGMA integrity_check; safety copy kept)")
            up = st.file_uploader("Upload backup .db", type=["db"], key="bk_up")
            if up is not None and st.button("♻️ Restore now", key="bk_restore", disabled=not has("restore")):
                try:
                    require("restore")
                    tmp = BK_DIR / "restore_candidate.db"
                    tmp.write_bytes(up.getvalue())
                    with closing(sqlite3.connect(tmp)) as con:
                        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                        need = {"meta", "records", "ledger", "exceptions", "audit", "imports"}
                        if not need.issubset(tables):
                            raise HubError(f"Backup missing tables: {need - tables}")
                        if con.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                            raise HubError("Backup failed SQLite integrity_check.")
                    safety = BK_DIR / f"pre_restore_{datetime.utcnow():%Y%m%d_%H%M%S}.db"
                    shutil.copy2(DB_PATH, safety)
                    shutil.copy2(tmp, DB_PATH)
                    for suf in ("-wal", "-shm"):
                        p = Path(str(DB_PATH) + suf)
                        if p.exists(): p.unlink()
                    audit("database_restored", "database", up.name, {"safety_copy": safety.name})
                    st.toast("Restore complete — reloading", icon="♻️")
                    st.rerun()
                except HubError as e:
                    st.error(str(e))
        with c2:
            st.markdown("**Health checks**")
            integ = q("PRAGMA integrity_check")[0][0]
            fk = q("PRAGMA foreign_key_check")
            st.markdown(pill_html(f"DB integrity: {integ}", C["green"] if integ == "ok" else C["red"]) + " " +
                        pill_html(f"FK violations: {len(fk)}", C["green"] if not fk else C["red"]),
                        unsafe_allow_html=True)
            ups = list(UP_DIR.glob("*"))[:3]
            for p in ups:
                st.caption(f"file {p.name[:18]}… · sha256 {sha_f(p)[:16]}…")
            st.caption(f"DB size {DB_PATH.stat().st_size/1024:.0f} KB · uploads {len(list(UP_DIR.glob('*')))} · "
                       f"evidence {len(list(EV_DIR.glob('*')))} · exports {len(list(EX_DIR.glob('*')))}")
            st.markdown("**Retention** (audit is immutable; pruning targets transient data only)")
            days = st.number_input("Retain sim events / orphan files (days)", 1, 365,
                                   int(meta_get("retention_days", 30)), key="rt_days")
            if st.button("Save retention policy", key="rt_save", disabled=not has("admin")):
                require("admin")
                meta_set("retention_days", int(days))
                audit("retention_updated", "policy", "retention", {"days": int(days)})
                st.toast("Retention policy saved", icon="🗓️")
            if st.button("🧹 Prune now", key="rt_prune", disabled=not has("prune")):
                require("prune")
                cutoff = (datetime.utcnow() - timedelta(days=int(days))).strftime("%Y-%m-%dT%H:%M:%SZ")
                n0 = q("SELECT COUNT(*) n FROM sim_events")[0]["n"]
                x("DELETE FROM sim_events WHERE ts < ?", (cutoff,))
                n1 = q("SELECT COUNT(*) n FROM sim_events")[0]["n"]
                referenced = {r["sha256"] for r in q("SELECT sha256 FROM imports")}
                orphans = 0
                for p in UP_DIR.glob("*"):
                    if p.stem not in referenced:
                        p.unlink(); orphans += 1
                audit("retention_pruned", "database", "prune",
                      {"sim_events_removed": n0 - n1, "orphan_files_removed": orphans, "cutoff": cutoff})
                st.toast(f"Pruned {n0-n1} sim events, {orphans} orphan files", icon="🧹")
    with t4:
        st.markdown("**Access-control matrix** (enforced in service functions, not only UI)")
        perms = sorted({p for r in ROLES.values() for p in r["perms"]})
        mat = pd.DataFrame([{**{"permission": p},
                             **{ROLES[rk]["title"]: ("✅" if p in ROLES[rk]["perms"] else "—") for rk in ROLES}}
                            for p in perms])
        dtable(mat, height=320)
        st.markdown("""
**Threat model (MVP scope)** — spoofing: persona switch is a demo affordance; team edition adds real authentication
(password hashing via a recognized library) and server-side sessions. Tampering: hash-chained audit + file checksums +
append-only sources. Repudiation: every material change carries actor, role, timestamp and reason. Information
disclosure: synthetic data only; sensitivity classes in the data dictionary; secrets never in source control.
Denial of service: 10 MB upload cap, pagination, sim-event retention. Elevation: permission checks raise before any write.

**Sensitive-data handling** — no real customer/account data in the demo; production requires de-identification,
encrypted transport (HTTPS), environment-managed secrets and documented retention. Injection: all SQL parameterized.
""")
        st.markdown("**License inventory / SBOM (commercial-compatible, open-source-first)**")
        lic = pd.DataFrame([
            ("streamlit", "Apache-2.0", "application framework"),
            ("pandas", "BSD-3-Clause", "tabular processing"),
            ("numpy", "BSD-3-Clause", "numerics"),
            ("plotly", "MIT", "interactive charts"),
            ("openpyxl", "MIT", "XLSX I/O"),
            ("matplotlib", "Matplotlib (PSF-based)", "film frame rendering"),
            ("imageio", "BSD-2-Clause", "video/GIF writing"),
            ("imageio-ffmpeg", "BSD-2-Clause (bundled FFmpeg build: LGPL/GPL — review before redistribution)", "local MP4 encoding"),
            ("pillow", "MIT-CMU", "GIF fallback"),
            ("Python stdlib (sqlite3, zipfile, hashlib)", "PSF", "storage, packs, hashing"),
        ], columns=["component", "license", "purpose"])
        dtable(lic, height=300)
    with t5:
        with st.expander("📄 Product brief & personas", expanded=True):
            st.markdown(f"""
**Problem.** Finance-ops teams reconcile source extracts to ledger totals in spreadsheets before regulatory
filings. Failures: silent row corruption, untraceable corrections, late/missing files, no separation of duties,
no evidence pack. **Narrow MVP workflow:** import → validate → reconcile → exception review → approval → export,
end-to-end on one Windows machine with synthetic data.

**Personas.** Amara (Administrator: rules, retention, restore) · Diego (Analyst: imports, corrections, preparation) ·
Priya (Reviewer: verification, overrides, approval — cannot prepare) · Sam (Stakeholder: read-only, downloads).

**In scope (MVP).** CSV/XLSX import with row errors · deterministic reconciliation with tolerances & timing ·
robust-z anomaly baseline · evidence & hash-chained audit · role enforcement · submission pack with manifest ·
live simulation · local demo film. **Out of scope (MVP).** Real regulator APIs, trained ML claims, multi-user auth,
SSO, certified compliance, autonomous decisions.

**Measurable success criteria.** 100% of malformed sample rows rejected with actionable errors · hand-check
example reproduced exactly · fingerprint stable across reruns · pack export gated by observable checks ·
self-test suite passes · clean-DB demo repeatable via documented commands. *No customer-impact claims until
piloted with real users.*
""")
        with st.expander("🗃️ Backlog — MVP / post-MVP / enterprise + 90-day roadmap"):
            dtable(pd.DataFrame([
                ("MVP ✅", "Import + validation + error reports", "done"),
                ("MVP ✅", "Reconciliation engine + explanations + fingerprints", "done"),
                ("MVP ✅", "Exceptions, corrections, overrides, evidence", "done"),
                ("MVP ✅", "Roles, audit chain, packs, backup/restore, demo film", "done"),
                ("Post-MVP", "FastAPI service (Pydantic, pagination, rate limits)", "weeks 1–4 of pilot"),
                ("Post-MVP", "Real authentication + PostgreSQL team edition", "weeks 3–8"),
                ("Post-MVP", "Scheduled file-drop adapters (SFTP/watch folder), FX reference data", "weeks 5–10"),
                ("Post-MVP", "Held-out real-data evaluation with practitioner review", "weeks 6–12"),
                ("Enterprise", "SSO/directory, audit exports, deployment controls, SLAs", "post-pilot"),
                ("Enterprise", "Docker + hardened Linux hosting, monitoring & incident runbook", "post-pilot"),
            ], columns=["tier", "capability", "timing"]), height=300)
            st.caption("90-day roadmap is driven by pilot feedback and measured usage — willingness to pay is "
                       "validated via interviews, not inferred from technical novelty.")
        with st.expander("⚠️ Risk register"):
            dtable(pd.DataFrame([
                ("Domain validity", "Rules mis-model reporting requirements", "High", "High", "Practitioner review; rule versioning; decision-support framing", "Reviewer"),
                ("Data quality", "Silent source corruption / duplicates", "Med", "High", "Row validation, checksums, append-only, quarantine", "Analyst"),
                ("Security", "Unauthorized approval / data leak", "Med", "High", "Server-side permission checks, audit chain, synthetic demo data", "Admin"),
                ("Adoption", "Teams keep spreadsheets", "Med", "Med", "Narrow workflow, fast-track demo, pilot success metrics", "Product"),
                ("Operational", "DB corruption / lost evidence", "Low", "High", "WAL, integrity checks, backup/restore with safety copies", "Admin"),
                ("Model risk", "Overclaiming anomaly detection", "Med", "Med", "Deterministic baseline, synthetic-only eval, limitations shown", "Reviewer"),
                ("Licensing", "FFmpeg build redistribution obligations", "Low", "Med", "License inventory; GIF fallback; legal review before bundling", "Admin"),
                ("Regulatory", "Prototype mistaken for compliance tool", "Med", "High", "Watermarks, human-approval gate, no certification claims", "Product"),
            ], columns=["category", "risk", "likelihood", "impact", "mitigation", "owner"]), height=280)
        with st.expander("🧱 Schema, lineage & PostgreSQL migration plan"):
            dtable(pd.DataFrame([
                ("organizations/users", "meta + ROLES (MVP) → users table (team)", "identity & tenancy"),
                ("accounts", "account_id PK, currency, kind", "reference catalog"),
                ("imports", "id PK, sha256, provenance, counts", "file registry"),
                ("records", "txn_id PK → imports FK, integer cents", "operational source rows"),
                ("ledger", "entry_id PK, batch_ref, booked_at", "ledger postings"),
                ("adjustments", "id PK → exceptions FK", "append-only corrections"),
                ("exceptions", "id PK, fingerprint dedupe, lifecycle", "review queue"),
                ("audit", "id PK, prev_hash→hash chain", "immutable evidence"),
                ("lineage", "entity → source file/row/transform", "traceability"),
                ("rule_versions / exports / evidence / sim_events", "versioned config, packs, attachments, telemetry", "governance"),
            ], columns=["entity", "keys / relationships", "purpose"]), height=300)
            st.markdown("""
**Migration path.** 1) Freeze writes, snapshot SQLite. 2) Create PostgreSQL schema from the same DDL
(types: INTEGER cents → BIGINT, TEXT timestamps → TIMESTAMPTZ). 3) Copy tables in FK order with checksums per table.
4) Verify row counts + audit chain head. 5) Switch connection layer (one `db()` function), enable multi-user auth,
rate limits and concurrent-transaction tests. Large files stay on disk/object storage; DB keeps metadata + SHA-256.

**Backup/restore runbook.** Download or snapshot from *Backup & health* → restore validates table set and
`PRAGMA integrity_check`, keeps a pre-restore safety copy, and is audited. Test restore quarterly; record evidence.
""")
        with st.expander("💼 Commercial hypothesis & release tiers"):
            st.markdown("""
**Buyer:** Head of Finance Operations at a payments company / lender preparing monthly regulatory returns.
**Expensive workflow today:** 2–4 analyst-days per period of spreadsheet reconciliation and evidence chasing.
**Pricing hypothesis:** local MVP free evaluation (synthetic demo) → Team edition per-seat subscription with shared
PostgreSQL, roles and approvals → Enterprise site license with SSO, audit exports, deployment controls and annual
support; paid integration services. Pricing metric aligned to seats + periods processed (predictable cost).
**Pilot success metric:** >=50% reduction in reconciliation prep time on the pilot's own permissioned data, with
zero unexplained figures — measured, not assumed. No compliance certifications are claimed.
""")

# ──────────────────────────────────────────────────────────────────────────────
# Page 8 — Demo Studio
# ──────────────────────────────────────────────────────────────────────────────
def page_studio():
    hdr("🎬 Demo Studio", "The working demo film is generated locally from live database figures — no API, no stock "
        "footage. Regenerate with your own scene mix, then run the quality gates.")
    film = film_ready()
    c1, c2 = st.columns([3, 2])
    with c1:
        if film:
            if film.suffix == ".mp4":
                st.video(str(film))
            else:
                st.image(str(film))
            st.download_button("⬇️ Download demo film", film.read_bytes(), film.name,
                               "video/mp4" if film.suffix == ".mp4" else "image/gif", key="fl_dl")
            st.caption(f"{film.name} · {film.stat().st_size/1024:.0f} KB · params {meta_get('film_params', {})}")
        else:
            st.info("No film yet — it auto-renders on the Command Center's first run, or generate it here.")
    with c2:
        st.markdown("**Render controls (applied on regenerate)**")
        dur = st.slider("Duration (seconds)", 8, 30, 14, key="fl_dur")
        fps = st.selectbox("Frame rate", [10, 12, 15, 24], index=1, key="fl_fps")
        res = st.selectbox("Resolution", ["640×360", "800×450", "960×540"], index=1, key="fl_res")
        sc1 = st.toggle("Scene 1 · Seeded & live", True, key="fl_s1")
        sc2 = st.toggle("Scene 2 · Ingest & validate", True, key="fl_s2")
        sc3 = st.toggle("Scene 3 · Reconcile & correct", True, key="fl_s3")
        sc4 = st.toggle("Scene 4 · Review & export", True, key="fl_s4")
        scenes = tuple(i for i, on in [(1, sc1), (2, sc2), (3, sc3), (4, sc4)] if on)
        if st.button("🎬 Regenerate film", type="primary", key="fl_go"):
            if not scenes:
                st.error("Select at least one scene.")
            else:
                with st.status("Rendering film locally…", expanded=True) as s:
                    pbar = st.progress(0.0)
                    def prog(v, msg):
                        pbar.progress(min(1.0, v), text=msg)
                    try:
                        out = render_film(dur, fps, res, scenes, prog)
                        s.update(label=f"Ready — {out.name}", state="complete")
                        st.rerun()
                    except Exception as e:
                        s.update(label="Render failed", state="error")
                        st.error(f"{e}. If FFmpeg is unavailable the app falls back to an animated GIF automatically.")
        st.caption("Pipeline: matplotlib frames (Agg) → imageio → bundled FFmpeg (libx264, yuv420p, faststart). "
                   "GIF fallback keeps the demo usable if the encoder is missing.")

    st.markdown("### ✅ Quality gates — in-app self-test suite")
    if st.button("🧪 Run self-tests", key="qt_run"):
        st.session_state.test_results = self_tests()
    res = st.session_state.test_results
    if res:
        rdf = pd.DataFrame(res)
        n_pass = int((rdf.result == "PASS").sum())
        (st.success if n_pass == len(rdf) else st.error)(f"{n_pass}/{len(rdf)} tests passed · "
                                                         f"{rdf.ms.sum():.0f} ms total")
        dtable(rdf, height=320)
    else:
        st.info("Run the suite: cents arithmetic, every validation rule, tolerance boundary hand-check, "
                "fingerprint reproducibility, audit tamper detection, permission enforcement, anomaly baseline, "
                "manifest integrity.")

    with st.expander("📈 Anomaly baseline evaluation — synthetic holdout (plumbing test, not a real-world claim)"):
        edf, m = baseline_eval()
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Precision", f"{m['precision']:.2f}")
        m2.metric("Recall", f"{m['recall']:.2f}")
        m3.metric("F1", f"{m['f1']:.2f}")
        m4.metric("False alarm rate", f"{m['false_alarm_rate']:.3f}")
        if not edf.empty:
            dtable(edf, height=220)
        st.caption(f"Method: rolling median ± k·1.4826·MAD (deterministic, untrained). Split: {m['holdout']} · "
                   f"{m['n_eval']} account-days. Ground truth = injected volume spikes in synthetic seed data. "
                   "These numbers validate software plumbing only — real predictive claims require representative, "
                   "permissioned, independently evaluated data.")

    st.markdown("### 📋 Acceptance checklist (observable)")
    pairs, fp, _, ver = recon_all()
    _, berr, _ = validate_frame(pd.read_csv(io.BytesIO(sample_broken_csv()), dtype=str, keep_default_na=False),
                                set(), rules()[0])
    v_ok, _, _ = validate_frame(pd.read_csv(io.BytesIO(sample_valid_csv()), dtype=str, keep_default_na=False),
                                set(), rules()[0])
    chain_ok, chain_msg, _ = chain_verify()
    checks = [
        ("Sample valid file imports cleanly", len(v_ok) == 24, f"{len(v_ok)}/24 rows valid"),
        ("Broken file rejected with actionable errors", len({e['rule_id'] for e in berr}) >= 6,
         f"{len(berr)} issues across {len({e['rule_id'] for e in berr})} rules"),
        ("Seed dataset loaded (no blank dashboard)", bool(q("SELECT COUNT(*) n FROM records")[0]["n"]),
         f"{q('SELECT COUNT(*) n FROM records')[0]['n']} records"),
        ("Reproducible calculation fingerprint", recon_all()[1] == fp, fp),
        ("Audit hash chain verifies", chain_ok, chain_msg),
        ("Permissions enforced server-side", not has("approve", "analyst") and not has("import", "stakeholder"),
         "analyst != approver, stakeholder read-only"),
        ("Submission pack exported", bool(q("SELECT id FROM exports")),
         "prepare + approve in Reports & Submission"),
        ("Live demo film generated in-app", film_ready() is not None,
         film_ready().name if film_ready() else "pending first render"),
        ("Self-test suite passing", bool(res) and all(r["result"] == "PASS" for r in res),
         "run the suite above"),
    ]
    for label, ok, detail in checks:
        st.markdown(f"{pill_html('PASS' if ok else 'PENDING/FAIL', C['green'] if ok else C['amber'])} "
                    f"{esc(label)} <span class='small'>· {esc(str(detail))}</span>", unsafe_allow_html=True)

    with st.expander("🎤 Presenter demo script (repeatable from a clean database)"):
        script = f"""# {APP_NAME} — 6-minute demo script
1. `streamlit run app.py` — dashboard opens populated; live feed already ticking; film auto-renders once.
2. Command Center — point at coverage gauge (<99.5%), red break markers, exception donut, workflow sankey.
3. Sidebar — move anomaly slider to 25%, press Inject anomaly; watch flagged events; Commit staged batch.
4. Import & Validate — run broken-sample demo; show rule IDs + fixes; download error report; run valid sample; commit.
5. Reconciliation — select the -EUR 310.55 pair; read the explanation panel; verify fingerprint.
6. Exceptions & Review — post the suggested adjustment on the missing-posting break; Verify -> Close;
   override the timing break with a >=20-char reason; attach evidence. (Or Admin fast-track.)
7. Reports & Submission — gates now PASS; Prepare pack; switch persona to Priya (Reviewer); Approve;
   download the zip and show manifest hashes + approval.json.
8. Audit & Governance — verify the hash chain; roll back a rule version; download a DB backup.
9. Demo Studio — play the generated film; run self-tests; show the acceptance checklist.
Reset: sidebar Reset demo data -> everything re-seeds deterministically (seed {SEED}).
"""
        st.markdown(script)
        st.download_button("⬇️ Demo script (Markdown)", script.encode(), "demo_script.md", "text/markdown", key="ds_dl")

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main():
    st.set_page_config(page_title=f"{APP_NAME} — {APP_TAG}", page_icon="🛡️", layout="wide",
                       initial_sidebar_state="expanded")
    st.markdown(CSS, unsafe_allow_html=True)
    init_state()
    fresh = not DB_PATH.exists()
    ensure_schema()
    if fresh:
        with st.status("🧬 Provisioning synthetic ledger, engineered breaks & audit genesis…", expanded=False) as s:
            seed_if_empty()
            s.update(label="Synthetic dataset ready — simulation starting", state="complete")
    else:
        seed_if_empty()
    page = sidebar()
    topbar()
    router = {
        PAGES[0]: page_overview, PAGES[1]: page_import, PAGES[2]: page_recon, PAGES[3]: page_exceptions,
        PAGES[4]: page_records, PAGES[5]: page_reports, PAGES[6]: page_governance, PAGES[7]: page_studio,
    }
    try:
        router[page]()
    except HubError as e:
        st.error(str(e))
    except Exception as e:
        st.error(f"Unexpected error (existing records are safe — all writes are transactional): {e}")
        with st.expander("Diagnostics"):
            import traceback
            st.code(traceback.format_exc())
    st.markdown("<hr><div class='small'>◆ LedgerSentinel · synthetic demonstration data · decision support only — "
                "all financial outputs are explainable, traceable and require authorized human approval · "
                "no compliance certification claimed · open-source-first, offline, no API keys.</div>",
                unsafe_allow_html=True)

if __name__ == "__main__":
    main()