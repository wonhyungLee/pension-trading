from __future__ import annotations

import asyncio
import csv
import json
import math
import os
import secrets
import sqlite3
import subprocess
import traceback
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import dotenv_values
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import HTMLResponse, ORJSONResponse, FileResponse
from pydantic import BaseModel

from exchange import get_bot
from exchange.model.schemas import find_env_file

KST = ZoneInfo("Asia/Seoul")
BASE_DIR = Path("/home/ubuntu/연금계좌자동매매")
PROJECT_DIR = BASE_DIR / "project"
DATA_DIR = BASE_DIR / "etf데이터"
SELECTED_DB_PATH = DATA_DIR / "etf_daily_close_selected_universe.db"
OUT_DIR = BASE_DIR / "out"
WEIGHTS_CSV_PATH = OUT_DIR / "best_target_weights_long.csv"
BEST_CONFIG_PATH = OUT_DIR / "best_config.json"
SITE2_DB_PATH = BASE_DIR / "site2.db"
SITE2_FRONTEND_DIST_DIR = BASE_DIR / "site2" / "frontend" / "dist"
SITE2_FRONTEND_ASSETS_DIR = SITE2_FRONTEND_DIST_DIR / "assets"

SESSION_TTL_HOURS = 12
ACTIVE_TOKENS: Dict[str, datetime] = {}

TARGET_ACCOUNTS = [5, 8]

SITE2_ROUTER = APIRouter(prefix="/site2", tags=["site2"])
_SERVICE_TASK: asyncio.Task | None = None
_SERVICE_LOCK = asyncio.Lock()


def _dotenv_map() -> Dict[str, str]:
    env_path = find_env_file()
    if not env_path:
        return {}
    values = dotenv_values(env_path)
    return {k: str(v) for k, v in values.items() if v is not None}


def _env_raw(name: str) -> str | None:
    # Prefer .env values so SITE2 비중 변경이 즉시 반영되고,
    # 이전 프로세스 환경값(os.environ)에 덮여쓰이지 않도록 한다.
    value = _dotenv_map().get(name)
    if value is not None:
        return value
    return os.getenv(name)


def _env_bool(name: str, default: bool) -> bool:
    raw = _env_raw(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_sync_interval_sec() -> int:
    raw = _env_raw("SITE2_SYNC_INTERVAL_SEC")
    return max(60, int(raw or "1800"))


def _get_trade_trigger() -> Tuple[int, int]:
    hour = int(_env_raw("SITE2_TRADE_HOUR") or "9")
    minute = int(_env_raw("SITE2_TRADE_MINUTE") or "0")
    return max(0, min(hour, 23)), max(0, min(minute, 59))


def _iso_now() -> str:
    return datetime.now(tz=KST).isoformat()


def _today_kst() -> date:
    return datetime.now(tz=KST).date()


def _normalize_code(value: Any) -> str:
    code = str(value).strip()
    if code.isdigit():
        return code.zfill(6)
    return code


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _get_sleeve_weight_targets() -> Tuple[float, float]:
    bond_weight = _safe_float(_env_raw("SITE2_BOND_WEIGHT"), 0.25)
    bond_weight = min(max(bond_weight, 0.0), 1.0)

    risk_weight = _safe_float(_env_raw("SITE2_RISK_WEIGHT"), 0.70)
    risk_weight = min(max(risk_weight, 0.0), 1.0)

    total = risk_weight + bond_weight
    if total <= 0:
        return 0.70, 0.25
    if total > 1.0:
        return risk_weight / total, bond_weight / total
    return risk_weight, bond_weight


def _apply_sleeve_targets(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "sleeve" not in df.columns:
        return df

    risk_target, bond_target = _get_sleeve_weight_targets()
    out = df.copy()
    out["sleeve"] = out["sleeve"].astype(str)

    for _, day_rows in out.groupby("date", sort=False):
        day_idx = day_rows.index
        sleeve_norm = day_rows["sleeve"].str.upper()

        risk_idx = day_rows[sleeve_norm == "RISK"].index
        bond_idx = day_rows[sleeve_norm == "BOND"].index
        other_idx = day_rows[(sleeve_norm != "RISK") & (sleeve_norm != "BOND")].index

        if len(risk_idx) > 0 and len(bond_idx) > 0:
            risk_alloc = risk_target
            bond_alloc = bond_target
        elif len(risk_idx) > 0:
            risk_alloc = 1.0
            bond_alloc = 0.0
        elif len(bond_idx) > 0:
            risk_alloc = 0.0
            bond_alloc = 1.0
        else:
            total = float(out.loc[day_idx, "weight"].sum())
            if total > 0:
                out.loc[day_idx, "weight"] = out.loc[day_idx, "weight"] / total
            continue

        risk_sum = float(out.loc[risk_idx, "weight"].sum()) if len(risk_idx) > 0 else 0.0
        if risk_sum > 0:
            out.loc[risk_idx, "weight"] = out.loc[risk_idx, "weight"] * (risk_alloc / risk_sum)
        elif len(risk_idx) > 0:
            out.loc[risk_idx, "weight"] = risk_alloc / len(risk_idx)

        bond_sum = float(out.loc[bond_idx, "weight"].sum()) if len(bond_idx) > 0 else 0.0
        if bond_sum > 0:
            out.loc[bond_idx, "weight"] = out.loc[bond_idx, "weight"] * (bond_alloc / bond_sum)
        elif len(bond_idx) > 0:
            out.loc[bond_idx, "weight"] = bond_alloc / len(bond_idx)

        if len(other_idx) > 0:
            out.loc[other_idx, "weight"] = 0.0

    return out


def _to_plain_dict(item: Any) -> Dict[str, Any]:
    if item is None:
        return {}
    if isinstance(item, dict):
        return item
    if is_dataclass(item):
        return asdict(item)
    if hasattr(item, "dict"):
        try:
            return item.dict()
        except Exception:
            pass
    if hasattr(item, "__dict__"):
        return dict(item.__dict__)
    return {}


def _db_connect() -> sqlite3.Connection:
    con = sqlite3.connect(SITE2_DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def _init_site2_db() -> None:
    SITE2_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _db_connect() as con:
        cur = con.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS portfolio_snapshot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account INTEGER NOT NULL,
                captured_at TEXT NOT NULL,
                total_krw REAL NOT NULL,
                cash_krw REAL NOT NULL,
                exchange_rate REAL,
                holdings_json TEXT NOT NULL,
                source TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_portfolio_snapshot_account_captured
            ON portfolio_snapshot(account, captured_at DESC);

            CREATE TABLE IF NOT EXISTS trade_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account INTEGER NOT NULL,
                requested_at TEXT NOT NULL,
                executed_at TEXT,
                code TEXT NOT NULL,
                side TEXT NOT NULL,
                qty INTEGER NOT NULL,
                price REAL,
                status TEXT NOT NULL,
                strategy_date TEXT,
                target_weight REAL,
                current_qty INTEGER,
                target_qty INTEGER,
                message TEXT,
                response_json TEXT,
                source TEXT DEFAULT 'ENGINE',
                external_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_trade_history_requested
            ON trade_history(requested_at DESC);

            CREATE TABLE IF NOT EXISTS refresh_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                level TEXT NOT NULL,
                event TEXT NOT NULL,
                message TEXT NOT NULL,
                details_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_refresh_log_created
            ON refresh_log(created_at DESC);

            CREATE TABLE IF NOT EXISTS live_quote (
                code TEXT PRIMARY KEY,
                price REAL NOT NULL,
                updated_at TEXT NOT NULL,
                source TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runtime_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )

        cols = {row["name"] for row in cur.execute("PRAGMA table_info(trade_history)").fetchall()}
        if "source" not in cols:
            cur.execute("ALTER TABLE trade_history ADD COLUMN source TEXT DEFAULT 'ENGINE'")
        if "external_id" not in cols:
            cur.execute("ALTER TABLE trade_history ADD COLUMN external_id TEXT")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_history_source_external ON trade_history(source, external_id)")


def _write_log(level: str, event: str, message: str, details: Dict[str, Any] | None = None) -> None:
    payload = json.dumps(details or {}, ensure_ascii=False)
    with _db_connect() as con:
        con.execute(
            """
            INSERT INTO refresh_log(created_at, level, event, message, details_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (_iso_now(), level, event, message, payload),
        )
    print(f"[site2][{level}] {event}: {message}")


def _set_runtime_state(key: str, value: str) -> None:
    with _db_connect() as con:
        con.execute(
            """
            INSERT INTO runtime_state(key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            (key, value, _iso_now()),
        )


def _get_runtime_state(key: str, default: str = "") -> str:
    with _db_connect() as con:
        row = con.execute("SELECT value FROM runtime_state WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


class _LoginPayload(BaseModel):
    username: str
    password: str


def _load_login_settings() -> Dict[str, Any]:
    required_raw = _get_runtime_state("site2_login_required", "false").strip().lower()
    return {
        "required": required_raw in {"1", "true", "yes", "y", "on"},
        "username": _get_runtime_state("site2_login_id", ""),
        "password": _get_runtime_state("site2_login_password", ""),
    }


def _purge_expired_tokens() -> None:
    now = datetime.now(tz=KST)
    for token, expires_at in list(ACTIVE_TOKENS.items()):
        if expires_at <= now:
            ACTIVE_TOKENS.pop(token, None)


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    prefix = "Bearer "
    if not authorization.startswith(prefix):
        return None
    token = authorization[len(prefix) :].strip()
    return token or None


def _is_token_valid(token: str) -> bool:
    _purge_expired_tokens()
    expires_at = ACTIVE_TOKENS.get(token)
    return bool(expires_at and expires_at > datetime.now(tz=KST))


def _require_site2_auth(authorization: str | None = Header(default=None)) -> None:
    settings = _load_login_settings()
    if not settings["required"]:
        return
    token = _extract_bearer_token(authorization)
    if not token or not _is_token_valid(token):
        raise HTTPException(status_code=401, detail="인증이 필요합니다.")


def _parse_holdings_json(holdings_json: str | None) -> List[Dict[str, Any]]:
    if not holdings_json:
        return []
    try:
        data = json.loads(holdings_json)
    except Exception:
        return []
    if not isinstance(data, list):
        return []

    holdings: List[Dict[str, Any]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        holdings.append(
            {
                "code": _normalize_code(row.get("code", "")),
                "name": str(row.get("name", "")),
                "qty": _safe_int(row.get("qty", 0)),
                "current_price": _safe_float(row.get("current_price", 0.0)),
                "eval_amount": _safe_float(row.get("eval_amount", 0.0)),
                "weight": _safe_float(row.get("weight", 0.0)),
            }
        )
    holdings.sort(key=lambda x: x["eval_amount"], reverse=True)
    return holdings


def _selected_universe_columns() -> set[str]:
    with sqlite3.connect(SELECTED_DB_PATH) as con:
        con.row_factory = sqlite3.Row
        cols = {row["name"] for row in con.execute("PRAGMA table_info(selected_universe)").fetchall()}
    return cols


def _load_universe_map() -> Dict[str, Dict[str, Any]]:
    cols = _selected_universe_columns()
    days_col = "days" if "days" in cols else "days_in_3y" if "days_in_3y" in cols else None
    days_sql = days_col if days_col else "NULL"
    with sqlite3.connect(SELECTED_DB_PATH) as con:
        con.row_factory = sqlite3.Row
        rows = con.execute(
            f"SELECT code, name, sleeve, total_return_3y, {days_sql} AS days FROM selected_universe"
        ).fetchall()
    limit_map = {"RISK": 70, "BOND": 100}
    result: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        code = _normalize_code(row["code"])
        sleeve = str(row["sleeve"] or "").upper()
        result[code] = {
            "name": row["name"],
            "sleeve": sleeve,
            "allocation_limit_pct": limit_map.get(sleeve, 70),
            "total_return_3y": _safe_float(row["total_return_3y"], 0.0),
            "days": _safe_int(row["days"], 0),
        }
    return result


def _latest_holding_codes() -> set[str]:
    with _db_connect() as con:
        rows = con.execute(
            """
            SELECT ps.holdings_json
            FROM portfolio_snapshot ps
            JOIN (
                SELECT account, MAX(captured_at) AS max_captured_at
                FROM portfolio_snapshot
                GROUP BY account
            ) latest
              ON latest.account = ps.account
             AND latest.max_captured_at = ps.captured_at
            """
        ).fetchall()
    codes: set[str] = set()
    for row in rows:
        for item in _parse_holdings_json(row["holdings_json"]):
            code = _normalize_code(item.get("code", ""))
            if code:
                codes.add(code)
    return codes

def _ensure_strategy_outputs() -> None:
    if not SELECTED_DB_PATH.exists():
        raise FileNotFoundError(f"selected universe DB not found: {SELECTED_DB_PATH}")

    if WEIGHTS_CSV_PATH.exists() and BEST_CONFIG_PATH.exists():
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    cmd = [
        "python3",
        str(PROJECT_DIR / "backtest_dual_sleeve_grid.py"),
        "--db_path",
        str(SELECTED_DB_PATH),
        "--out_dir",
        str(OUT_DIR),
    ]
    subprocess.run(cmd, check=True)


def _load_weights_df() -> pd.DataFrame:
    _ensure_strategy_outputs()
    df = pd.read_csv(WEIGHTS_CSV_PATH, dtype={"code": str})
    if df.empty:
        return df
    df["code"] = df["code"].map(_normalize_code)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["weight"] = df["weight"].astype(float)
    if "sleeve" not in df.columns:
        df["sleeve"] = "UNKNOWN"
    return _apply_sleeve_targets(df)


def _load_best_config() -> Dict[str, Any]:
    _ensure_strategy_outputs()
    if not BEST_CONFIG_PATH.exists():
        return {}
    return json.loads(BEST_CONFIG_PATH.read_text(encoding="utf-8"))


def _load_strategy_selection_rows_for_day(day: date) -> Tuple[str | None, List[Dict[str, Any]]]:
    _ensure_strategy_outputs()
    if not WEIGHTS_CSV_PATH.exists():
        return None, []

    target_day = day.isoformat()
    rows: List[Dict[str, Any]] = []
    date_values: set[str] = set()

    with WEIGHTS_CSV_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_date = str(row.get("date") or "").strip()
            if not raw_date:
                continue
            day_str = raw_date[:10]
            date_values.add(day_str)
            rows.append(
                {
                    "date": day_str,
                    "code": _normalize_code(row.get("code", "")),
                    "sleeve": str(row.get("sleeve", "") or "UNKNOWN").upper(),
                    "weight": _safe_float(row.get("weight", 0.0), 0.0),
                }
            )

    if not rows:
        return None, []

    le_dates = [d for d in date_values if d <= target_day]
    if le_dates:
        use_day = max(le_dates)
    else:
        use_day = min(date_values)

    selected = [r for r in rows if r["date"] == use_day]

    # Apply live sleeve targets (e.g., 70:25) so dashboard selection matches execution logic.
    risk_target, bond_target = _get_sleeve_weight_targets()
    risk_rows = [r for r in selected if r["sleeve"] == "RISK"]
    bond_rows = [r for r in selected if r["sleeve"] == "BOND"]
    other_rows = [r for r in selected if r["sleeve"] not in {"RISK", "BOND"}]

    if risk_rows and bond_rows:
        risk_alloc = risk_target
        bond_alloc = bond_target
    elif risk_rows:
        risk_alloc = 1.0
        bond_alloc = 0.0
    elif bond_rows:
        risk_alloc = 0.0
        bond_alloc = 1.0
    else:
        risk_alloc = 0.0
        bond_alloc = 0.0

    risk_sum = sum(r["weight"] for r in risk_rows)
    if risk_rows:
        if risk_sum > 0:
            for row in risk_rows:
                row["weight"] = row["weight"] * (risk_alloc / risk_sum)
        else:
            even = risk_alloc / len(risk_rows)
            for row in risk_rows:
                row["weight"] = even

    bond_sum = sum(r["weight"] for r in bond_rows)
    if bond_rows:
        if bond_sum > 0:
            for row in bond_rows:
                row["weight"] = row["weight"] * (bond_alloc / bond_sum)
        else:
            even = bond_alloc / len(bond_rows)
            for row in bond_rows:
                row["weight"] = even

    for row in other_rows:
        row["weight"] = 0.0

    selected.sort(key=lambda x: x["weight"], reverse=True)
    return use_day, selected


def _selected_codes() -> List[str]:
    with sqlite3.connect(SELECTED_DB_PATH) as con:
        rows = con.execute("SELECT DISTINCT code FROM selected_universe ORDER BY code").fetchall()
    return [_normalize_code(row[0]) for row in rows]


def _target_weights_for_day(df: pd.DataFrame, day: date) -> Tuple[date, List[Dict[str, Any]], Dict[str, float]]:
    if df.empty:
        raise ValueError("target weights are empty")
    candidate = df[df["date"] <= day]
    if candidate.empty:
        use_day = df["date"].min()
    else:
        use_day = candidate["date"].max()
    rows = df[df["date"] == use_day].sort_values(["sleeve", "weight"], ascending=[True, False])

    items = []
    weight_map: Dict[str, float] = {}
    for _, row in rows.iterrows():
        code = _normalize_code(row["code"])
        weight = float(row["weight"])
        item = {
            "date": use_day.isoformat(),
            "code": code,
            "sleeve": str(row.get("sleeve", "UNKNOWN")),
            "weight": weight,
        }
        items.append(item)
        weight_map[code] = weight_map.get(code, 0.0) + weight

    return use_day, items, weight_map


def _future_plan(df: pd.DataFrame, config: Dict[str, Any], horizon: int = 5) -> List[Dict[str, Any]]:
    if df.empty:
        return []

    today = _today_kst()
    plans: List[Dict[str, Any]] = []

    upcoming_days = sorted({d for d in df["date"].tolist() if d >= today})
    for d in upcoming_days[:horizon]:
        g = df[df["date"] == d].sort_values("weight", ascending=False)
        plans.append(
            {
                "date": d.isoformat(),
                "projected": False,
                "items": [
                    {
                        "code": _normalize_code(row["code"]),
                        "sleeve": str(row.get("sleeve", "UNKNOWN")),
                        "weight": float(row["weight"]),
                    }
                    for _, row in g.iterrows()
                ],
            }
        )

    if plans:
        return plans

    last_day = max(df["date"])
    latest = df[df["date"] == last_day].copy()
    risk_rb = int(config.get("risk_strategy", {}).get("rebalance_n") or 21)
    bond_rb = int(config.get("bond_strategy", {}).get("rebalance_n") or 21)
    rebalance_n = max(1, max(risk_rb, bond_rb))

    for idx in range(1, horizon + 1):
        projected_day = (pd.Timestamp(last_day) + pd.offsets.BDay(rebalance_n * idx)).date()
        plans.append(
            {
                "date": projected_day.isoformat(),
                "projected": True,
                "items": [
                    {
                        "code": _normalize_code(row["code"]),
                        "sleeve": str(row.get("sleeve", "UNKNOWN")),
                        "weight": float(row["weight"]),
                    }
                    for _, row in latest.sort_values("weight", ascending=False).iterrows()
                ],
            }
        )

    return plans


def _fetch_domestic_price(bot: Any, code: str) -> float:
    tr_id = "FHKST01010100"
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": code,
    }
    headers = bot._get_headers(tr_id)
    data = bot._api_call_with_retry(
        "GET",
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        headers=headers,
        params=params,
    )
    output = data.get("output", {})
    price = _safe_float(output.get("stck_prpr"), 0.0)
    return price


def _sync_quotes_into_selected_db(quotes: Dict[str, float]) -> Dict[str, Any]:
    today_key = datetime.now(tz=KST).strftime("%Y%m%d")
    rows = [(code, today_key, int(round(price))) for code, price in quotes.items() if price > 0]
    with sqlite3.connect(SELECTED_DB_PATH) as con:
        cur = con.cursor()
        cur.execute("DELETE FROM etf_daily_close WHERE trade_date = ?", (today_key,))
        cur.executemany(
            "INSERT INTO etf_daily_close(code, trade_date, close_price) VALUES (?, ?, ?)",
            rows,
        )
        con.commit()
    return {"trade_date": today_key, "inserted": len(rows)}


def _sync_quotes_into_site2_db(quotes: Dict[str, float], source: str) -> None:
    now = _iso_now()
    rows = [(_normalize_code(code), float(price), now, source) for code, price in quotes.items() if price > 0]
    with _db_connect() as con:
        con.executemany(
            """
            INSERT INTO live_quote(code, price, updated_at, source)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                price = excluded.price,
                updated_at = excluded.updated_at,
                source = excluded.source
            """,
            rows,
        )


def _refresh_market_data_sync(reason: str) -> Dict[str, Any]:
    codes = _selected_codes()
    bots = {
        5: get_bot("KRX", 5),
        8: get_bot("KRX", 8),
    }

    quotes: Dict[str, float] = {}
    errors = []
    used_by = {5: 0, 8: 0}

    for idx, code in enumerate(codes):
        # Alternate between KIS5 and KIS8, and fallback to the other account on failure.
        primary = 5 if idx % 2 == 0 else 8
        order = [primary, 8 if primary == 5 else 5]
        price = 0.0
        last_error = None

        for account in order:
            try:
                price = _fetch_domestic_price(bots[account], code)
                if price > 0:
                    used_by[account] += 1
                    break
            except Exception as exc:
                last_error = str(exc)

        if price > 0:
            quotes[code] = price
        elif len(errors) < 20:
            errors.append({"code": code, "error": last_error or "price unavailable"})

    if not quotes:
        raise RuntimeError("failed to fetch any quote from KIS")

    selected_db_sync = _sync_quotes_into_selected_db(quotes)
    _sync_quotes_into_site2_db(quotes, "kis_rest")

    details = {
        "reason": reason,
        "requested": len(codes),
        "synced": len(quotes),
        "errors": len(errors),
        "used_by": used_by,
        "selected_db": selected_db_sync,
    }
    if errors:
        details["error_samples"] = errors

    _write_log("INFO", "market_sync", "KIS REST quotes synced", details)
    return details


def _live_quotes_map() -> Dict[str, float]:
    with _db_connect() as con:
        rows = con.execute("SELECT code, price FROM live_quote").fetchall()
    return {_normalize_code(r["code"]): float(r["price"]) for r in rows}


def _capture_snapshot_for_account(account: int, source: str) -> Dict[str, Any]:
    bot = get_bot("KRX", account)
    domestic = bot.get_domestic_balance() or {}
    stocks_raw = domestic.get("stocks", [])

    holdings: List[Dict[str, Any]] = []
    eval_sum = 0.0

    for stock in stocks_raw:
        data = _to_plain_dict(stock)
        code = _normalize_code(data.get("symbol", ""))
        qty = _safe_int(data.get("quantity", 0))
        current_price = _safe_float(data.get("current_price", 0.0))
        eval_amount = _safe_float(data.get("eval_amount", current_price * qty))
        if qty <= 0:
            continue

        holdings.append(
            {
                "code": code,
                "name": str(data.get("name", "")),
                "qty": qty,
                "current_price": current_price,
                "eval_amount": eval_amount,
            }
        )
        eval_sum += eval_amount

    total_krw = _safe_float(domestic.get("total_krw", 0.0))
    if total_krw <= 0:
        total_krw = eval_sum

    cash_krw = max(0.0, total_krw - eval_sum)

    for item in holdings:
        item["weight"] = (item["eval_amount"] / total_krw) if total_krw > 0 else 0.0

    payload = {
        "account": account,
        "captured_at": _iso_now(),
        "total_krw": total_krw,
        "cash_krw": cash_krw,
        "exchange_rate": 0.0,
        "holdings": sorted(holdings, key=lambda x: x["eval_amount"], reverse=True),
        "source": source,
    }

    with _db_connect() as con:
        con.execute(
            """
            INSERT INTO portfolio_snapshot(
                account, captured_at, total_krw, cash_krw, exchange_rate, holdings_json, source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["account"],
                payload["captured_at"],
                payload["total_krw"],
                payload["cash_krw"],
                payload["exchange_rate"],
                json.dumps(payload["holdings"], ensure_ascii=False),
                payload["source"],
            ),
        )

    return payload


def _capture_all_snapshots_sync(reason: str) -> List[Dict[str, Any]]:
    snapshots = []
    for account in TARGET_ACCOUNTS:
        try:
            snap = _capture_snapshot_for_account(account, source=reason)
            snapshots.append(snap)
        except Exception as exc:
            _write_log(
                "ERROR",
                "portfolio_snapshot",
                f"failed to capture account {account}",
                {"account": account, "error": str(exc), "trace": traceback.format_exc()},
            )

    _write_log(
        "INFO",
        "portfolio_snapshot",
        "portfolio snapshots captured",
        {"reason": reason, "captured_accounts": [s["account"] for s in snapshots]},
    )
    return snapshots


def _build_order_preview(
    snapshot: Dict[str, Any],
    target_weights: Dict[str, float],
    quote_map: Dict[str, float],
) -> List[Dict[str, Any]]:
    total_krw = _safe_float(snapshot.get("total_krw", 0.0))
    if total_krw <= 0:
        return []

    holdings = snapshot.get("holdings", []) or []
    positions = {_normalize_code(item.get("code", "")): _safe_int(item.get("qty", 0)) for item in holdings}
    holding_price = {_normalize_code(item.get("code", "")): _safe_float(item.get("current_price", 0.0)) for item in holdings}

    codes = sorted(set(list(target_weights.keys()) + list(positions.keys())))
    previews: List[Dict[str, Any]] = []

    for code in codes:
        target_weight = float(target_weights.get(code, 0.0))
        price = _safe_float(quote_map.get(code, holding_price.get(code, 0.0)))
        if price <= 0:
            continue
        current_qty = int(positions.get(code, 0))
        target_qty = int(math.floor((total_krw * target_weight) / price))
        diff = target_qty - current_qty
        if diff == 0:
            continue

        previews.append(
            {
                "code": code,
                "side": "SELL" if diff < 0 else "BUY",
                "qty": abs(diff),
                "price": price,
                "target_weight": target_weight,
                "current_qty": current_qty,
                "target_qty": target_qty,
            }
        )

    sells = [row for row in previews if row["side"] == "SELL"]
    buys = [row for row in previews if row["side"] == "BUY"]
    return sells + buys


def _record_trade_history(
    account: int,
    requested_at: str,
    preview: Dict[str, Any],
    status: str,
    message: str,
    response: Dict[str, Any] | None,
    strategy_date: str,
    source: str = "ENGINE",
    external_id: str | None = None,
) -> None:
    with _db_connect() as con:
        con.execute(
            """
            INSERT INTO trade_history(
                account, requested_at, executed_at, code, side, qty, price, status,
                strategy_date, target_weight, current_qty, target_qty, message, response_json,
                source, external_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account,
                requested_at,
                _iso_now(),
                preview["code"],
                preview["side"],
                int(preview["qty"]),
                float(preview.get("price", 0.0)),
                status,
                strategy_date,
                float(preview.get("target_weight", 0.0)),
                int(preview.get("current_qty", 0)),
                int(preview.get("target_qty", 0)),
                message,
                json.dumps(response or {}, ensure_ascii=False),
                source,
                external_id,
            ),
        )


def _pick_first(data: Dict[str, Any], keys: List[str]) -> Any:
    for key in keys:
        if key in data and data[key] not in ("", None):
            return data[key]
    return None


def _parse_kis_requested_at(row: Dict[str, Any]) -> Tuple[str, str]:
    date_raw = str(_pick_first(row, ["ord_dt", "trad_dt", "ord_date"]) or "")
    time_raw = str(_pick_first(row, ["ord_tmd", "ord_tm", "trad_tmd", "ord_time"]) or "")

    date_digits = "".join(ch for ch in date_raw if ch.isdigit())[:8]
    time_digits = "".join(ch for ch in time_raw if ch.isdigit())[:6].ljust(6, "0")

    if len(date_digits) == 8:
        try:
            dt = datetime.strptime(f"{date_digits}{time_digits}", "%Y%m%d%H%M%S").replace(tzinfo=KST)
            return dt.isoformat(), dt.date().isoformat()
        except Exception:
            pass
    now = _iso_now()
    return now, _today_kst().isoformat()


def _parse_kis_side(row: Dict[str, Any]) -> str:
    side_text = str(
        _pick_first(row, ["sll_buy_dvsn_cd_name", "sll_buy_dvsn_cd", "trad_dvsn_name", "trad_dvsn"])
        or ""
    ).upper()
    if "매도" in side_text or side_text in {"01", "1", "SELL"}:
        return "SELL"
    if "매수" in side_text or side_text in {"02", "2", "BUY"}:
        return "BUY"
    return "UNKNOWN"


def _sync_kis_trade_history_sync(days: int = 180) -> Dict[str, Any]:
    end = datetime.now(tz=KST).strftime("%Y%m%d")
    start = (datetime.now(tz=KST) - pd.Timedelta(days=days)).strftime("%Y%m%d")

    details: Dict[str, Any] = {"start": start, "end": end, "accounts": {}, "inserted_total": 0}

    with _db_connect() as con:
        for account in TARGET_ACCOUNTS:
            account_detail = {"fetched": 0, "inserted": 0, "errors": 0}
            try:
                bot = get_bot("KRX", account)
                tr_id = "VTTC8001R" if "openapivts" in str(getattr(bot, "base_url", "")) else "TTTC8001R"
                headers = bot._get_headers(tr_id)
                params = {
                    "CANO": bot.account_number,
                    "ACNT_PRDT_CD": bot.account_code,
                    "INQR_STRT_DT": start,
                    "INQR_END_DT": end,
                    "SLL_BUY_DVSN_CD": "00",
                    "INQR_DVSN": "00",
                    "PDNO": "",
                    "CCLD_DVSN": "00",
                    "ORD_GNO_BRNO": "",
                    "ODNO": "",
                    "INQR_DVSN_3": "01",
                    "INQR_DVSN_1": "",
                    "CTX_AREA_FK100": "",
                    "CTX_AREA_NK100": "",
                }
                response = bot._api_call_with_retry(
                    "GET",
                    "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
                    headers=headers,
                    params=params,
                )
                rows = response.get("output1", []) or []
                account_detail["fetched"] = len(rows)

                for row in rows:
                    row_data = _to_plain_dict(row)
                    code = _normalize_code(
                        _pick_first(row_data, ["pdno", "mksc_shrn_iscd", "iscd", "item_no"]) or ""
                    )
                    qty = _safe_int(
                        _pick_first(row_data, ["tot_ccld_qty", "tot_ord_qty", "ord_qty", "ccld_qty", "exec_qty"]),
                        0,
                    )
                    if not code or qty <= 0:
                        continue

                    side = _parse_kis_side(row_data)
                    requested_at, strategy_date = _parse_kis_requested_at(row_data)
                    price = _safe_float(
                        _pick_first(
                            row_data,
                            ["avg_prvs", "avg_ccld_unpr", "ord_unpr", "avg_cntr_unpr", "ccld_unpr3"],
                        ),
                        0.0,
                    )
                    order_no = str(_pick_first(row_data, ["odno", "ord_no", "ord_gno_brno", "odno2"]) or "")
                    external_id = f"{account}:{strategy_date}:{order_no}:{code}:{side}:{qty}:{price:.4f}"

                    exists = con.execute(
                        "SELECT 1 FROM trade_history WHERE source = ? AND external_id = ? LIMIT 1",
                        ("KIS_REST", external_id),
                    ).fetchone()
                    if exists:
                        continue

                    con.execute(
                        """
                        INSERT INTO trade_history(
                            account, requested_at, executed_at, code, side, qty, price, status,
                            strategy_date, target_weight, current_qty, target_qty, message, response_json,
                            source, external_id
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            account,
                            requested_at,
                            requested_at,
                            code,
                            side,
                            qty,
                            price,
                            "KIS_HISTORY",
                            strategy_date,
                            None,
                            None,
                            None,
                            "imported from KIS REST",
                            json.dumps(row_data, ensure_ascii=False),
                            "KIS_REST",
                            external_id,
                        ),
                    )
                    account_detail["inserted"] += 1
                    details["inserted_total"] += 1

            except Exception as exc:
                account_detail["errors"] += 1
                account_detail["error_message"] = str(exc)

            details["accounts"][str(account)] = account_detail

    _write_log("INFO", "trade_history_sync", "KIS order history synced", details)
    return details


def _is_buying_power_error(message: str) -> bool:
    text = str(message or "")
    lowered = text.lower()
    kr_keywords = [
        "주문가능금액",
        "가능금액을 초과",
        "주문 가능 금액",
        "주문가능수량",
        "가능수량",
    ]
    en_keywords = [
        "insufficient",
        "buying power",
        "orderable amount",
    ]
    return any(k in text for k in kr_keywords) or any(k in lowered for k in en_keywords)


def _buy_qty_candidates(original_qty: int) -> List[int]:
    if original_qty <= 1:
        return [max(1, original_qty)]
    ratios = [1.0, 0.95, 0.9, 0.85, 0.8, 0.75, 0.7, 0.65, 0.6, 0.55, 0.5, 0.45, 0.4, 0.35, 0.3, 0.25, 0.2, 0.15, 0.1]
    qtys: List[int] = []
    for ratio in ratios:
        qty = max(1, int(math.floor(original_qty * ratio)))
        if qty not in qtys:
            qtys.append(qty)
    return qtys


def _submit_order_with_fallback(bot: Any, preview: Dict[str, Any]) -> Dict[str, Any]:
    side = str(preview.get("side", "")).upper()
    code = _normalize_code(preview.get("code", ""))
    original_qty = max(1, int(preview.get("qty", 1)))
    attempts: List[Dict[str, Any]] = []

    if side != "BUY":
        try:
            response = bot.create_order("KRX", code, "market", side.lower(), original_qty)
            if isinstance(response, dict) and response.get("rt_cd") not in (None, "0"):
                message = str(response.get("msg1", "order rejected"))
                return {"status": "ERROR", "message": message, "response": response, "used_qty": original_qty, "attempts": [{"qty": original_qty, "message": message}]}
            return {"status": "SUCCESS", "message": "order submitted", "response": response if isinstance(response, dict) else {"raw": str(response)}, "used_qty": original_qty, "attempts": [{"qty": original_qty, "message": "ok"}]}
        except Exception as exc:
            message = str(exc)
            return {"status": "ERROR", "message": message, "response": {"error": message}, "used_qty": original_qty, "attempts": [{"qty": original_qty, "message": message}]}

    last_error_message = "order rejected"
    last_error_response: Dict[str, Any] = {"error": last_error_message}
    last_qty = original_qty

    for qty in _buy_qty_candidates(original_qty):
        last_qty = qty
        try:
            response = bot.create_order("KRX", code, "market", "buy", qty)
            if isinstance(response, dict) and response.get("rt_cd") not in (None, "0"):
                message = str(response.get("msg1", "order rejected"))
                attempts.append({"qty": qty, "message": message})
                last_error_message = message
                last_error_response = response
                if _is_buying_power_error(message) and qty > 1:
                    continue
                return {"status": "ERROR", "message": message, "response": response, "used_qty": qty, "attempts": attempts}

            if qty < original_qty:
                message = f"order submitted (qty adjusted {original_qty}->{qty})"
            else:
                message = "order submitted"
            attempts.append({"qty": qty, "message": message})
            return {"status": "SUCCESS", "message": message, "response": response if isinstance(response, dict) else {"raw": str(response)}, "used_qty": qty, "attempts": attempts}
        except Exception as exc:
            message = str(exc)
            attempts.append({"qty": qty, "message": message})
            last_error_message = message
            last_error_response = {"error": message}
            if _is_buying_power_error(message) and qty > 1:
                continue
            return {"status": "ERROR", "message": message, "response": {"error": message}, "used_qty": qty, "attempts": attempts}

    return {
        "status": "ERROR",
        "message": f"{last_error_message} (qty fallback exhausted)",
        "response": last_error_response,
        "used_qty": last_qty,
        "attempts": attempts,
    }


def _execute_trade_for_account_sync(account: int, reason: str, force: bool = False) -> Dict[str, Any]:
    weights_df = _load_weights_df()
    strategy_date, _, weight_map = _target_weights_for_day(weights_df, _today_kst())

    snapshot = _capture_snapshot_for_account(account, source=f"pre_trade:{reason}")
    quotes = _live_quotes_map()
    previews = _build_order_preview(snapshot, weight_map, quotes)

    if not previews:
        _write_log(
            "INFO",
            "trade_execute",
            f"account {account}: no rebalance order needed",
            {"account": account, "reason": reason, "strategy_date": strategy_date.isoformat()},
        )
        _set_runtime_state(f"last_trade_{account}", _today_kst().isoformat())
        return {
            "account": account,
            "strategy_date": strategy_date.isoformat(),
            "orders": [],
            "executed": 0,
            "simulated": 0,
        }

    live_order = _env_bool("SITE2_LIVE_ORDER", False)
    requested_at = _iso_now()
    bot = get_bot("KRX", account)

    executed_count = 0
    simulated_count = 0
    errors = 0

    for preview in previews:
        if not live_order and not force:
            simulated_count += 1
            _record_trade_history(
                account=account,
                requested_at=requested_at,
                preview=preview,
                status="SIMULATED",
                message="SITE2_LIVE_ORDER=false (dry-run)",
                response={"dry_run": True},
                strategy_date=strategy_date.isoformat(),
            )
            continue

        order_result = _submit_order_with_fallback(bot, preview)
        status = str(order_result.get("status", "ERROR"))
        message = str(order_result.get("message", "order rejected"))
        response = order_result.get("response", {})
        used_qty = int(order_result.get("used_qty", int(preview["qty"])))

        if status == "SUCCESS":
            executed_count += 1
        else:
            errors += 1

        record_preview = dict(preview)
        record_preview["qty"] = used_qty

        _record_trade_history(
            account=account,
            requested_at=requested_at,
            preview=record_preview,
            status=status,
            message=message,
            response=response if isinstance(response, dict) else {"raw": str(response)},
            strategy_date=strategy_date.isoformat(),
        )

    _set_runtime_state(f"last_trade_{account}", _today_kst().isoformat())
    _capture_snapshot_for_account(account, source=f"post_trade:{reason}")

    _write_log(
        "INFO",
        "trade_execute",
        f"account {account}: trade cycle completed",
        {
            "account": account,
            "reason": reason,
            "strategy_date": strategy_date.isoformat(),
            "order_count": len(previews),
            "executed": executed_count,
            "simulated": simulated_count,
            "errors": errors,
            "live_order": live_order or force,
        },
    )

    return {
        "account": account,
        "strategy_date": strategy_date.isoformat(),
        "orders": previews,
        "executed": executed_count,
        "simulated": simulated_count,
        "errors": errors,
        "live_order": live_order or force,
    }


def _refresh_all_sync(reason: str) -> Dict[str, Any]:
    _init_site2_db()
    _ensure_strategy_outputs()
    market_sync = _refresh_market_data_sync(reason=reason)
    history_sync_days = max(7, int(os.getenv("SITE2_HISTORY_SYNC_DAYS", "180")))
    trade_history_sync = _sync_kis_trade_history_sync(days=history_sync_days)
    snapshots = _capture_all_snapshots_sync(reason=reason)
    return {
        "market_sync": market_sync,
        "trade_history_sync": trade_history_sync,
        "snapshots": snapshots,
    }


def _is_scheduled_trade_time(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    hour, minute = _get_trade_trigger()
    trigger = time(hour=hour, minute=minute)
    market_open = time(hour=9, minute=0)
    market_close = time(hour=15, minute=30)
    window_start = max(trigger, market_open)
    return window_start <= now.time() <= market_close


def _latest_snapshots() -> Dict[int, Dict[str, Any]]:
    with _db_connect() as con:
        rows = con.execute(
            """
            SELECT p.*
            FROM portfolio_snapshot p
            JOIN (
                SELECT account, MAX(captured_at) AS max_captured
                FROM portfolio_snapshot
                GROUP BY account
            ) x
            ON p.account = x.account AND p.captured_at = x.max_captured
            ORDER BY p.account
            """
        ).fetchall()

    snapshots: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        snapshots[int(row["account"])] = {
            "account": int(row["account"]),
            "captured_at": row["captured_at"],
            "total_krw": float(row["total_krw"]),
            "cash_krw": float(row["cash_krw"]),
            "exchange_rate": float(row["exchange_rate"] or 0.0),
            "holdings": json.loads(row["holdings_json"] or "[]"),
            "source": row["source"],
        }
    return snapshots


def _recent_trades(limit: int = 120) -> List[Dict[str, Any]]:
    with _db_connect() as con:
        rows = con.execute(
            """
            SELECT *
            FROM trade_history
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    result = []
    for row in rows:
        result.append(
            {
                "id": int(row["id"]),
                "account": int(row["account"]),
                "requested_at": row["requested_at"],
                "executed_at": row["executed_at"],
                "code": row["code"],
                "side": row["side"],
                "qty": int(row["qty"]),
                "price": float(row["price"] or 0.0),
                "status": row["status"],
                "strategy_date": row["strategy_date"],
                "target_weight": float(row["target_weight"] or 0.0),
                "message": row["message"],
                "source": row["source"] if "source" in row.keys() else "ENGINE",
            }
        )
    return result


def _recent_logs(limit: int = 120) -> List[Dict[str, Any]]:
    with _db_connect() as con:
        rows = con.execute(
            """
            SELECT *
            FROM refresh_log
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    result = []
    for row in rows:
        result.append(
            {
                "id": int(row["id"]),
                "created_at": row["created_at"],
                "level": row["level"],
                "event": row["event"],
                "message": row["message"],
                "details": json.loads(row["details_json"] or "{}"),
            }
        )
    return result


def _compose_dashboard() -> Dict[str, Any]:
    _init_site2_db()
    weights_df = _load_weights_df()
    config = _load_best_config()
    plan_day, plan_items, plan_map = _target_weights_for_day(weights_df, _today_kst())
    future_plan = _future_plan(weights_df, config)

    snapshots = _latest_snapshots()
    quote_map = _live_quotes_map()

    accounts = []
    for account in TARGET_ACCOUNTS:
        snap = snapshots.get(account)
        if not snap:
            accounts.append(
                {
                    "account": account,
                    "captured_at": None,
                    "total_krw": 0.0,
                    "cash_krw": 0.0,
                    "holdings": [],
                    "plan_date": plan_day.isoformat(),
                    "order_preview": [],
                    "last_trade_date": _get_runtime_state(f"last_trade_{account}", ""),
                }
            )
            continue

        preview = _build_order_preview(snap, plan_map, quote_map)
        accounts.append(
            {
                "account": account,
                "captured_at": snap["captured_at"],
                "total_krw": snap["total_krw"],
                "cash_krw": snap["cash_krw"],
                "exchange_rate": snap["exchange_rate"],
                "holdings": snap["holdings"],
                "plan_date": plan_day.isoformat(),
                "order_preview": preview,
                "last_trade_date": _get_runtime_state(f"last_trade_{account}", ""),
            }
        )

    with _db_connect() as con:
        quote_meta = con.execute(
            "SELECT MAX(updated_at) AS updated_at, COUNT(*) AS count FROM live_quote"
        ).fetchone()

    return {
        "now": _iso_now(),
        "config": config,
        "active_plan_date": plan_day.isoformat(),
        "active_plan": plan_items,
        "future_plan": future_plan,
        "accounts": accounts,
        "recent_trades": _recent_trades(),
        "refresh_logs": _recent_logs(),
        "quote_status": {
            "updated_at": quote_meta["updated_at"] if quote_meta else None,
            "count": int(quote_meta["count"] or 0) if quote_meta else 0,
        },
        "settings": {
            "autotrade_enabled": _env_bool("SITE2_AUTOTRADE_ENABLED", True),
            "live_order_enabled": _env_bool("SITE2_LIVE_ORDER", False),
            "trade_trigger": {
                "hour": _get_trade_trigger()[0],
                "minute": _get_trade_trigger()[1],
            },
        },
    }


async def _refresh_all(reason: str) -> Dict[str, Any]:
    async with _SERVICE_LOCK:
        return await asyncio.to_thread(_refresh_all_sync, reason)


async def _execute_trade_all(reason: str, force: bool = False) -> Dict[str, Any]:
    async with _SERVICE_LOCK:
        result = await asyncio.to_thread(
            lambda: [_execute_trade_for_account_sync(account, reason, force=force) for account in TARGET_ACCOUNTS]
        )
    return {"results": result}


async def _background_loop() -> None:
    while True:
        try:
            await _refresh_all("scheduled")

            if _env_bool("SITE2_AUTOTRADE_ENABLED", True):
                now = datetime.now(tz=KST)
                if _is_scheduled_trade_time(now):
                    for account in TARGET_ACCOUNTS:
                        key = f"last_trade_{account}"
                        if _get_runtime_state(key) != now.date().isoformat():
                            await _execute_trade_all("scheduled", force=False)
                            break

        except Exception as exc:
            _write_log(
                "ERROR",
                "scheduler",
                "background loop error",
                {"error": str(exc), "trace": traceback.format_exc()},
            )

        await asyncio.sleep(_get_sync_interval_sec())


async def site2_startup() -> None:
    global _SERVICE_TASK
    if _SERVICE_TASK is not None and not _SERVICE_TASK.done():
        return

    try:
        _init_site2_db()
        _ensure_strategy_outputs()
        _SERVICE_TASK = asyncio.create_task(_background_loop(), name="site2-background-loop")
        _write_log("INFO", "startup", "site2 service started", {"accounts": TARGET_ACCOUNTS})
    except Exception as exc:
        _write_log(
            "ERROR",
            "startup",
            "site2 service startup failed",
            {"error": str(exc), "trace": traceback.format_exc()},
        )


@SITE2_ROUTER.get("", include_in_schema=False)
@SITE2_ROUTER.get("/", include_in_schema=False)
async def site2_index() -> HTMLResponse:
    index_path = SITE2_FRONTEND_DIST_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    return HTMLResponse(
        "<h3>site2 프론트엔드 빌드가 없습니다.</h3>"
        "<p><code>cd /home/ubuntu/연금계좌자동매매/site2/frontend && npm install && npm run build</code></p>",
        status_code=503,
    )


@SITE2_ROUTER.get("/assets/{asset_path:path}", include_in_schema=False)
async def site2_assets(asset_path: str) -> FileResponse:
    asset_file = (SITE2_FRONTEND_ASSETS_DIR / asset_path).resolve()
    assets_root = SITE2_FRONTEND_ASSETS_DIR.resolve()
    if not str(asset_file).startswith(str(assets_root)) or not asset_file.exists() or not asset_file.is_file():
        raise HTTPException(status_code=404, detail="asset not found")
    return FileResponse(asset_file)


@SITE2_ROUTER.get("/api/dashboard", response_class=ORJSONResponse)
async def site2_dashboard() -> ORJSONResponse:
    data = await asyncio.to_thread(_compose_dashboard)
    return ORJSONResponse(data)


@SITE2_ROUTER.post("/api/refresh-now", response_class=ORJSONResponse)
async def site2_refresh_now() -> ORJSONResponse:
    data = await _refresh_all("manual")
    return ORJSONResponse({"ok": True, "result": data})


@SITE2_ROUTER.post("/api/trade-now", response_class=ORJSONResponse)
async def site2_trade_now(force: bool = False) -> ORJSONResponse:
    data = await _execute_trade_all("manual", force=force)
    return ORJSONResponse({"ok": True, "result": data})


@SITE2_ROUTER.get("/api/health", response_class=ORJSONResponse)
async def site2_health() -> ORJSONResponse:
    task_running = _SERVICE_TASK is not None and not _SERVICE_TASK.done()
    return ORJSONResponse(
        {
            "ok": True,
            "service_task_running": task_running,
            "sync_interval_sec": _get_sync_interval_sec(),
            "autotrade_enabled": _env_bool("SITE2_AUTOTRADE_ENABLED", True),
            "live_order_enabled": _env_bool("SITE2_LIVE_ORDER", False),
            "trade_trigger": {
                "hour": _get_trade_trigger()[0],
                "minute": _get_trade_trigger()[1],
            },
            "accounts": TARGET_ACCOUNTS,
        }
    )


@SITE2_ROUTER.get("/api/auth/status", response_class=ORJSONResponse)
@SITE2_ROUTER.get("/api/api/auth/status", response_class=ORJSONResponse)
async def site2_auth_status(authorization: str | None = Header(default=None)) -> ORJSONResponse:
    settings = _load_login_settings()
    if not settings["required"]:
        return ORJSONResponse({"login_required": False, "authenticated": True})
    token = _extract_bearer_token(authorization)
    return ORJSONResponse(
        {
            "login_required": True,
            "authenticated": bool(token and _is_token_valid(token)),
        }
    )


@SITE2_ROUTER.post("/api/auth/login", response_class=ORJSONResponse)
@SITE2_ROUTER.post("/api/api/auth/login", response_class=ORJSONResponse)
async def site2_auth_login(payload: _LoginPayload) -> ORJSONResponse:
    settings = _load_login_settings()
    if not settings["required"]:
        return ORJSONResponse(
            {"login_required": False, "token": "", "expires_at": None, "username": ""}
        )

    if payload.username != settings["username"] or payload.password != settings["password"]:
        raise HTTPException(status_code=401, detail="아이디 또는 비밀번호가 올바르지 않습니다.")

    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(tz=KST) + timedelta(hours=SESSION_TTL_HOURS)
    ACTIVE_TOKENS[token] = expires_at
    return ORJSONResponse(
        {
            "login_required": True,
            "token": token,
            "expires_at": expires_at.isoformat(),
            "username": settings["username"],
        }
    )


@SITE2_ROUTER.post("/api/auth/logout", response_class=ORJSONResponse)
@SITE2_ROUTER.post("/api/api/auth/logout", response_class=ORJSONResponse)
async def site2_auth_logout(authorization: str | None = Header(default=None)) -> ORJSONResponse:
    token = _extract_bearer_token(authorization)
    if token:
        ACTIVE_TOKENS.pop(token, None)
    return ORJSONResponse({"ok": True})


@SITE2_ROUTER.get("/api/summary", response_class=ORJSONResponse)
@SITE2_ROUTER.get("/api/api/summary", response_class=ORJSONResponse)
async def site2_summary(_: None = Depends(_require_site2_auth)) -> ORJSONResponse:
    _init_site2_db()
    _ensure_strategy_outputs()

    with _db_connect() as con:
        account_rows = con.execute(
            """
            SELECT ps.account, ps.captured_at, ps.total_krw, ps.cash_krw, ps.holdings_json
            FROM portfolio_snapshot ps
            JOIN (
                SELECT account, MAX(captured_at) AS max_captured_at
                FROM portfolio_snapshot
                GROUP BY account
            ) latest
              ON latest.account = ps.account
             AND latest.max_captured_at = ps.captured_at
            ORDER BY ps.account
            """
        ).fetchall()

        accounts: List[Dict[str, Any]] = []
        for row in account_rows:
            holdings = _parse_holdings_json(row["holdings_json"])
            total_krw = _safe_float(row["total_krw"], 0.0)
            cash_krw = _safe_float(row["cash_krw"], 0.0)
            accounts.append(
                {
                    "account": _safe_int(row["account"]),
                    "captured_at": row["captured_at"],
                    "total_krw": total_krw,
                    "cash_krw": cash_krw,
                    "invested_krw": max(0.0, total_krw - cash_krw),
                    "holding_count": len(holdings),
                }
            )

        trade_status_counts = {
            str(r["status"]): _safe_int(r["cnt"])
            for r in con.execute("SELECT status, COUNT(*) AS cnt FROM trade_history GROUP BY status").fetchall()
        }

        recent_trades = [
            {
                "requested_at": r["requested_at"],
                "account": _safe_int(r["account"]),
                "code": _normalize_code(r["code"]),
                "side": r["side"],
                "qty": _safe_int(r["qty"]),
                "price": _safe_float(r["price"], 0.0),
                "status": r["status"],
                "message": r["message"] or "",
            }
            for r in con.execute(
                """
                SELECT requested_at, account, code, side, qty, price, status, message
                FROM trade_history
                ORDER BY id DESC
                LIMIT 20
                """
            ).fetchall()
        ]

        error_trades_raw = [
            {
                "requested_at": r["requested_at"],
                "account": _safe_int(r["account"]),
                "code": _normalize_code(r["code"]),
                "side": str(r["side"] or ""),
                "qty": _safe_int(r["qty"]),
                "price": _safe_float(r["price"], 0.0),
                "status": r["status"],
                "message": r["message"] or "",
            }
            for r in con.execute(
                """
                SELECT requested_at, account, code, side, qty, price, status, message
                FROM trade_history
                WHERE status = 'ERROR'
                ORDER BY id DESC
                LIMIT 120
                """
            ).fetchall()
        ]

        refresh_events = [
            {
                "created_at": r["created_at"],
                "level": r["level"],
                "event": r["event"],
                "message": r["message"],
            }
            for r in con.execute(
                """
                SELECT created_at, level, event, message
                FROM refresh_log
                ORDER BY id DESC
                LIMIT 12
                """
            ).fetchall()
        ]

    with sqlite3.connect(SELECTED_DB_PATH) as con:
        con.row_factory = sqlite3.Row
        cnt_rows = con.execute(
            "SELECT sleeve, COUNT(*) AS cnt FROM selected_universe GROUP BY sleeve"
        ).fetchall()
        counts = {str(r["sleeve"]).upper(): _safe_int(r["cnt"]) for r in cnt_rows}
        meta_row = con.execute("SELECT * FROM universe_metadata LIMIT 1").fetchone()
        universe_meta = dict(meta_row) if meta_row else {}
        universe_rows = con.execute(
            "SELECT code, name, sleeve FROM selected_universe"
        ).fetchall()

    universe_map = {
        _normalize_code(r["code"]): {
            "name": str(r["name"] or ""),
            "sleeve": str(r["sleeve"] or "").upper(),
        }
        for r in universe_rows
    }

    strategy_selection = {"plan_date": None, "items": []}
    try:
        plan_date, plan_items = _load_strategy_selection_rows_for_day(_today_kst())
        enriched_items = []
        for item in plan_items:
            code = _normalize_code(item.get("code", ""))
            sleeve = str(item.get("sleeve", "") or "").upper()
            meta = universe_map.get(code, {})
            enriched_items.append(
                {
                    "code": code,
                    "name": meta.get("name", ""),
                    "sleeve": sleeve,
                    "weight": _safe_float(item.get("weight", 0.0), 0.0),
                    "allocation_limit_pct": 100 if sleeve == "BOND" else 70,
                }
            )
        strategy_selection = {
            "plan_date": plan_date,
            "items": sorted(enriched_items, key=lambda x: x["weight"], reverse=True),
        }
    except Exception:
        strategy_selection = {"plan_date": None, "items": []}

    error_trades = []
    for row in error_trades_raw:
        meta = universe_map.get(row["code"], {})
        item = {
            **row,
            "name": meta.get("name", ""),
            "sleeve": meta.get("sleeve", "UNKNOWN"),
        }
        error_trades.append(item)

    bond_buy_failures = [
        row
        for row in error_trades
        if row.get("side") == "BUY" and row.get("sleeve") == "BOND"
    ]

    bond_buy_failure_reason_counts: Dict[str, int] = {}
    for row in bond_buy_failures:
        reason = (row.get("message") or "").strip() or "사유 미상"
        bond_buy_failure_reason_counts[reason] = bond_buy_failure_reason_counts.get(reason, 0) + 1

    bond_buy_failure_summary = sorted(
        [{"reason": k, "count": v} for k, v in bond_buy_failure_reason_counts.items()],
        key=lambda x: x["count"],
        reverse=True,
    )

    total_krw_sum = sum(row["total_krw"] for row in accounts)
    cash_krw_sum = sum(row["cash_krw"] for row in accounts)
    risk_target, bond_target = _get_sleeve_weight_targets()

    return ORJSONResponse(
        {
            "generated_at": _iso_now(),
            "accounts": accounts,
            "portfolio_totals": {
                "total_krw": total_krw_sum,
                "cash_krw": cash_krw_sum,
                "invested_krw": max(0.0, total_krw_sum - cash_krw_sum),
            },
            "etf_counts": {
                "limit_70_count": counts.get("RISK", 0),
                "limit_100_count": counts.get("BOND", 0),
                "total_count": counts.get("RISK", 0) + counts.get("BOND", 0),
            },
            "sleeve_targets": {
                "risk_weight": risk_target,
                "bond_weight": bond_target,
                "cash_buffer_weight": max(0.0, 1.0 - (risk_target + bond_target)),
            },
            "trade_status_counts": trade_status_counts,
            "recent_trades": recent_trades,
            "error_trades": error_trades,
            "bond_buy_failures": bond_buy_failures,
            "bond_buy_failure_summary": bond_buy_failure_summary,
            "strategy_selection": strategy_selection,
            "refresh_events": refresh_events,
            "universe_meta": universe_meta,
        }
    )


@SITE2_ROUTER.get("/api/portfolio/history", response_class=ORJSONResponse)
@SITE2_ROUTER.get("/api/api/portfolio/history", response_class=ORJSONResponse)
async def site2_portfolio_history(
    account: int = Query(..., description="계좌번호"),
    limit: int = Query(240, ge=30, le=2000),
    _: None = Depends(_require_site2_auth),
) -> ORJSONResponse:
    with _db_connect() as con:
        rows = con.execute(
            """
            SELECT captured_at, total_krw, cash_krw, holdings_json
            FROM portfolio_snapshot
            WHERE account = ?
            ORDER BY captured_at DESC
            LIMIT ?
            """,
            (account, limit),
        ).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"계좌 {account} 데이터가 없습니다.")

    history = []
    for row in reversed(rows):
        total_krw = _safe_float(row["total_krw"], 0.0)
        cash_krw = _safe_float(row["cash_krw"], 0.0)
        history.append(
            {
                "captured_at": row["captured_at"],
                "total_krw": total_krw,
                "cash_krw": cash_krw,
                "invested_krw": max(0.0, total_krw - cash_krw),
                "holding_count": len(_parse_holdings_json(row["holdings_json"])),
            }
        )
    return ORJSONResponse({"account": account, "history": history})


@SITE2_ROUTER.get("/api/portfolio/latest", response_class=ORJSONResponse)
@SITE2_ROUTER.get("/api/api/portfolio/latest", response_class=ORJSONResponse)
async def site2_portfolio_latest(
    account: int = Query(..., description="계좌번호"),
    _: None = Depends(_require_site2_auth),
) -> ORJSONResponse:
    with _db_connect() as con:
        row = con.execute(
            """
            SELECT captured_at, total_krw, cash_krw, holdings_json
            FROM portfolio_snapshot
            WHERE account = ?
            ORDER BY captured_at DESC
            LIMIT 1
            """,
            (account,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"계좌 {account} 데이터가 없습니다.")

    universe_map = _load_universe_map()
    holdings = _parse_holdings_json(row["holdings_json"])
    for item in holdings:
        uni = universe_map.get(_normalize_code(item["code"]))
        if uni:
            item["sleeve"] = uni["sleeve"]
            item["allocation_limit_pct"] = uni["allocation_limit_pct"]
            item["total_return_3y"] = uni["total_return_3y"]
        else:
            item["sleeve"] = "UNKNOWN"
            item["allocation_limit_pct"] = 70
            item["total_return_3y"] = None

    total_krw = _safe_float(row["total_krw"], 0.0)
    cash_krw = _safe_float(row["cash_krw"], 0.0)

    return ORJSONResponse(
        {
            "account": account,
            "captured_at": row["captured_at"],
            "total_krw": total_krw,
            "cash_krw": cash_krw,
            "invested_krw": max(0.0, total_krw - cash_krw),
            "holdings": holdings,
        }
    )


@SITE2_ROUTER.get("/api/etf/categories", response_class=ORJSONResponse)
@SITE2_ROUTER.get("/api/api/etf/categories", response_class=ORJSONResponse)
async def site2_etf_categories(
    search: str = Query("", max_length=60),
    _: None = Depends(_require_site2_auth),
) -> ORJSONResponse:
    cols = _selected_universe_columns()
    days_col = "days" if "days" in cols else "days_in_3y" if "days_in_3y" in cols else None
    first_date_col = "first_date" if "first_date" in cols else None
    last_date_col = "last_date" if "last_date" in cols else None

    days_sql = days_col if days_col else "NULL"
    first_sql = first_date_col if first_date_col else "NULL"
    last_sql = last_date_col if last_date_col else "NULL"

    with sqlite3.connect(SELECTED_DB_PATH) as con:
        con.row_factory = sqlite3.Row
        max_trade_row = con.execute("SELECT MAX(trade_date) AS d FROM etf_daily_close").fetchone()
        max_trade_date = max_trade_row["d"] if max_trade_row else None

        query = f"""
            SELECT
                su.code,
                su.name,
                su.sleeve,
                su.total_return_3y,
                {days_sql} AS days,
                {first_sql} AS first_date,
                {last_sql} AS last_date,
                dc.close_price AS latest_close
            FROM selected_universe su
            LEFT JOIN etf_daily_close dc
              ON dc.code = su.code
             AND dc.trade_date = ?
        """
        params: List[Any] = [max_trade_date]
        if search.strip():
            keyword = f"%{search.strip()}%"
            query += " WHERE su.code LIKE ? OR su.name LIKE ?"
            params.extend([keyword, keyword])
        query += " ORDER BY su.sleeve, su.total_return_3y DESC"
        rows = con.execute(query, params).fetchall()

    held_codes = _latest_holding_codes()

    def _fmt_trade_date(v: Any) -> str | None:
        s = str(v or "").strip()
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        return s or None

    limit_map = {"RISK": 70, "BOND": 100}
    limit_70: List[Dict[str, Any]] = []
    limit_100: List[Dict[str, Any]] = []
    for row in rows:
        sleeve = str(row["sleeve"] or "").upper()
        limit_pct = limit_map.get(sleeve, 70)
        item = {
            "code": _normalize_code(row["code"]),
            "name": row["name"],
            "sleeve": sleeve,
            "allocation_limit_pct": limit_pct,
            "total_return_3y": _safe_float(row["total_return_3y"], 0.0),
            "days": _safe_int(row["days"], 0),
            "first_date": _fmt_trade_date(row["first_date"]),
            "last_date": _fmt_trade_date(row["last_date"]),
            "latest_close": _safe_float(row["latest_close"], 0.0),
            "currently_held": _normalize_code(row["code"]) in held_codes,
        }
        if limit_pct == 100:
            limit_100.append(item)
        else:
            limit_70.append(item)

    return ORJSONResponse(
        {
            "as_of_trade_date": _fmt_trade_date(max_trade_date),
            "search": search.strip(),
            "limit_70": limit_70,
            "limit_100": limit_100,
            "counts": {
                "limit_70_count": len(limit_70),
                "limit_100_count": len(limit_100),
                "total_count": len(limit_70) + len(limit_100),
            },
        }
    )


@SITE2_ROUTER.get("/api/etf/price-series", response_class=ORJSONResponse)
@SITE2_ROUTER.get("/api/api/etf/price-series", response_class=ORJSONResponse)
async def site2_etf_price_series(
    code: str = Query(..., min_length=4, max_length=8),
    days: int = Query(360, ge=60, le=3000),
    _: None = Depends(_require_site2_auth),
) -> ORJSONResponse:
    target_code = _normalize_code(code)
    with sqlite3.connect(SELECTED_DB_PATH) as con:
        con.row_factory = sqlite3.Row
        name_row = con.execute(
            "SELECT name FROM etf_master WHERE code = ?",
            (target_code,),
        ).fetchone()
        if not name_row:
            raise HTTPException(status_code=404, detail=f"ETF code {target_code}을(를) 찾을 수 없습니다.")

        rows = con.execute(
            """
            SELECT trade_date, close_price
            FROM etf_daily_close
            WHERE code = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            (target_code, days),
        ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"ETF code {target_code} 가격 데이터가 없습니다.")

    ordered = list(reversed(rows))
    first_price = _safe_float(ordered[0]["close_price"], 0.0)
    series = []
    for row in ordered:
        close_price = _safe_float(row["close_price"], 0.0)
        raw_date = str(row["trade_date"])
        trade_date = (
            f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
            if len(raw_date) == 8 and raw_date.isdigit()
            else raw_date
        )
        change_pct = (close_price / first_price - 1.0) if first_price > 0 else 0.0
        series.append(
            {
                "trade_date": trade_date,
                "close_price": close_price,
                "change_pct": change_pct,
            }
        )

    return ORJSONResponse(
        {
            "code": target_code,
            "name": name_row["name"],
            "days": days,
            "series": series,
        }
    )
