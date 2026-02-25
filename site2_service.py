from __future__ import annotations

import asyncio
import json
import math
import os
import sqlite3
import subprocess
import traceback
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import dotenv_values
from fastapi import APIRouter
from fastapi.responses import HTMLResponse, ORJSONResponse

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
    # Prefer .env values so runtime settings are not overridden by stale process env.
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
    minute = int(_env_raw("SITE2_TRADE_MINUTE") or "5")
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
    return df


def _load_best_config() -> Dict[str, Any]:
    _ensure_strategy_outputs()
    if not BEST_CONFIG_PATH.exists():
        return {}
    return json.loads(BEST_CONFIG_PATH.read_text(encoding="utf-8"))


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

        try:
            response = bot.create_order(
                "KRX",
                preview["code"],
                "market",
                preview["side"].lower(),
                int(preview["qty"]),
            )
            status = "SUCCESS"
            message = "order submitted"
            if isinstance(response, dict) and response.get("rt_cd") not in (None, "0"):
                status = "ERROR"
                message = str(response.get("msg1", "order rejected"))
                errors += 1
            else:
                executed_count += 1
        except Exception as exc:
            response = {"error": str(exc)}
            status = "ERROR"
            message = str(exc)
            errors += 1

        _record_trade_history(
            account=account,
            requested_at=requested_at,
            preview=preview,
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
    market_close = time(hour=15, minute=30)
    return trigger <= now.time() <= market_close


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
    return HTMLResponse(
        """
<!doctype html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>연금 자동매매 Site2</title>
  <style>
    :root {
      --bg: #f2f5f7;
      --ink: #102027;
      --card: #ffffff;
      --line: #d6dde2;
      --accent: #0f766e;
      --accent-2: #0c4a6e;
      --danger: #b91c1c;
      --muted: #546e7a;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Pretendard Variable", "Noto Sans KR", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(1200px 600px at 100% -10%, #d5f5e6 0%, rgba(213,245,230,0) 55%),
        radial-gradient(1200px 500px at -10% 0%, #dbeafe 0%, rgba(219,234,254,0) 50%),
        var(--bg);
    }
    .wrap { max-width: 1300px; margin: 0 auto; padding: 18px; }
    .top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 14px;
    }
    .title {
      font-size: 26px;
      font-weight: 800;
      letter-spacing: -0.5px;
    }
    .meta { color: var(--muted); font-size: 13px; }
    .btns { display: flex; gap: 8px; }
    button {
      border: 0;
      border-radius: 10px;
      padding: 10px 14px;
      color: #fff;
      background: linear-gradient(135deg, var(--accent), var(--accent-2));
      cursor: pointer;
      font-weight: 700;
    }
    button.warn {
      background: linear-gradient(135deg, #ef4444, #b91c1c);
    }
    .grid {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(12, minmax(0, 1fr));
    }
    .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 12px;
      box-shadow: 0 8px 22px rgba(16, 32, 39, 0.06);
    }
    .span-6 { grid-column: span 6; }
    .span-12 { grid-column: span 12; }
    .card h3 {
      margin: 0 0 10px;
      font-size: 15px;
      color: var(--accent-2);
      letter-spacing: 0.1px;
    }
    .kpis { display: flex; flex-wrap: wrap; gap: 8px; }
    .kpi {
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      padding: 8px 10px;
      min-width: 160px;
    }
    .kpi .k { font-size: 11px; color: #64748b; }
    .kpi .v { font-size: 16px; font-weight: 700; margin-top: 2px; }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }
    th, td {
      border-bottom: 1px solid #edf2f7;
      text-align: left;
      padding: 6px 4px;
      vertical-align: top;
    }
    th { color: #475569; font-weight: 700; }
    .small { font-size: 12px; color: #64748b; }
    .pill {
      display: inline-block;
      border-radius: 999px;
      padding: 3px 8px;
      font-size: 11px;
      font-weight: 700;
      background: #e2e8f0;
      color: #334155;
    }
    .pill.ok { background: #dcfce7; color: #166534; }
    .pill.err { background: #fee2e2; color: #991b1b; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    @media (max-width: 960px) {
      .span-6, .span-12 { grid-column: span 12; }
      .title { font-size: 22px; }
      .top { flex-direction: column; align-items: flex-start; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <div>
        <div class="title">연금 자동매매 Site2</div>
        <div class="meta" id="meta">데이터 로딩 중...</div>
      </div>
      <div class="btns">
        <button onclick="manualRefresh()">DB/KIS 최신화</button>
        <button class="warn" onclick="manualTrade()">지금 매매 실행</button>
      </div>
    </div>

    <div class="grid">
      <section class="card span-12">
        <h3>운영 상태</h3>
        <div class="kpis" id="kpis"></div>
      </section>

      <section class="card span-6">
        <h3>KIS5 계좌 현황</h3>
        <div id="acct5"></div>
      </section>

      <section class="card span-6">
        <h3>KIS8 계좌 현황</h3>
        <div id="acct8"></div>
      </section>

      <section class="card span-12">
        <h3>지난 매매 내역</h3>
        <div id="trades"></div>
      </section>

      <section class="card span-12">
        <h3>앞으로의 매매 계획</h3>
        <div id="plan"></div>
      </section>

      <section class="card span-12">
        <h3>최신화 로그</h3>
        <div id="logs"></div>
      </section>
    </div>
  </div>

  <script>
    const fmt = new Intl.NumberFormat('ko-KR');
    let latestData = null;

    function won(v) { return `${fmt.format(Math.round(v || 0))}원`; }
    function pct(v) { return `${((v || 0) * 100).toFixed(2)}%`; }

    function badge(status) {
      const cls = (status === 'SUCCESS' || status === 'SIMULATED') ? 'ok' : (status === 'ERROR' ? 'err' : '');
      return `<span class="pill ${cls}">${status}</span>`;
    }

    function accountBox(account) {
      if (!account || !account.captured_at) {
        return `<div class="small">스냅샷 데이터 없음</div>`;
      }
      const preview = account.order_preview || [];
      const holdings = account.holdings || [];

      let h = '<div class="kpis">';
      h += `<div class="kpi"><div class="k">총 평가액</div><div class="v">${won(account.total_krw)}</div></div>`;
      h += `<div class="kpi"><div class="k">현금 추정</div><div class="v">${won(account.cash_krw)}</div></div>`;
      h += `<div class="kpi"><div class="k">계획 기준일</div><div class="v mono">${account.plan_date}</div></div>`;
      h += `<div class="kpi"><div class="k">예상 주문수</div><div class="v">${preview.length}건</div></div>`;
      h += '</div>';

      h += '<div style="margin-top:10px"><table><thead><tr><th>종목</th><th>수량</th><th>평가액</th><th>비중</th></tr></thead><tbody>';
      for (const row of holdings.slice(0, 12)) {
        h += `<tr><td class="mono">${row.code} ${row.name || ''}</td><td>${fmt.format(row.qty)}</td><td>${won(row.eval_amount)}</td><td>${pct(row.weight)}</td></tr>`;
      }
      h += '</tbody></table></div>';

      h += '<div style="margin-top:10px"><div class="small">리밸런싱 예정 주문(상위 12건)</div>';
      h += '<table><thead><tr><th>코드</th><th>구분</th><th>수량</th><th>현재→목표</th><th>목표비중</th></tr></thead><tbody>';
      for (const row of preview.slice(0, 12)) {
        h += `<tr><td class="mono">${row.code}</td><td>${row.side}</td><td>${fmt.format(row.qty)}</td><td>${fmt.format(row.current_qty)}→${fmt.format(row.target_qty)}</td><td>${pct(row.target_weight)}</td></tr>`;
      }
      h += '</tbody></table></div>';
      return h;
    }

    function renderTrades(trades) {
      let h = '<table><thead><tr><th>ID</th><th>시각</th><th>계좌</th><th>코드</th><th>구분</th><th>수량</th><th>상태</th><th>메시지</th></tr></thead><tbody>';
      for (const t of (trades || []).slice(0, 80)) {
        h += `<tr><td>${t.id}</td><td class="mono">${t.requested_at}</td><td>KIS${t.account}</td><td class="mono">${t.code}</td><td>${t.side}</td><td>${fmt.format(t.qty)}</td><td>${badge(t.status)}</td><td>${t.message || ''}</td></tr>`;
      }
      h += '</tbody></table>';
      return h;
    }

    function renderPlan(plan) {
      let h = '<table><thead><tr><th>날짜</th><th>구분</th><th>상위 종목</th></tr></thead><tbody>';
      for (const p of (plan || []).slice(0, 15)) {
        const head = (p.items || []).slice(0, 8).map(x => `${x.code}(${pct(x.weight)})`).join(', ');
        h += `<tr><td class="mono">${p.date}</td><td>${p.projected ? 'Projection' : 'From Backtest'}</td><td class="mono">${head}</td></tr>`;
      }
      h += '</tbody></table>';
      return h;
    }

    function renderLogs(logs) {
      let h = '<table><thead><tr><th>시각</th><th>레벨</th><th>이벤트</th><th>메시지</th></tr></thead><tbody>';
      for (const log of (logs || []).slice(0, 80)) {
        h += `<tr><td class="mono">${log.created_at}</td><td>${log.level}</td><td>${log.event}</td><td>${log.message}</td></tr>`;
      }
      h += '</tbody></table>';
      return h;
    }

    function renderKpis(data) {
      const settings = data.settings || {};
      const quote = data.quote_status || {};
      const cfg = data.config || {};
      const s = cfg.best_pair_metrics || {};

      let h = '';
      h += `<div class="kpi"><div class="k">활성 플랜 기준일</div><div class="v mono">${data.active_plan_date || '-'}</div></div>`;
      h += `<div class="kpi"><div class="k">실시간 시세 종목수</div><div class="v">${fmt.format(quote.count || 0)}개</div></div>`;
      h += `<div class="kpi"><div class="k">최근 시세 동기화</div><div class="v mono">${quote.updated_at || '-'}</div></div>`;
      h += `<div class="kpi"><div class="k">자동매매 스케줄</div><div class="v mono">${settings.trade_trigger?.hour ?? 9}:${String(settings.trade_trigger?.minute ?? 5).padStart(2,'0')}</div></div>`;
      h += `<div class="kpi"><div class="k">자동매매 활성</div><div class="v">${settings.autotrade_enabled ? 'ON' : 'OFF'}</div></div>`;
      h += `<div class="kpi"><div class="k">실주문 실행</div><div class="v">${settings.live_order_enabled ? 'ON' : 'DRY-RUN'}</div></div>`;
      h += `<div class="kpi"><div class="k">백테스트 누적 성과</div><div class="v">x${(s.eq_all || 0).toFixed(3)}</div></div>`;
      return h;
    }

    async function loadDashboard() {
      const res = await fetch('/site2/api/dashboard', { cache: 'no-store' });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      latestData = data;

      document.getElementById('meta').textContent = `현재시각 ${data.now} | 전략 기준일 ${data.active_plan_date}`;
      document.getElementById('kpis').innerHTML = renderKpis(data);

      const a5 = (data.accounts || []).find(a => a.account === 5);
      const a8 = (data.accounts || []).find(a => a.account === 8);
      document.getElementById('acct5').innerHTML = accountBox(a5);
      document.getElementById('acct8').innerHTML = accountBox(a8);

      document.getElementById('trades').innerHTML = renderTrades(data.recent_trades);
      document.getElementById('plan').innerHTML = renderPlan(data.future_plan);
      document.getElementById('logs').innerHTML = renderLogs(data.refresh_logs);
    }

    async function manualRefresh() {
      await fetch('/site2/api/refresh-now', { method: 'POST' });
      await loadDashboard();
    }

    async function manualTrade() {
      await fetch('/site2/api/trade-now', { method: 'POST' });
      await loadDashboard();
    }

    async function loop() {
      try {
        await loadDashboard();
      } catch (err) {
        document.getElementById('meta').textContent = `로딩 실패: ${err.message}`;
      } finally {
        setTimeout(loop, 20000);
      }
    }

    loop();
  </script>
</body>
</html>
        """.strip()
    )


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
