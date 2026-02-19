#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backtest_dual_sleeve_grid.py

목적
- selected_universe DB(선정된 ETF들만 포함)로부터 가격을 읽어서
- 70%(RISK) / 30%(BOND) 룰을 강제하는 듀얼 슬리브 로테이션 전략을 백테스트
- 파라미터 그리드 탐색 후, (학습/검증 분리) 기반으로 "미래 기대 수익률"을 고려한 베스트 조합을 선택
- 자동매매용 타겟 비중 파일(날짜별 code,weight)을 생성

백테스트는 close-only DB이므로 close[t]→close[next] 수익률 기반입니다.
(실전 체결은 시가/장중/종가 등으로 달라질 수 있어 슬리피지/스프레드 반영이 필요)

사용 예:
  python3 backtest_dual_sleeve_grid.py --db_path ./etf_daily_close_selected_universe.db --out_dir ./out

옵션:
  --start 2018-01-01
  --train_end 2023-12-29
  --test_start 2024-01-02
"""

from __future__ import annotations
import argparse
import os
import json
import math
import sqlite3
import datetime as dt
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def to_dt(s: str) -> pd.Timestamp:
    return pd.to_datetime(s)

def yyyymmdd_to_dt(s: str) -> pd.Timestamp:
    return pd.to_datetime(s, format="%Y%m%d")

def dt_to_yyyymmdd(ts: pd.Timestamp) -> str:
    return ts.strftime("%Y%m%d")

def load_master_and_universe(con: sqlite3.Connection) -> pd.DataFrame:
    master = pd.read_sql_query("SELECT code, name FROM etf_master", con)
    sel = pd.read_sql_query("SELECT code, sleeve FROM selected_universe", con)
    m = sel.merge(master, on="code", how="left")
    return m

def load_close_matrix(con: sqlite3.Connection, codes: List[str], start: str) -> pd.DataFrame:
    # load only selected codes, from start date
    start_dt = to_dt(start)
    start_key = start_dt.strftime("%Y%m%d")
    # chunk IN clause
    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]
    parts=[]
    for ch in chunks(codes, 900):
        q = "SELECT code, trade_date, close_price FROM etf_daily_close WHERE trade_date >= ? AND code IN ({})".format(",".join(["?"]*len(ch)))
        params = [start_key] + ch
        parts.append(pd.read_sql_query(q, con, params=params))
    df = pd.concat(parts, ignore_index=True)
    df["trade_date"] = df["trade_date"].apply(yyyymmdd_to_dt)
    close = df.pivot(index="trade_date", columns="code", values="close_price").sort_index()
    return close

def precompute_features(close: pd.DataFrame, ret: pd.DataFrame, mom_lbs: List[int], vol_lbs: List[int], sma_filters: List[int]):
    idx = ret.index
    caches = {"close_shift1": close.shift(1).reindex(idx), "mom":{}, "vol":{}, "sma":{}}
    for ml in mom_lbs:
        caches["mom"][ml] = close.pct_change(ml).shift(1).reindex(idx)
    for vl in vol_lbs:
        caches["vol"][vl] = close.pct_change().shift(1).rolling(vl).std(ddof=0).reindex(idx)
    for s in sma_filters:
        caches["sma"][s] = close.rolling(s).mean().shift(1).reindex(idx)
    return caches

def eval_config_fast(close: pd.DataFrame, ret: pd.DataFrame, caches: dict, score_type: str, mom_lb: int, vol_lb: Optional[int],
                     rebalance_n: int, top_k: int, sma_filter: Optional[int], cost_rate: float):
    idx = ret.index
    mom = caches["mom"][mom_lb]
    if score_type == "mom":
        score = mom
    else:
        if vol_lb is None:
            raise ValueError("vol_lb required")
        vol = caches["vol"][vol_lb]
        if score_type == "mom_over_vol":
            score = mom / vol
        elif score_type == "mom_times_vol":
            score = mom * vol
        elif score_type == "mom2_over_vol":
            score = (mom**2) / vol
        elif score_type == "mom_over_vol2":
            score = mom / (vol**2)
        else:
            raise ValueError(score_type)

    if sma_filter is not None:
        sma = caches["sma"][sma_filter]
        eligible = caches["close_shift1"] > sma
        score = score.where(eligible)

    score = score.where(ret.notna())
    score_np = score.to_numpy(dtype=np.float64)
    score_np[~np.isfinite(score_np)] = -np.inf
    ret_np = np.nan_to_num(ret.to_numpy(dtype=np.float64), nan=0.0)

    T, N = score_np.shape
    reb_mask = np.zeros(T, dtype=bool)
    reb_mask[::rebalance_n] = True

    # top-k per day
    if top_k == 1:
        top_idx = np.argmax(score_np, axis=1).reshape(-1,1)
        valid_row = score_np[np.arange(T), top_idx[:,0]] > -np.inf
    else:
        top_idx = np.argpartition(-score_np, top_k-1, axis=1)[:,:top_k]
        min_score = np.min(score_np[np.arange(T)[:,None], top_idx], axis=1)
        valid_row = min_score > -np.inf
    top_idx = top_idx.astype(np.int32)
    top_idx[~valid_row,:] = -1

    # apply rebalance schedule (ffill selections)
    sel = top_idx.copy()
    sel[~reb_mask,:] = -2
    valid_sel = np.zeros(T, dtype=bool)
    last = None
    for t in range(T):
        if reb_mask[t]:
            if sel[t,0] >= 0:
                last = sel[t].copy()
                valid_sel[t] = True
            else:
                last = None
                valid_sel[t] = False
        else:
            if last is not None:
                sel[t] = last
                valid_sel[t] = True
            else:
                sel[t] = -1
                valid_sel[t] = False

    sel_safe = np.where(sel>=0, sel, 0)
    picked = np.take_along_axis(ret_np, sel_safe, axis=1)
    gross = picked.mean(axis=1)
    gross[~valid_sel] = 0.0

    # turnover on rebalance days
    turnover = np.zeros(T, dtype=np.float64)
    prev = None
    prev_valid = False
    for t in range(T):
        if reb_mask[t]:
            cur_valid = bool(valid_sel[t])
            cur = tuple(int(x) for x in sel[t]) if cur_valid else None
            if prev_valid and prev is not None and cur_valid and cur is not None:
                overlap = len(set(prev).intersection(cur))
                turnover[t] = 2.0 * (1.0 - overlap/top_k)
            elif (not prev_valid or prev is None) and cur_valid and cur is not None:
                turnover[t] = 1.0
            elif prev_valid and prev is not None and (not cur_valid or cur is None):
                turnover[t] = 1.0
            else:
                turnover[t] = 0.0
            prev = cur
            prev_valid = cur_valid

    cost = cost_rate * turnover
    net = gross - cost
    return net.astype(np.float32), sel.astype(np.int16), valid_sel

def log_eq(net: np.ndarray) -> float:
    x = np.clip(1+net, 1e-12, None)
    return float(np.log(x).sum())

def grid_search(close: pd.DataFrame, ret: pd.DataFrame, sleeve: str, cost_rate: float,
               score_types: List[str], mom_lbs: List[int], vol_lbs: List[int],
               rebalance_ns: List[int], top_ks: List[int], sma_filters: List[Optional[int]],
               mask_train: np.ndarray, mask_test: np.ndarray):
    sma_ints = [s for s in sma_filters if s is not None]
    caches = precompute_features(close, ret, mom_lbs, vol_lbs, sma_ints)

    results=[]
    nets=[]
    cfgs=[]
    for st in score_types:
        for ml in mom_lbs:
            vol_opts = [None] if st=="mom" else vol_lbs
            for vl in vol_opts:
                for rb in rebalance_ns:
                    for k in top_ks:
                        for sf in sma_filters:
                            net, sel, valid = eval_config_fast(close, ret, caches, st, ml, vl, rb, k, sf, cost_rate)
                            l_tr = log_eq(net[mask_train])
                            l_te = log_eq(net[mask_test])
                            results.append({
                                "sleeve": sleeve,
                                "score_type": st,
                                "mom_lb": ml,
                                "vol_lb": "" if vl is None else vl,
                                "rebalance_n": rb,
                                "top_k": k,
                                "sma_filter": "" if sf is None else sf,
                                "eq_train": float(np.exp(l_tr)),
                                "eq_test": float(np.exp(l_te)),
                                "eq_all": float(np.exp(l_tr+l_te)),
                                "log_train": l_tr,
                                "log_test": l_te,
                            })
                            nets.append(net)
                            cfgs.append((st, ml, vl, rb, k, sf))
    df = pd.DataFrame(results)
    net_mat = np.vstack(nets)
    return df, net_mat, cfgs, caches

def select_best_pair(df_r, net_r, cfg_r, df_b, net_b, cfg_b, mask_train, mask_test, w_r=0.7, w_b=0.3, preselect_n=150, alpha=0.8):
    # preselect by train equity
    idx_r = df_r.sort_values("eq_train", ascending=False).head(preselect_n).index.to_numpy()
    idx_b = df_b.sort_values("eq_train", ascending=False).head(preselect_n).index.to_numpy()

    r_tr = net_r[idx_r][:, mask_train]
    r_te = net_r[idx_r][:, mask_test]
    b_tr = net_b[idx_b][:, mask_train]
    b_te = net_b[idx_b][:, mask_test]

    best=None
    best_obj=-1e18
    best_metrics=None

    for a_i, i in enumerate(idx_r):
        comb_tr = w_r*r_tr[a_i][None,:] + w_b*b_tr
        comb_te = w_r*r_te[a_i][None,:] + w_b*b_te
        log_tr = np.log(np.clip(1+comb_tr, 1e-12, None)).sum(axis=1)
        log_te = np.log(np.clip(1+comb_te, 1e-12, None)).sum(axis=1)
        obj = alpha*log_te + (1-alpha)*log_tr
        j_local = int(np.argmax(obj))
        if obj[j_local] > best_obj:
            best_obj = float(obj[j_local])
            best = (int(i), int(idx_b[j_local]))
            best_metrics = {
                "obj": best_obj,
                "eq_train": float(np.exp(log_tr[j_local])),
                "eq_test": float(np.exp(log_te[j_local])),
                "eq_all": float(np.exp(log_tr[j_local]+log_te[j_local])),
            }
    return best, best_metrics

def annual_summary(net: pd.Series) -> pd.DataFrame:
    df = pd.DataFrame({"net": net})
    df["year"] = df.index.year
    rows=[]
    for y,g in df.groupby("year"):
        eq = (1+g["net"]).cumprod()
        rows.append({
            "year": int(y),
            "days": int(len(g)),
            "year_return": float(eq.iloc[-1]-1),
            "year_MDD": float((eq/eq.cummax()-1).min()),
        })
    return pd.DataFrame(rows).sort_values("year")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db_path", required=True)
    ap.add_argument("--out_dir", default="./out")
    ap.add_argument("--start", default="2018-01-01")
    ap.add_argument("--train_end", default="2023-12-29")
    ap.add_argument("--test_start", default="2024-01-02")
    ap.add_argument("--cost_rate", type=float, default=0.0005)
    ap.add_argument("--risk_weight", type=float, default=0.7)
    ap.add_argument("--bond_weight", type=float, default=0.3)
    ap.add_argument("--alpha", type=float, default=0.8, help="objective에서 test 비중(0~1)")
    ap.add_argument("--preselect_n", type=int, default=150)
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    con = sqlite3.connect(args.db_path)
    uni = load_master_and_universe(con)
    risk_codes = uni[uni["sleeve"]=="RISK"]["code"].tolist()
    bond_codes = uni[uni["sleeve"]=="BOND"]["code"].tolist()

    close = load_close_matrix(con, risk_codes + bond_codes, start=args.start)
    con.close()

    # close->close next return
    ret = close.shift(-1)/close - 1.0
    ret = ret.iloc[:-1]
    close = close.reindex(ret.index)  # align

    dates = ret.index
    train_end = to_dt(args.train_end)
    test_start = to_dt(args.test_start)
    mask_train = np.asarray(dates <= train_end)
    mask_test = np.asarray(dates >= test_start)

    # sleeve matrices
    close_r = close[risk_codes]
    ret_r = ret[risk_codes]
    close_b = close[bond_codes]
    ret_b = ret[bond_codes]

    # grid (기본값: 충분히 넓지만 과도하진 않게)
    score_types = ["mom", "mom_over_vol", "mom_times_vol", "mom2_over_vol"]
    mom_lbs = [20, 60, 120, 180]
    vol_lbs = [20, 60, 120]
    rebalance_ns = [1, 5, 10, 21]
    sma_filters = [None, 200]
    topk_risk = [1, 3]
    topk_bond = [1, 2]

    df_r, net_r, cfg_r, _ = grid_search(close_r, ret_r, "RISK", args.cost_rate, score_types, mom_lbs, vol_lbs, rebalance_ns, topk_risk, sma_filters, mask_train, mask_test)
    df_b, net_b, cfg_b, _ = grid_search(close_b, ret_b, "BOND", args.cost_rate, score_types, mom_lbs, vol_lbs, rebalance_ns, topk_bond, sma_filters, mask_train, mask_test)

    df_r.sort_values("eq_train", ascending=False).head(50).to_csv(os.path.join(args.out_dir, "top_risk_strategies.csv"), index=False)
    df_b.sort_values("eq_train", ascending=False).head(50).to_csv(os.path.join(args.out_dir, "top_bond_strategies.csv"), index=False)

    best_pair, best_metrics = select_best_pair(df_r, net_r, cfg_r, df_b, net_b, cfg_b, mask_train, mask_test,
                                              w_r=args.risk_weight, w_b=args.bond_weight, preselect_n=args.preselect_n, alpha=args.alpha)
    i_r, i_b = best_pair
    cfgR = cfg_r[i_r]
    cfgB = cfg_b[i_b]

    # Re-run best to get selections
    # (grid_search에서 net만 저장했으니, 여기서는 feature cache 재생성을 위해 다시 eval 수행)
    # 단, 결과 재현성에 문제 없도록 동일 파라미터 사용
    # 재계산은 비용이 크지 않음 (유니버스가 작음)
    def compute_best(close_s, ret_s, cfg):
        st, ml, vl, rb, k, sf = cfg
        sma_ints = [200] if sf == 200 else []
        caches = precompute_features(close_s, ret_s, [ml], [vl] if vl is not None else [20], sma_ints)
        net, sel, valid = eval_config_fast(close_s, ret_s, caches, st, ml, vl, rb, k, sf, args.cost_rate)
        return net, sel, valid

    netR, selR, validR = compute_best(close_r, ret_r, cfgR)
    netB, selB, validB = compute_best(close_b, ret_b, cfgB)

    netC = args.risk_weight*netR + args.bond_weight*netB
    eq = np.cumprod(1+netC)

    out_cfg = {
        "db_path": os.path.abspath(args.db_path),
        "start": args.start,
        "train_end": args.train_end,
        "test_start": args.test_start,
        "risk_weight": args.risk_weight,
        "bond_weight": args.bond_weight,
        "cost_rate": args.cost_rate,
        "objective_alpha": args.alpha,
        "best_pair_metrics": best_metrics,
        "risk_strategy": {"score_type": cfgR[0], "mom_lb": cfgR[1], "vol_lb": cfgR[2], "rebalance_n": cfgR[3], "top_k": cfgR[4], "sma_filter": cfgR[5]},
        "bond_strategy": {"score_type": cfgB[0], "mom_lb": cfgB[1], "vol_lb": cfgB[2], "rebalance_n": cfgB[3], "top_k": cfgB[4], "sma_filter": cfgB[5]},
        "risk_universe_size": len(risk_codes),
        "bond_universe_size": len(bond_codes),
        "note": "close-only backtest. Signals use t-1 data (shift(1)). Trade/execution assumptions differ in live.",
    }
    with open(os.path.join(args.out_dir, "best_config.json"), "w", encoding="utf-8") as f:
        json.dump(out_cfg, f, ensure_ascii=False, indent=2)

    # daily returns & annual summary
    daily = pd.DataFrame({"date": dates.astype(str), "net_return": netC, "equity": eq})
    daily.to_csv(os.path.join(args.out_dir, "best_daily_returns.csv"), index=False)
    annual = annual_summary(pd.Series(netC, index=dates))
    annual.to_csv(os.path.join(args.out_dir, "best_annual_summary.csv"), index=False)

    # equity plot
    plt.figure()
    plt.plot(dates, eq)
    plt.title("Dual-sleeve equity (70% RISK rotation + 30% BOND rotation)")
    plt.xlabel("Date")
    plt.ylabel("Equity (start=1.0)")
    plt.tight_layout()
    plt.savefig(os.path.join(args.out_dir, "best_equity_curve.png"), dpi=150)

    # weights_by_day_long.csv
    # selections -> weights: sleeve_weight * (1/top_k) each selected code
    risk_codes_arr = np.array(risk_codes)
    bond_codes_arr = np.array(bond_codes)

    rows=[]
    for t, d in enumerate(dates):
        # risk
        if validR[t]:
            ids = selR[t]
            ids = ids[ids>=0]
            k = len(ids)
            if k>0:
                w = args.risk_weight / k
                for j in ids:
                    rows.append({"date": d.strftime("%Y-%m-%d"), "code": risk_codes_arr[int(j)], "sleeve": "RISK", "weight": w})
        # bond
        if validB[t]:
            ids = selB[t]
            ids = ids[ids>=0]
            k = len(ids)
            if k>0:
                w = args.bond_weight / k
                for j in ids:
                    rows.append({"date": d.strftime("%Y-%m-%d"), "code": bond_codes_arr[int(j)], "sleeve": "BOND", "weight": w})

    wdf = pd.DataFrame(rows)
    wdf.to_csv(os.path.join(args.out_dir, "best_target_weights_long.csv"), index=False)

    print("Saved outputs to:", os.path.abspath(args.out_dir))
    print("Best config:", json.dumps(out_cfg["risk_strategy"], ensure_ascii=False), json.dumps(out_cfg["bond_strategy"], ensure_ascii=False))
    print("Equity(all):", float(eq[-1]))

if __name__ == "__main__":
    main()
