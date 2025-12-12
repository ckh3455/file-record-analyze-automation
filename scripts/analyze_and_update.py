# -*- coding: utf-8 -*-
from __future__ import annotations

import os, re, json, time, random
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Optional, Union

import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ===================== 설정 =====================
LOG_DIR = Path("analyze_report")
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "artifacts")

SUMMARY_SHEET_NAME = "거래요약"

# 거래요약 헤더에 존재할 수 있는 열들(실제로 존재하는 열에만 기록)
SUMMARY_COLS = [
    "전국", "서울", "서울특별시",
    "강남구", "압구정동",
    "경기도", "인천광역시", "세종특별자치시", "울산광역시",
    "서초구", "송파구", "용산구", "강동구", "성동구", "마포구", "양천구", "동작구", "영등포구", "종로구", "광진구",
    "강서구", "강북구", "관악구", "구로구", "금천구", "도봉구", "노원구",
    "동대문구", "서대문구", "성북구", "은평구", "중구", "중랑구",
    "부산광역시", "대구광역시", "광주광역시", "대전광역시",
    "강원특별자치도", "경상남도", "경상북도", "전라남도", "전북특별자치도", "충청남도", "충청북도", "제주특별자치도"
]

# 서울/전국 탭의 지역 열
SEOUL_REGIONS = [
    "강남구","강동구","강북구","강서구","관악구","광진구","구로구","금천구","노원구","도봉구",
    "동대문구","동작구","마포구","서대문구","서초구","성동구","성북구","송파구","양천구","영등포구",
    "용산구","은평구","종로구","중구","중랑구","총합계"
]
NATION_REGIONS = [
    "강원특별자치도","경기도","경상남도","경상북도","광주광역시","대구광역시","대전광역시","부산광역시",
    "서울특별시","세종특별자치시","울산광역시","인천광역시","전라남도","전북특별자치도","제주특별자치도",
    "충청남도","충청북도","총합계"
]

# ===== 한국 공휴일 (2024-10 ~ 2025-09) =====
KR_HOLIDAYS = {
    "2024-10-03", "2024-10-09", "2024-12-25",
    "2025-01-01", "2025-01-27", "2025-01-28", "2025-01-29", "2025-01-30",
    "2025-03-01", "2025-03-03",
    "2025-05-05", "2025-05-06",
    "2025-06-06",
    "2025-08-15",
}

# ===================== 로깅/리트라이 =====================
def _ensure_logdir():
    try:
        if LOG_DIR.exists() and not LOG_DIR.is_dir():
            LOG_DIR.unlink()
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

_ensure_logdir()
RUN_LOG = LOG_DIR / "latest.log"

def log(msg: str):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line)
    try:
        with RUN_LOG.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

_LAST = 0.0
def _throttle(sec=0.60):  # Sheets API RPS 완화
    import time as _t
    global _LAST
    now = _t.time()
    if now - _LAST < sec:
        _t.sleep(sec - (now - _LAST))
    _LAST = _t.time()

def _retry(fn, *a, **kw):
    base = 0.8
    for i in range(7):
        try:
            _throttle()
            return fn(*a, **kw)
        except APIError as e:
            s = str(e)
            if any(x in s for x in ("429", "500", "502", "503")):
                time.sleep(base * (2 ** i) + random.uniform(0, 0.25))
                continue
            raise

# ===================== 이름/정규화/캐시 =====================
def _norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "").replace("\u3000", "").strip())

SUMMARY_COLS_N = [_norm(c) for c in SUMMARY_COLS]
SEOUL_SET_N = set(_norm(c) for c in SEOUL_REGIONS)
NATION_SET_N = set(_norm(c) for c in NATION_REGIONS)
TOTAL_N = _norm("총합계"); NATION_N = _norm("전국")

# ------- Sheets read cache (ws.id -> values) -------
_WS_VALUES_CACHE: Dict[int, List[List[str]]] = {}
def _get_all_values_cached(ws: gspread.Worksheet) -> List[List[str]]:
    if ws.id in _WS_VALUES_CACHE:
        return _WS_VALUES_CACHE[ws.id]
    vals = _retry(ws.get_all_values) or []
    _WS_VALUES_CACHE[ws.id] = vals
    return vals

# ===================== gspread 헬퍼 =====================
def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def values_batch_update(ws: gspread.Worksheet, data: List[Dict]):
    body = {"valueInputOption": "USER_ENTERED", "data": data}
    return _retry(ws.spreadsheet.values_batch_update, body=body)

def batch_format(ws: gspread.Worksheet, requests: List[dict]):
    if not requests:
        return
    _retry(ws.spreadsheet.batch_update, {"requests": requests})

def fuzzy_ws(sh: gspread.Spreadsheet, wanted: str) -> Optional[gspread.Worksheet]:
    tgt = _norm(wanted)
    for ws in sh.worksheets():
        if _norm(ws.title) == tgt:
            log(f"[ws] matched: '{ws.title}' (wanted='{wanted}')")
            return ws
    return None

def get_or_create_ws(sh: gspread.Spreadsheet, title: str, rows: int = 100, cols: int = 20) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, title)
    if ws is None:
        ws = _retry(sh.add_worksheet, title=title, rows=rows, cols=cols)
        log(f"[ws] created: {title}")
    return ws

# === 포커스 링크/이름범위 유틸 ===
from urllib.parse import quote

def log_focus_link(ws: gspread.Worksheet, row_idx: int, last_col_index: int, sheet_id: str):
    """
    방금 기록한 행으로 바로 점프하는 링크를 where_written.txt에 남긴다.
    """
    try:
        a1_last = a1_col(last_col_index if last_col_index >= 1 else 1)
        range_a1 = f"{ws.title}!A{row_idx}:{a1_last}{row_idx}"
        link = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={ws.id}&range={quote(range_a1)}"

        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with (LOG_DIR / "where_written.txt").open("a", encoding="utf-8") as f:
            f.write(f"[{ws.title}] wrote row {row_idx} → {range_a1}\n")
            f.write(f"Open here: {link}\n")
    except Exception:
        pass

def upsert_named_range(ws: gspread.Worksheet, name: str, row_idx: int, last_col_index: int):
    """
    선택 사항: 같은 이름으로 최신 행에 이름범위를 갱신해 둔다.
    URL에 range=LATEST_<sheetId> 같이 넣어도 바로 점프 가능.
    """
    try:
        grid_range = {
            "sheetId": ws.id,
            "startRowIndex": row_idx - 1,
            "endRowIndex": row_idx,
            "startColumnIndex": 0,
            "endColumnIndex": max(1, last_col_index),
        }
        # 기존 동일 이름 삭제
        meta = _retry(ws.spreadsheet.fetch_sheet_metadata)
        existing = [nr for nr in meta.get("namedRanges", []) if nr.get("name") == name]
        reqs = []
        for nr in existing:
            reqs.append({"deleteNamedRange": {"namedRangeId": nr["namedRangeId"]}})
        # 새 이름범위 추가
        reqs.append({"addNamedRange": {"namedRange": {"name": name, "range": grid_range}}})
        batch_format(ws, reqs)
    except Exception:
        pass

# ===================== 파일/읽기/집계 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data", dtype=str)
    df = df.fillna("")
    for c in ["계약년", "계약월", "계약일", "거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def eok_series(ser) -> pd.Series:
    s = pd.Series(ser)
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return pd.Series([], dtype=float)
    return s / 10000.0

def round2(v) -> str:
    try:
        return f"{float(v):.2f}"
    except Exception:
        return ""

def agg_all_stats(df: pd.DataFrame):
    counts = {col: 0 for col in SUMMARY_COLS}
    med = {col: "" for col in SUMMARY_COLS}
    mean = {col: "" for col in SUMMARY_COLS}

    counts["전국"] = int(len(df))
    all_eok = eok_series(df.get("거래금액(만원)", []))
    if not all_eok.empty:
        med["전국"] = round2(all_eok.median())
        mean["전국"] = round2(all_eok.mean())

    if "광역" in df.columns:
        for prov, sub in df.groupby("광역"):
            prov = str(prov)
            if prov in counts:
                counts[prov] += int(len(sub))
                se = eok_series(sub.get("거래금액(만원)", []))
                if not se.empty:
                    med[prov] = round2(se.median()); mean[prov] = round2(se.mean())

    seoul = df[df.get("광역", "") == "서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul) > 0:
        se = eok_series(seoul["거래금액(만원)"])
        if not se.empty:
            med["서울"] = round2(se.median()); mean["서울"] = round2(se.mean())
        if "구" in seoul.columns:
            for gu, sub in seoul.groupby("구"):
                gu = str(gu)
                if gu in counts:
                    counts[gu] += int(len(sub))
                    se2 = eok_series(sub["거래금액(만원)"])
                    if not se2.empty:
                        med[gu] = round2(se2.median()); mean[gu] = round2(se2.mean())

    ap = seoul[seoul.get("법정동", "") == "압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap) > 0:
        s = eok_series(ap["거래금액(만원)"])
        if not s.empty:
            med["압구정동"] = round2(s.median()); mean["압구정동"] = round2(s.mean())

    return counts, med, mean

# ===================== 공통 유틸 =====================
def kdate(d: datetime) -> str:
    return f"{d.year}. {d.month}. {d.day}"

def is_holiday(d: date) -> bool:
    return d.strftime("%Y-%m-%d") in KR_HOLIDAYS

YM_RE = re.compile(r"(\d{4})년\s*(\d{1,2})월")
def yymm_from_title(title: str) -> Optional[str]:
    m = YM_RE.search(title)
    if not m: return None
    y, mm = int(m.group(1)), int(m.group(2))
    return f"{str(y % 100).zfill(2)}/{mm}"

def first_data_date(ws: gspread.Worksheet) -> Optional[date]:
    vals = _get_all_values_cached(ws)
    if len(vals) <= 1: return None
    for r in vals[1:]:
        if not r: continue
        s = str(r[0]).strip()
        m = re.search(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})", s)
        if m:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return None

def latest_data_date(ws: gspread.Worksheet) -> Optional[date]:
    vals = _get_all_values_cached(ws)
    for r in reversed(vals[1:]):
        if not r: continue
        s = str(r[0]).strip()
        m = re.search(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})", s)
        if m:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return None

def month_sheet_to_frame(ws: gspread.Worksheet) -> pd.DataFrame:
    vals = _get_all_values_cached(ws)
    if not vals or len(vals) < 2:
        return pd.DataFrame()
    raw_header = [str(h) for h in vals[0]]
    rows = []
    col_map: Dict[int, str] = {}
    for i, h in enumerate(raw_header[1:], start=1):
        if not h: continue
        col_map[i] = _norm(h)

    for r in vals[1:]:
        if not r or not r[0]: continue
        m = re.search(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})", str(r[0]))
        if not m: continue
        d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        row = {"date": d}
        for i, hn in col_map.items():
            try:
                row[hn] = int(float(r[i])) if i < len(r) and str(r[i]).strip() else 0
            except Exception:
                row[hn] = 0
        rows.append(row)
    df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    return df

def daily_increments(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    inc = df.copy()
    for c in inc.columns:
        if c == "date": continue
        inc[c] = inc[c].astype(int)
        inc[c] = inc[c].diff().fillna(inc[c]).astype(int)
        inc[c] = inc[c].clip(lower=0)
    return inc

def is_weekend_or_holiday(d: date) -> bool:
    if d.weekday() >= 5:  # 토(5)·일(6)
        return True
    if d.strftime("%Y-%m-%d") in KR_HOLIDAYS:
        return True
    return False

def weekday_ratio_for_completion(d: date) -> float:
    # 약한 날(토·일·공휴일) ↑, 평일 ↓  — 필요시 조정
    return 1.12 if is_weekend_or_holiday(d) else 0.95

# ===== 일차별 누적 프레임(단조증가화 포함) =====
def build_cum_by_day(ws: gspread.Worksheet, allow_cols: set, horizon: int = 90) -> Tuple[pd.DataFrame, Optional[date]]:
    df = month_sheet_to_frame(ws)
    if df.empty: return pd.DataFrame(), None
    fday = first_data_date(ws)
    if not fday: return pd.DataFrame(), None

    cols = ["date"] + [c for c in df.columns if c != "date" and c in allow_cols]
    if len(cols) <= 1: return pd.DataFrame(), None

    g = df[cols].copy().sort_values("date").reset_index(drop=True)
    for c in cols:
        if c == "date": continue
        g[c] = pd.to_numeric(g[c], errors="coerce").fillna(0).astype(int)
        g[c] = g[c].cummax()  # 누적 단조화 (감소분 제거)

    idx = pd.date_range(fday, fday + pd.Timedelta(days=horizon-1), freq="D")
    g = g.set_index("date").reindex(idx, method="ffill").fillna(0)
    g.index.name = "date"
    g = g.reset_index()
    return g, fday

# ===== 학습 달 판정(성숙 달: >= min_days 경과) =====
def is_mature_month(ws: gspread.Worksheet, min_days: int = 85) -> bool:
    f = first_data_date(ws); l = latest_data_date(ws)
    if not f or not l: return False
    return (l - f).days + 1 >= min_days

# ===== 학습: d일차 전역 median(C90/Cd) + p99 상한 =====
def train_day_ratios(
    sheets: Dict[str, Dict[str, gspread.Worksheet]],
    level: str,                     # "전국" 또는 "서울"
    start_key: Tuple[int,int],
    end_key_exclusive: Tuple[int,int],
    allow_cols: set,
    min_days: int = 85,
    horizon: int = 90,
) -> Tuple[Dict[str, List[float]], Dict[str, List[float]]]:
    """반환:
        ratios_final[region][d] : d일차 전역 median(C90/Cd)
        caps_p99[region][d]     : d일차 배율 p99 (동적 상한)
    """
    def ym_to_key(ym: str) -> Tuple[int,int]:
        yy, mm = ym.split("/")
        return (2000 + int(yy), int(mm))

    pool = {ym: ws for ym, ws in sheets[level].items()
            if ym_to_key(ym) >= start_key and ym_to_key(ym) < end_key_exclusive}

    ratios_by_region_day: Dict[str, List[List[float]]] = {}
    for ym, ws in sorted(pool.items(), key=lambda kv: ym_to_key(kv[0])):
        if not is_mature_month(ws, min_days=min_days):
            continue
        df_cum, _ = build_cum_by_day(ws, allow_cols, horizon=horizon)
        if df_cum.empty: continue

        for col in df_cum.columns:
            if col == "date": continue
            s = pd.to_numeric(df_cum[col], errors="coerce").fillna(0).astype(int)
            c90 = int(s.iloc[horizon-1])
            if c90 <= 0: continue
            day_ratios = [float(c90) / max(int(s.iloc[d-1]), 1) for d in range(1, horizon+1)]

            if col not in ratios_by_region_day:
                ratios_by_region_day[col] = [[] for _ in range(horizon)]
            for d in range(horizon):
                ratios_by_region_day[col][d].append(day_ratios[d])

    ratios_final: Dict[str, List[float]] = {}
    caps_p99: Dict[str, List[float]] = {}
    for region, day_lists in ratios_by_region_day.items():
        med = [0.0]; p99 = [float("inf")]
        for d in range(horizon):
            vals = np.array(day_lists[d], dtype=float)
            med.append(float(np.median(vals)) if vals.size else 2.0)
            p99.append(float(np.quantile(vals, 0.99)) if vals.size else float("inf"))
        ratios_final[region] = med
        caps_p99[region] = p99
    return ratios_final, caps_p99

# ===== 학습: (d, Cd) → (Cd, C90) 페어 수집(국지 배율용) =====
def collect_training_pairs(
    sheets: Dict[str, Dict[str, gspread.Worksheet]],
    level: str,
    start_key: Tuple[int,int],
    end_key_exclusive: Tuple[int,int],
    allow_cols: set,
    min_days: int = 85,
    horizon: int = 90,
) -> Dict[str, Dict[int, List[Tuple[int,int]]]]:
    """
    반환: pairs[region_n][d] = [(Cd, C90), ...]
    성숙달만 사용. 지역별·일차별로 (당일누적, 최종누적) 페어 모음.
    """
    def ym_to_key(ym: str) -> Tuple[int,int]:
        yy, mm = ym.split("/")
        return (2000 + int(yy), int(mm))

    pool = {ym: ws for ym, ws in sheets[level].items()
            if ym_to_key(ym) >= start_key and ym_to_key(ym) < end_key_exclusive}

    out: Dict[str, Dict[int, List[Tuple[int,int]]]] = {}
    for ym, ws in sorted(pool.items(), key=lambda kv: ym_to_key(kv[0])):
        if not is_mature_month(ws, min_days=min_days):
            continue
        df_cum, _ = build_cum_by_day(ws, allow_cols, horizon=horizon)
        if df_cum.empty: continue

        for col in df_cum.columns:
            if col == "date": continue
            s = pd.to_numeric(df_cum[col], errors="coerce").fillna(0).astype(int)
            c90 = int(s.iloc[horizon-1])
            if c90 <= 0: continue
            for d in range(1, horizon+1):
                cd = int(s.iloc[d-1])
                if cd <= 0:  # 0은 비정보성
                    continue
                out.setdefault(col, {}).setdefault(d, []).append((cd, c90))
    return out

def local_ratio_for_observed(
    pairs_for_day: List[Tuple[int,int]],
    observed_cd: int,
    k_neighbors: int = 25,
    width_factor: float = 2.0,
) -> Optional[float]:
    """
    같은 d일차 학습 페어 중 '현재 Cd와 비슷한' 이웃들의 median(C90/Cd)을 반환.
    - 1차: Cd in [obs/width_factor, obs*width_factor]
    - 2차: 최근접 k개
    """
    if not pairs_for_day: return None
    obs = max(1, int(observed_cd))

    lo, hi = int(obs/width_factor), int(obs*width_factor)
    bucket = [(cd, c90) for (cd, c90) in pairs_for_day if lo <= cd <= hi]
    if not bucket:
        bucket = sorted(pairs_for_day, key=lambda t: abs(t[0]-obs))[:k_neighbors]
    if not bucket: return None

    ratios = [float(c90)/max(cd,1) for (cd, c90) in bucket if cd > 0 and c90 > 0]
    if not ratios: return None
    return float(np.median(ratios))

# ===== 예측: 국지 배율 + 전역 fallback + 동적(p99) 상한 + 누적 보장 =====
def predict_by_day_ratio(
    ws: gspread.Worksheet,
    ratios: Dict[str, List[float]],                      # 전역 median(d별)
    allow_cols: set,
    today: date,
    fallback_ratio: float = 2.0,
    horizon: int = 90,
    training_pairs: Optional[Dict[str, Dict[int, List[Tuple[int,int]]]]] = None,
    day_caps: Optional[Dict[str, List[float]]] = None,
) -> Dict[str, int]:
    """
    - 효과 기준일 = today - 1일 (다음날 반영 가정)
    - 먼저 (d, Cd) 근처 학습에서 국지 median(C90/Cd), 없으면 전역 median(d).
    - p99 동적 상한으로 과소/과다 방지(하드 고정 상한 제거).
    - 누적 단조화/관측 이하 금지.
    """
    df_cum, fday = build_cum_by_day(ws, allow_cols, horizon=horizon)
    if df_cum.empty or not fday: return {}

    eff_today = today - timedelta(days=1)
    d = min(max(1, (eff_today - fday).days + 1), horizon)
    dow_factor = weekday_ratio_for_completion(eff_today)

    out: Dict[str, int] = {}
    for col in df_cum.columns:
        if col == "date": continue
        s = pd.to_numeric(df_cum[col], errors="coerce").fillna(0).astype(int)
        obs = int(s.iloc[d-1])

        # 1) 국지 배율
        loc_ratio = None
        if training_pairs is not None:
            loc_ratio = local_ratio_for_observed(training_pairs.get(col, {}).get(d, []), obs)

        # 2) 전역 median(d일차) fallback
        base_ratio = float(ratios.get(col, [0]*(horizon+1))[d]) if loc_ratio is None else float(loc_ratio)

        # 평일/주말 보정
        ratio = max(1.0, base_ratio * dow_factor)

        # 동적 상한: p99 * 1.2 허용
        if day_caps is not None and col in day_caps and d < len(day_caps[col]):
            cap = day_caps[col][d]
            if cap and cap != float("inf"):
                ratio = min(ratio, cap * 1.2)

        pred = int(round(obs * ratio))
        pred = max(pred, obs)   # 예측 누적은 관측 누적보다 작지 않게
        out[col] = pred

    # 총합계는 구성항들의 합보다 작지 않게
    keys = [c for c in df_cum.columns if c != "date" and c != TOTAL_N]
    subtotal = int(sum(int(out.get(k, 0)) for k in keys))
    if TOTAL_N in df_cum.columns:
        out[TOTAL_N] = max(int(df_cum[TOTAL_N].iloc[-1]), subtotal)
    else:
        out[TOTAL_N] = subtotal

    return out

# ===================== 시트/요약 라인 유틸 =====================
def find_or_append_date_row(ws: gspread.Worksheet, date_label: str) -> int:
    vals = _get_all_values_cached(ws)
    if not vals: return 2
    for i, row in enumerate(vals[1:], start=2):
        if (len(row) > 0) and str(row[0]).strip() == date_label:
            return i
    return len(vals) + 1

def write_month_sheet(ws, date_label: str, header: List[str], values_by_colname: Dict[str, int]):
    hmap = {str(h).strip(): idx + 1 for idx, h in enumerate(header) if str(h).strip()}
    row_idx = find_or_append_date_row(ws, date_label)
    sheet_prefix = f"'{ws.title}'!"
    payload = [{"range": f"{sheet_prefix}A{row_idx}", "values": [[date_label]]}]
    for col_name, val in values_by_colname.items():
        if col_name in hmap:
            c = hmap[col_name]
            payload.append({"range": f"{sheet_prefix}{a1_col(c)}{row_idx}", "values": [[val]]})
    for possible in ("총합계", "합계"):
        if possible in hmap:
            v = values_by_colname.get("총합계", None)
            if v is None:
                v = values_by_colname.get("전국", values_by_colname.get("서울특별시", None))
            if v is not None:
                payload.append({"range": f"{sheet_prefix}{a1_col(hmap[possible])}{row_idx}", "values": [[int(v)]]})
            break
    if payload:
        values_batch_update(ws, payload)
        log(f"[ws] {ws.title} -> {date_label} row={row_idx} (wrote {len(payload)} cells incl. date)")
        # 포커스 링크/이름범위 기록
        header_len = len(header or [])
        sheet_id_env = os.environ.get("SHEET_ID", "").strip()
        log_focus_link(ws, row_idx, header_len, sheet_id_env)
        upsert_named_range(ws, f"LATEST_{ws.id}", row_idx, header_len)

def ym_from_filename(fname: str):
    m = re.search(r"(\d{2})(\d{2})_", fname)
    if not m: return None, None, None
    yy, mm = m.group(1), int(m.group(2))
    return f"전국 20{yy}년 {mm}월", f"서울 20{yy}년 {mm}월", f"{yy}/{mm}"

def prev_ym(ym: str) -> str:
    yy, mm = ym.split("/")
    y = int(yy); m = int(mm)
    if m == 1: return f"{y-1}/12"
    return f"{yy}/{m-1}"

def find_summary_row(ws, ym: str, label: str) -> int:
    vals = _get_all_values_cached(ws)
    if not vals: return 2
    for i, row in enumerate(vals[1:], start=2):
        a = str(row[0]).strip() if len(row) > 0 else ""
        b = str(row[1]).strip() if len(row) > 1 else ""
        if a == ym and b == label:
            return i
    return len(vals) + 1

def put_summary_line(ws, row_idx: int, ym: str, label: str, line_map: dict):
    header = _retry(ws.row_values, 1)
    if not header:
        # '서울'을 새로 만들지 않도록 기본 헤더에서 제외
        default_cols = [c for c in SUMMARY_COLS if c != "서울"]
        _retry(ws.update, [["년월", "구분"] + default_cols], "A1")
        header = ["년월", "구분"] + default_cols

    hmap_norm = {_norm(h): i + 1 for i, h in enumerate(header)}
    sheet_prefix = f"'{ws.title}'!"
    payload = [
        {"range": f"{sheet_prefix}A{row_idx}", "values": [[ym]]},
        {"range": f"{sheet_prefix}B{row_idx}", "values": [[label]]},
    ]
    for k_raw, v in (line_map or {}).items():
        k = "서울특별시" if k_raw == "서울" else k_raw
        if _norm(k) in hmap_norm:
            payload.append({"range": f"{sheet_prefix}{a1_col(hmap_norm[_norm(k)])}{row_idx}", "values": [[v]]})
    # 총합계/합계 자동 채움
    for possible in ("총합계", "합계"):
        pn = _norm(possible)
        if pn in hmap_norm:
            vv = (line_map.get("총합계", "") if isinstance(line_map, dict) else "")
            if vv == "":
                vv = line_map.get("전국", line_map.get("서울특별시", ""))
            payload.append({"range": f"{sheet_prefix}{a1_col(hmap_norm[pn])}{row_idx}", "values": [[vv if vv != "" else 0]]})
            break

    if payload:
        values_batch_update(ws, payload)

def color_diff_line(ws, row_idx: int, diff_line: dict, header: List[str]):
    hmap = {h: i + 1 for i, h in enumerate(header)}
    reqs = []
    for k, v in diff_line.items():
        if k not in hmap or v in ("", "0"): continue
        r, g, b = (0.0, 0.35, 1.0) if str(v).startswith("+") else (1.0, 0.0, 0.0)
        reqs.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": row_idx - 1, "endRowIndex": row_idx,
                    "startColumnIndex": hmap[k] - 1, "endColumnIndex": hmap[k]
                },
                "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red": r, "green": g, "blue": b}}}},
                "fields": "userEnteredFormat.textFormat.foregroundColor"
            }
        })
    batch_format(ws, reqs)

def write_month_summary(ws, y: int, m: int, counts: dict, med: dict, mean: dict, prev_counts: Optional[dict]):
    ym = f"{str(y % 100).zfill(2)}/{m}"

    r1 = find_summary_row(ws, ym, "거래건수")
    put_summary_line(ws, r1, ym, "거래건수", counts)
    log(f"[summary] {ym} 거래건수 -> row={r1}")

    r2 = find_summary_row(ws, ym, "중앙값(단위:억)")
    put_summary_line(ws, r2, ym, "중앙값(단위:억)", med)
    log(f"[summary] {ym} 중앙값 -> row={r2}")

    r3 = find_summary_row(ws, ym, "평균가(단위:억)")
    put_summary_line(ws, r3, ym, "평균가(단위:억)", mean)
    log(f"[summary] {ym} 평균가 -> row={r3}")

    if prev_counts:
        diffs = {}
        for c in SUMMARY_COLS:
            key = "서울특별시" if c == "서울" else c
            cur = int(counts.get(key, 0) or 0)
            prv = int(prev_counts.get(key, 0) or 0)
            d = cur - prv
            diffs[key] = f"+{d}" if d > 0 else (str(d) if d < 0 else "0")
    else:
        diffs = {("서울특별시" if c == "서울" else c): "" for c in SUMMARY_COLS}
    r4 = find_summary_row(ws, ym, "전월대비 건수증감")
    put_summary_line(ws, r4, ym, "전월대비 건수증감", diffs)
    header = _retry(ws.row_values, 1)
    color_diff_line(ws, r4, diffs, header)
    log(f"[summary] {ym} 전월대비 -> row={r4}")

    # 포커스 링크/이름범위 기록 (전월대비 라인 기준)
    header_now = _retry(ws.row_values, 1)
    sheet_id_env = os.environ.get("SHEET_ID", "").strip()
    log_focus_link(ws, r4, len(header_now or []), sheet_id_env)
    upsert_named_range(ws, f"LATEST_{ws.id}", r4, len(header_now or []))

def write_predicted_line(ws_sum: gspread.Worksheet, ym: str, pred_map: dict):
    # '서울' 키는 절대 기록하지 않고, '서울특별시'로 통일
    if pred_map and "서울" in pred_map:
        if (pred_map.get("서울특별시") in ("", None)) and (pred_map["서울"] not in ("", None)):
            pred_map["서울특별시"] = pred_map["서울"]
        pred_map.pop("서울", None)

    r = find_summary_row(ws_sum, ym, "예상건수")
    put_summary_line(ws_sum, r, ym, "예상건수", pred_map)
    header = _retry(ws_sum.row_values, 1)
    reqs = [{
        "repeatCell": {
            "range": {
                "sheetId": ws_sum.id,
                "startRowIndex": r-1, "endRowIndex": r,
                "startColumnIndex": 0, "endColumnIndex": len(header or [])
            },
            "cell": {"userEnteredFormat": {
                "textFormat": {"foregroundColor": {"red": 0.0, "green": 0.5, "blue": 0.0}, "bold": True}
            }},
            "fields": "userEnteredFormat.textFormat"
        }
    }]
    batch_format(ws_sum, reqs)
    filled = sum(1 for v in (pred_map or {}).values() if (isinstance(v, int) and v > 0))
    log(f"[summary] {ym} 예상건수 -> row={r} (green bold) filled={filled}")

    # 포커스 링크/이름범위 기록
    header_sum = _retry(ws_sum.row_values, 1)
    sheet_id_env = os.environ.get("SHEET_ID", "").strip()
    log_focus_link(ws_sum, r, len(header_sum or []), sheet_id_env)
    upsert_named_range(ws_sum, f"LATEST_{ws_sum.id}", r, len(header_sum or []))

# ===================== 압구정동 원본/변동 기록 =====================
APGU_BASE_COLS = [
    "광역", "구", "법정동", "리", "번지", "본번", "부번", "단지명", "전용면적(㎡)",
    "계약년", "계약월", "계약일", "거래금액(만원)", "동", "층",
    "매수자", "매도자", "건축년도", "도로명", "해제사유발생일", "거래유형",
    "중개사소재지", "등기일자", "주택유형"
]

def _apgu_norm(v) -> str: return "" if v is None else str(v).strip()

def _apgu_key_from_row_values(values: List[str], header: List[str]) -> str:
    idx = {h: i for i, h in enumerate(header)}
    parts = []
    for h in APGU_BASE_COLS:
        i = idx.get(h, None)
        parts.append(_apgu_norm(values[i] if (i is not None and i < len(values)) else ""))
    return "|".join(parts)

def _ensure_rows(ws: gspread.Worksheet, need_end_row: int):
    if need_end_row > ws.row_count:
        _retry(ws.add_rows, need_end_row - ws.row_count)

def fmt_kdate(d: date) -> str: return f"{d.year}. {d.month}. {d.day}"

def upsert_apgu_verbatim(ws: gspread.Worksheet, df_all: pd.DataFrame, run_day: date):
    """
    요구사항 반영(append-only):
    - base(원본) 영역은 덮어쓰지 않고, 신규/변경분만 append
    - 변경이력(history) 영역은 기존 이력을 지우지 않고, 신규/삭제 이벤트만 append
    - 삭제는 base에서 제거하지 않음(원본 보존), 대신 history에 (삭제)로만 남김
    """
    df = df_all[(df_all.get("광역", "") == "서울특별시") & (df_all.get("법정동", "") == "압구정동")].copy()
    if df.empty:
        log("[압구정동] no rows")
        return

    # 컬럼 보정
    for c in APGU_BASE_COLS:
        if c not in df.columns:
            df[c] = ""

    # 정렬(원본 보기 좋게)
    df = df.sort_values(["계약년", "계약월", "계약일"], ascending=[True, True, True], kind="mergesort")

    # 캐시 무효화
    _WS_VALUES_CACHE.pop(ws.id, None)

    # 시트 전체 읽기
    vals = _retry(ws.get_all_values) or []

    # 헤더 보장
    if not vals:
        _retry(ws.update, [APGU_BASE_COLS], "A1")
        vals = [APGU_BASE_COLS]

    if vals[0] != APGU_BASE_COLS:
        _retry(ws.update, [APGU_BASE_COLS], "A1")
        vals[0] = APGU_BASE_COLS

    header = APGU_BASE_COLS
    body = vals[1:]

    # trailing empty rows 제거(판정 안정화)
    def _is_empty_row(r: List[str]) -> bool:
        return (not r) or all(_apgu_norm(x) == "" for x in r)

    while body and _is_empty_row(body[-1]):
        body.pop()

    # base / history 분리
    # history 시작점: 첫 컬럼이 "변경구분"인 행
    change_header = ["변경구분", "변경일"] + APGU_BASE_COLS
    hist_start_idx = None  # sheet row index (1-based)
    for i, r in enumerate(body, start=2):
        if r and _apgu_norm(r[0]) == _apgu_norm("변경구분"):
            hist_start_idx = i
            break

    if hist_start_idx is None:
        base_rows_old = [ (r + [""]*len(header))[:len(header)] for r in body if not _is_empty_row(r) ]
        hist_rows_old = []  # (변경구분 헤더 포함) 없음
    else:
        base_part = body[:hist_start_idx-2]  # row2 ~ hist_start_idx-1
        hist_part = body[hist_start_idx-2:]  # hist_start_idx ~ end (헤더 포함)
        base_rows_old = [ (r + [""]*len(header))[:len(header)] for r in base_part if not _is_empty_row(r) ]
        hist_rows_old = hist_part[:]  # 그대로 유지

    # 오늘 수집분(base 후보) 구성
    base_rows_new = [[_apgu_norm(row.get(c, "")) for c in APGU_BASE_COLS] for _, row in df.iterrows()]

    # 키 셋
    old_keys = {_apgu_key_from_row_values(r, header) for r in base_rows_old}
    new_keys = {_apgu_key_from_row_values(r, header) for r in base_rows_new}

    added_keys = sorted(list(new_keys - old_keys))
    removed_keys = sorted(list(old_keys - new_keys))

    # base는 "추가만" 한다 (원본 보존)
    new_map = {_apgu_key_from_row_values(r, header): r for r in base_rows_new}
    base_append = [new_map[k] for k in added_keys]
    base_rows_updated = base_rows_old + base_append

    # history는 "기존 유지 + 신규 append"
    # 기존 history body 추출(헤더 제거)
    hist_body_old = []
    if hist_rows_old:
        # 첫 행이 change_header인지 확인(아니면 강제로 헤더 재작성)
        if hist_rows_old[0] and _apgu_norm(hist_rows_old[0][0]) == _apgu_norm("변경구분"):
            hist_body_old = [ (r + [""]*len(change_header))[:len(change_header)]
                              for r in hist_rows_old[1:] if not _is_empty_row(r) ]

    today_str = fmt_kdate(run_day)

    # removed 행의 원본 값은 base_old에서 가져와 기록
    old_map = {_apgu_key_from_row_values(r, header): r for r in base_rows_old}

    hist_append_rows = []
    for k in added_keys:
        hist_append_rows.append(["(신규)", today_str] + new_map[k])
    for k in removed_keys:
        hist_append_rows.append(["(삭제)", today_str] + old_map[k])

    # 최종 시트 내용 재구성(전체를 다시 써서 base가 늘어나도 history 보존)
    out = []
    out.append(APGU_BASE_COLS)
    out.extend(base_rows_updated)

    # history가 하나라도 존재/추가되면 구간 생성
    if hist_body_old or hist_append_rows or hist_start_idx is not None:
        out.append([""] * len(APGU_BASE_COLS))  # 구분용 빈 줄(선택)
        out.append(change_header)
        out.extend(hist_body_old)
        out.extend(hist_append_rows)

    # 시트에 쓰기
    max_cols = max(len(APGU_BASE_COLS), len(change_header))
    end_row = len(out)
    end_col = a1_col(max_cols)

    _ensure_rows(ws, end_row)

    _retry(ws.update, out, f"A1:{end_col}{end_row}")

    # 이전 내용이 더 길었으면 잔여 영역 clear
    prev_total_rows = len(vals)
    if prev_total_rows > end_row:
        blanks = [[""] * max_cols for _ in range(prev_total_rows - end_row)]
        _retry(ws.update, blanks, f"A{end_row+1}:{end_col}{prev_total_rows}")

    # 색상 포맷팅(이번에 append된 history 부분만)
    # history 신규 시작 행 계산:
    # row1: base header
    # row2~: base_rows_updated
    # sep row: 1 + 1 + len(base_rows_updated)
    # hist header row: sep+1
    # hist body old: 다음부터 len(hist_body_old)
    # append start: 그 다음
    if hist_append_rows:
        sep_row = 1 + 1 + len(base_rows_updated)
        hist_header_row = sep_row + 1
        append_start_row = hist_header_row + 1 + len(hist_body_old)

        reqs = []

        # (신규) 블록
        if len(added_keys) > 0:
            reqs.append({
                "repeatCell": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": append_start_row - 1,
                        "endRowIndex": append_start_row - 1 + len(added_keys),
                        "startColumnIndex": 0,
                        "endColumnIndex": len(change_header),
                    },
                    "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red": 0.0, "green": 0.0, "blue": 1.0}}}},
                    "fields": "userEnteredFormat.textFormat.foregroundColor",
                }
            })

        # (삭제) 블록
        if len(removed_keys) > 0:
            del_start = append_start_row + len(added_keys)
            reqs.append({
                "repeatCell": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": del_start - 1,
                        "endRowIndex": del_start - 1 + len(removed_keys),
                        "startColumnIndex": 0,
                        "endColumnIndex": len(change_header),
                    },
                    "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red": 1.0, "green": 0.0, "blue": 0.0}}}},
                    "fields": "userEnteredFormat.textFormat.foregroundColor",
                }
            })

        batch_format(ws, reqs)

    log(f"[압구정동] base append={len(base_append)} / history append 신규={len(added_keys)} 삭제={len(removed_keys)}")


# ===== 패턴 분석 시트 (표 + 라인차트) =====
def render_pattern_analysis(sh: gspread.Spreadsheet, month_title: str, df_cum: pd.DataFrame, df_inc: pd.DataFrame, targets: List[str]):
    ws = get_or_create_ws(sh, "거래량 패턴분석", rows=1000, cols=40)
    _retry(ws.clear)
    header1 = ["[누적] 날짜"] + targets; header2 = ["[일증분] 날짜"] + targets
    cum_rows = [header1]; inc_rows = [header2]
    for i in range(len(df_cum)):
        d = df_cum.iloc[i]["date"]
        row = [d.strftime("%Y-%m-%d")] + [int(df_cum.iloc[i].get(_norm(t), 0)) for t in targets]
        cum_rows.append(row)
    for i in range(len(df_inc)):
        d = df_inc.iloc[i]["date"]
        row = [d.strftime("%Y-%m-%d")] + [int(df_inc.iloc[i].get(_norm(t), 0)) for t in targets]
        inc_rows.append(row)
    _retry(ws.update, cum_rows, "A1")
    start_inc = len(cum_rows) + 2
    _retry(ws.update, inc_rows, f"A{start_inc}")
    nrows = len(cum_rows)
    series = []
    for j in range(len(targets)):
        series.append({"series":{"sourceRange":{"sources":[{
            "sheetId":ws.id,"startRowIndex":1,"endRowIndex":nrows,"startColumnIndex":1+j,"endColumnIndex":2+j
        }]}}, "targetAxis":"LEFT_AXIS"})
    add_chart = {"addChart":{"chart":{"spec":{"title":f"{month_title} - 누적 등록 추이","basicChart":{
        "chartType":"LINE","legendPosition":"BOTTOM_LEGEND",
        "axis":[{"position":"BOTTOM_AXIS","title":"Date"},{"position":"LEFT_AXIS","title":"Cumulative"}],
        "domains":[{"domain":{"sourceRange":{"sources":[{
            "sheetId":ws.id,"startRowIndex":1,"endRowIndex":nrows,"startColumnIndex":0,"endColumnIndex":1
        }]}}}],"series":series,"headerCount":1}},
        "position":{"overlayPosition":{"anchorCell":{"sheetId":ws.id,"rowIndex":0,"columnIndex":10},
        "offsetXPixels":0,"offsetYPixels":0,"widthPixels":800,"heightPixels":320}}}}}
    _retry(ws.spreadsheet.batch_update, {"requests":[add_chart]})
    log("[pattern] 분석 탭 작성 및 차트 추가")

# ===== 월 시트 누적으로 거래건수 읽기 =====
def read_counts_from_month_sheet(ws: gspread.Worksheet) -> Dict[str, int]:
    df_cum = month_sheet_to_frame(ws)
    if df_cum.empty: return {}
    last = df_cum.iloc[-1]
    out: Dict[str, int] = {}
    for col in list(df_cum.columns):
        if col == "date": continue
        try:
            out[col] = int(float(last[col] or 0))
        except Exception:
            out[col] = 0
    if TOTAL_N in last.index:
        try:
            out[NATION_N] = int(float(last[TOTAL_N] or out.get(NATION_N, 0)))
        except Exception:
            pass
    return out

# ===== 무자료 달 판정 =====
def is_no_data_month(df: pd.DataFrame) -> bool:
    if df is None or df.empty: return True
    need = {"계약년", "계약월"}
    if not need.issubset(set(df.columns)): return True
    yy = pd.to_numeric(df["계약년"], errors="coerce")
    mm = pd.to_numeric(df["계약월"], errors="coerce")
    return (yy.dropna().empty or mm.dropna().empty)

# ===== 시트 목록/탭 탐색 =====
def list_month_sheets(sh: gspread.Spreadsheet):
    out = {"전국": {}, "서울": {}}
    for ws in sh.worksheets():
        t = ws.title.strip()
        if t.startswith("전국 ") and YM_RE.search(t):
            ym = yymm_from_title(t); out["전국"][ym] = ws
        elif t.startswith("서울 ") and YM_RE.search(t):
            ym = yymm_from_title(t); out["서울"][ym] = ws
    return out

# ===== 모멘텀(선택: 총합 소폭 보정) =====
def momentum_factor_from_increments(df_cum: pd.DataFrame, region_col_n: str, day_idx_for_window: int, k: int = 3, gamma: float = 0.20) -> float:
    if region_col_n not in df_cum.columns: return 1.0
    s = pd.to_numeric(df_cum[region_col_n], errors="coerce").fillna(0).astype(int)
    inc = s.diff().fillna(s).clip(lower=0)
    end = max(1, min(day_idx_for_window, len(inc)))
    start_recent = max(1, end - k + 1)
    start_prev   = max(1, start_recent - k)
    recent_sum = float(inc.iloc[start_recent-1:end].sum()) if end >= start_recent else 0.0
    prev_sum   = float(inc.iloc[start_prev-1:start_recent-1].sum()) if start_recent-1 >= start_prev else 0.0
    momentum = (recent_sum - prev_sum) / max(prev_sum, 1.0)
    factor = 1.0 + gamma * momentum
    return float(max(0.85, min(1.15, factor)))  # ±15%

# ===================== 메인 =====================
def main():
    try:
        RUN_LOG.write_text("", encoding="utf-8")
    except Exception:
        pass

    log("[MAIN]"); log(f"artifacts_dir={ARTIFACTS_DIR}")

    # 인증
    sa_json = os.environ.get("SA_JSON", "").strip()
    sa_path = os.environ.get("SA_PATH", "sa.json")
    if sa_json:
        creds = Credentials.from_service_account_info(
            json.loads(sa_json),
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
        )
    else:
        if not Path(sa_path).exists():
            raise RuntimeError("service account not provided")
        creds = Credentials.from_service_account_file(
            sa_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
        )
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, os.environ.get("SHEET_ID", "").strip())
    log("[gspread] spreadsheet opened")

    # 파일 수집 및 전국/서울 시트 기록(원래 로직 유지)
    files = sorted(Path(ARTIFACTS_DIR).rglob("전국 *.xlsx"))
    log(f"[collect] found {len(files)} xlsx files")

    month_cache = {}  # ym -> {counts, med, mean}
    today_label = kdate(datetime.now())
    run_day = datetime.now().date()
    apgu_all: List[pd.DataFrame] = []

    for p in files:
        nat_title, se_title, ym = ym_from_filename(p.name)
        if not ym: continue
        log(f"[file] {p.name} -> nat='{nat_title}' / seoul='{se_title}' / ym={ym}")

        df = read_month_df(p); log(f"[read] rows={len(df)} cols={len(df.columns)}")

        if is_no_data_month(df):
            log(f"[no-data] {p.name} -> 0건으로 기록")
            zero_counts = {c: 0 for c in SUMMARY_COLS}
            empty_med   = {c: "" for c in SUMMARY_COLS}
            empty_mean  = {c: "" for c in SUMMARY_COLS}
            month_cache[ym] = {"counts": zero_counts, "med": empty_med, "mean": empty_mean}

            ws_nat = fuzzy_ws(sh, nat_title)
            if ws_nat:
                header_nat = _retry(ws_nat.row_values, 1)
                values_nat = {}
                for h in header_nat:
                    if not h or h == "날짜": continue
                    if h == "총합계": values_nat["총합계"] = 0
                    elif h in zero_counts: values_nat[h] = 0
                write_month_sheet(ws_nat, today_label, header_nat, values_nat)

            ws_se = fuzzy_ws(sh, se_title)
            if ws_se:
                header_se = _retry(ws_se.row_values, 1)
                values_se = {}
                for h in header_se:
                    if not h or h == "날짜": continue
                    if h == "총합계": values_se["총합계"] = 0
                    elif h in zero_counts: values_se[h] = 0
                write_month_sheet(ws_se, today_label, header_se, values_se)
            continue

        counts, med, mean = agg_all_stats(df)
        month_cache[ym] = {"counts": counts, "med": med, "mean": mean}

        ws_nat = fuzzy_ws(sh, nat_title)
        if ws_nat:
            header_nat = _retry(ws_nat.row_values, 1)
            values_nat: Dict[str, int] = {}
            for h in header_nat:
                if not h or h == "날짜": continue
                if h == "총합계": values_nat["총합계"] = int(counts.get("전국", 0))
                elif h in counts: values_nat[h] = int(counts[h])
            write_month_sheet(ws_nat, today_label, header_nat, values_nat)

        ws_se = fuzzy_ws(sh, se_title)
        if ws_se:
            header_se = _retry(ws_se.row_values, 1)
            values_se: Dict[str, int] = {}
            for h in header_se:
                if not h or h == "날짜": continue
                if h == "총합계": values_se["총합계"] = int(counts.get("서울", 0))
                elif h in counts: values_se[h] = int(counts[h])
            write_month_sheet(ws_se, today_label, header_se, values_se)

        ap = df[(df.get("광역", "") == "서울특별시") & (df.get("법정동", "") == "압구정동")].copy()
        if not ap.empty: apgu_all.append(ap)

    # 거래요약: 원본 집계 기록
    ws_sum = fuzzy_ws(sh, SUMMARY_SHEET_NAME)
    if ws_sum:
        if month_cache:
            def ym_key(ym): yy, mm = ym.split("/"); return (int(yy), int(mm))
            for ym in sorted(month_cache.keys(), key=ym_key):
                cur = month_cache[ym]; prv = month_cache.get(prev_ym(ym))
                write_month_summary(ws_sum, 2000 + int(ym.split("/")[0]), int(ym.split("/")[1]),
                                    cur["counts"], cur["med"], cur["mean"],
                                    prv["counts"] if prv else None)
                time.sleep(0.15)

        # 최근 12개월 보정 기록(월 시트 누적)
        month_sheets = [ws for ws in sh.worksheets() if YM_RE.search(ws.title)]
        ym_ws: Dict[str, gspread.Worksheet] = {}
        for ws in month_sheets:
            ym = yymm_from_title(ws.title)
            if ym: ym_ws[ym] = ws

        today = datetime.now().date()
        def ym_to_yM(ym: str) -> Tuple[int,int]:
            a, b = ym.split("/"); return 2000 + int(a), int(b)
        def yM_to_ym(y: int, m: int) -> str:
            return f"{str(y % 100).zfill(2)}/{m}"
        def add_months(y, m, delta):
            nm = m + delta; y += (nm - 1) // 12; nm = ((nm - 1) % 12) + 1; return y, nm

        cur_y, cur_m = today.year, today.month
        last12_yM = [add_months(cur_y, cur_m, -i) for i in range(0, 12)]
        last12_ym = [yM_to_ym(y, m) for (y, m) in last12_yM]

        for ym in last12_ym:
            counts = None; med = None; mean = None; prv_counts = None

            if ym in month_cache:
                counts = month_cache[ym]["counts"]; med = month_cache[ym]["med"]; mean = month_cache[ym]["mean"]
                prv = month_cache.get(prev_ym(ym)); prv_counts = prv["counts"] if prv else None
            else:
                ws_nat = None; ws_se = None
                for w in sh.worksheets():
                    if re.search(r"^전국\s+\d{4}년\s+\d{1,2}월$", w.title) and yymm_from_title(w.title) == ym: ws_nat = w
                    if re.search(r"^서울\s+\d{4}년\s+\d{1,2}월$", w.title) and yymm_from_title(w.title) == ym: ws_se = w
                c_nat = read_counts_from_month_sheet(ws_nat) if ws_nat else {}
                c_se  = read_counts_from_month_sheet(ws_se) if ws_se else {}
                if c_nat or c_se:
                    merged_n = {k: c_nat.get(k, 0) for k in c_nat.keys()}
                    for k, v in c_se.items(): merged_n[k] = v
                    counts = {}
                    for k_n, v in merged_n.items():
                        human = next((orig for orig in SUMMARY_COLS if _norm(orig) == k_n), k_n)
                        if human == "서울": human = "서울특별시"
                        counts[human] = v

                    ym_prev = prev_ym(ym)
                    ws_nat_prev = None; ws_se_prev = None
                    for w in sh.worksheets():
                        if re.search(r"^전국\s+\d{4}년\s+\d{1,2}월$", w.title) and yymm_from_title(w.title) == ym_prev: ws_nat_prev = w
                        if re.search(r"^서울\s+\d{4}년\s+\d{1,2}월$", w.title) and yymm_from_title(w.title) == ym_prev: ws_se_prev = w
                    if ws_nat_prev or ws_se_prev:
                        c_nat_prev = read_counts_from_month_sheet(ws_nat_prev) if ws_nat_prev else {}
                        c_se_prev  = read_counts_from_month_sheet(ws_se_prev) if ws_se_prev else {}
                        prv_merged = {k: c_nat_prev.get(k, 0) for k in c_nat_prev.keys()}
                        for k, v in c_se_prev.items(): prv_merged[k] = v
                        prv_counts = {}
                        for k_n, v in prv_merged.items():
                            human = next((orig for orig in SUMMARY_COLS if _norm(orig) == k_n), k_n)
                            if human == "서울": human = "서울특별시"
                            prv_counts[human] = v

            if counts:
                r_cnt = find_summary_row(ws_sum, ym, "거래건수")
                put_summary_line(ws_sum, r_cnt, ym, "거래건수", counts)
                if med:
                    r_med = find_summary_row(ws_sum, ym, "중앙값(단위:억)"); put_summary_line(ws_sum, r_med, ym, "중앙값(단위:억)", med)
                if mean:
                    r_mean = find_summary_row(ws_sum, ym, "평균가(단위:억)"); put_summary_line(ws_sum, r_mean, ym, "평균가(단위:억)", mean)
                if prv_counts:
                    diffs = {}
                    for c in SUMMARY_COLS:
                        key = "서울특별시" if c == "서울" else c
                        cur = int(counts.get(key, 0) or 0)
                        prv = int(prv_counts.get(key, 0) or 0)
                        d = cur - prv
                        diffs[key] = f"+{d}" if d > 0 else (str(d) if d < 0 else "0")
                else:
                    diffs = {("서울특별시" if c == "서울" else c): "" for c in SUMMARY_COLS}
                r_diff = find_summary_row(ws_sum, ym, "전월대비 건수증감")
                put_summary_line(ws_sum, r_diff, ym, "전월대비 건수증감", diffs)
                header_sum = _retry(ws_sum.row_values, 1)
                color_diff_line(ws_sum, r_diff, diffs, header_sum)

    # 압구정동 원본 기록
    if apgu_all:
        ws_ap = fuzzy_ws(sh, "압구정동")
        if ws_ap:
            all_df = pd.concat(apgu_all, ignore_index=True)
            upsert_apgu_verbatim(ws_ap, all_df, run_day)
        else:
            log("[압구정동] sheet not found (skip)")

    # ===== 예측(현재월 포함 최근 3개월) =====
    if ws_sum:
        sheets = list_month_sheets(sh)
        if not sheets["서울"] and not sheets["전국"]:
            log("[predict] no month sheets found"); return

        today = datetime.now().date()

        def add_months(y, m, delta):
            nm = m + delta; y += (nm - 1) // 12; nm = ((nm - 1) % 12) + 1; return y, nm
        def key_to_ym(y, m): return f"{str(y % 100).zfill(2)}/{m}"

        START_KEY = (2024, 10)
        cur_y, cur_m = today.year, today.month
        end_y, end_m = add_months(cur_y, cur_m, -3)          # 오늘-3개월(포함)
        END_KEY_EXCL = add_months(end_y, end_m, +1)          # exclusive

        allow_seoul = SEOUL_SET_N | {TOTAL_N}
        allow_nat   = NATION_SET_N | {TOTAL_N}

        # 학습(전역 median, p99 상한, 국지 페어)
        ratios_nat,  caps_nat  = train_day_ratios(sheets, "전국", START_KEY, END_KEY_EXCL, allow_nat,  min_days=85, horizon=90)
        ratios_seou, caps_seou = train_day_ratios(sheets, "서울", START_KEY, END_KEY_EXCL, allow_seoul, min_days=85, horizon=90)
        pairs_nat   = collect_training_pairs(sheets, "전국", START_KEY, END_KEY_EXCL, allow_nat,  min_days=85, horizon=90)
        pairs_seou  = collect_training_pairs(sheets, "서울", START_KEY, END_KEY_EXCL, allow_seoul, min_days=85, horizon=90)

        targets = [key_to_ym(*add_months(cur_y, cur_m, -i)) for i in range(0, 3)]
        targets = [ym for ym in targets if ym in sheets["전국"] or ym in sheets["서울"]]
        targets = sorted(set(targets), key=lambda ym: (2000 + int(ym.split("/")[0]), int(ym.split("/")[1])))
        log(f"[predict] targets={targets}")

        for ym in targets:
            merged_pred: Dict[str, Union[int, str]] = {col: "" for col in SUMMARY_COLS}

            # 전국 탭 예측
            ws_nat = sheets["전국"].get(ym)
            if ws_nat:
                pred_nat_n  = predict_by_day_ratio(ws_nat, ratios_nat,  allow_nat,  today,
                                                   fallback_ratio=2.0, horizon=90,
                                                   training_pairs=pairs_nat, day_caps=caps_nat)
                # 모멘텀(선택)
                df_cum_nat, fday_nat = build_cum_by_day(ws_nat, allow_nat, horizon=90)
                if not df_cum_nat.empty and fday_nat and TOTAL_N in df_cum_nat.columns:
                    mf = momentum_factor_from_increments(df_cum_nat, TOTAL_N, 1, k=3, gamma=0.20)
                    pred_nat_n[TOTAL_N] = int(round(pred_nat_n[TOTAL_N] * mf))

                for k_n, v in pred_nat_n.items():
                    human = next((orig for orig in SUMMARY_COLS if _norm(orig) == k_n), k_n)
                    if human == "서울": human = "서울특별시"
                    merged_pred[human] = int(v)
                if "전국" in SUMMARY_COLS and TOTAL_N in pred_nat_n:
                    merged_pred["전국"] = int(pred_nat_n[TOTAL_N])

            # 서울 탭 예측
            ws_se  = sheets["서울"].get(ym)
            if ws_se:
                pred_seoul_n = predict_by_day_ratio(ws_se,  ratios_seou, allow_seoul, today,
                                                    fallback_ratio=2.0, horizon=90,
                                                    training_pairs=pairs_seou, day_caps=caps_seou)
                df_cum_se, fday_se = build_cum_by_day(ws_se, allow_seoul, horizon=90)
                if not df_cum_se.empty and fday_se:
                    base_col = TOTAL_N if TOTAL_N in df_cum_se.columns else next((c for c in df_cum_se.columns if c != "date"), None)
                    if base_col:
                        mf = momentum_factor_from_increments(df_cum_se, base_col, 1, k=3, gamma=0.20)
                        if TOTAL_N in pred_seoul_n:
                            pred_seoul_n[TOTAL_N] = int(round(pred_seoul_n[TOTAL_N] * mf))

                for k_n, v in pred_seoul_n.items():
                    human = next((orig for orig in SUMMARY_COLS if _norm(orig) == k_n), k_n)
                    if human == "서울": human = "서울특별시"
                    merged_pred[human] = int(v)
                if "서울특별시" in SUMMARY_COLS and TOTAL_N in pred_seoul_n:
                    merged_pred["서울특별시"] = int(pred_seoul_n[TOTAL_N])

            write_predicted_line(ws_sum, ym, merged_pred)

        # 패턴 분석 탭
        if targets:
            latest_ym = targets[-1]
            ws_latest = sheets["서울"].get(latest_ym) or sheets["전국"].get(latest_ym)
            if ws_latest:
                df_cum = month_sheet_to_frame(ws_latest)
                df_inc = daily_increments(df_cum)
                targets_cols: List[str] = []
                if _norm("총합계") in df_cum.columns: targets_cols.append("총합계")
                for t in ["서울특별시", "강남구", "압구정동"]:
                    if _norm(t) in df_cum.columns and t not in targets_cols:
                        targets_cols.append(t)
                if not targets_cols:
                    targets_cols = [c for c in df_cum.columns if c != "date"][:3]
                render_pattern_analysis(sh, ws_latest.title, df_cum, df_inc, targets_cols[:3])

    log("[main] done")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {repr(e)}")
        raise
