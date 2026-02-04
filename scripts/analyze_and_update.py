# -*- coding: utf-8 -*-
from __future__ import annotations

"""
전체본(수정 완료본)

핵심 수정(183 bytes / 기록 안됨 문제 해결):
A) ✅ main()이 pass로 끝나던 “무동작 파일”이 아니라, 실제 전체 로직이 포함된 실행본
B) ✅ 날짜 라벨 비교를 "문자열 완전일치"가 아니라 "날짜 파싱 후 동일 날짜"로 비교
   -> '2026. 2. 3' / '2026.2.3' / '2026-02-03' / 공백 유무가 달라도 같은 날짜로 인식
C) ✅ month_sheet_to_frame / first_data_date / latest_data_date 도 동일 파서 사용
"""

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
def _throttle(sec=0.60):
    import time as _t
    global _LAST
    now = _t.time()
    if now - _LAST < sec:
        _t.sleep(sec - (now - _LAST))
    _LAST = _t.time()

def _retry(fn, *a, **kw):
    """gspread 호출 리트라이 래퍼.

    - 429/5xx 뿐 아니라, 가끔 발생하는 400 'Precondition check failed'도
      (동시 수정/서버측 상태 전파 지연으로) 일시 오류로 보고 재시도합니다.
    """
    base = 0.8
    for i in range(9):
        try:
            _throttle()
            return fn(*a, **kw)
        except APIError as e:
            s = str(e)
            # 일시적 오류로 간주할 케이스
            transient = any(x in s for x in ("429", "500", "502", "503"))
            transient = transient or ("Precondition check failed" in s) or ("precondition" in s.lower())
            if transient:
                time.sleep(base * (2 ** i) + random.uniform(0, 0.35))
                continue
            raise

# ===================== 이름/정규화/캐시 =====================
def _norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "").replace("\u3000", "").strip())

SUMMARY_COLS_N = [_norm(c) for c in SUMMARY_COLS]
SEOUL_SET_N = set(_norm(c) for c in SEOUL_REGIONS)
NATION_SET_N = set(_norm(c) for c in NATION_REGIONS)
TOTAL_N = _norm("총합계")
NATION_N = _norm("전국")

_WS_VALUES_CACHE: Dict[int, List[List[str]]] = {}

def _invalidate_cache(ws: Optional[gspread.Worksheet]):
    try:
        if ws is not None:
            _WS_VALUES_CACHE.pop(ws.id, None)
    except Exception:
        pass

def _get_all_values_cached(ws: gspread.Worksheet) -> List[List[str]]:
    if ws.id in _WS_VALUES_CACHE:
        return _WS_VALUES_CACHE[ws.id]
    vals = _retry(ws.get_all_values) or []
    _WS_VALUES_CACHE[ws.id] = vals
    return vals

# ===================== gspread “쓰기” 래퍼(캐시 무효화 보장) =====================
def ws_update(ws: gspread.Worksheet, values, range_name: str):
    resp = _retry(ws.update, values, range_name)
    _invalidate_cache(ws)
    return resp

def ws_clear(ws: gspread.Worksheet):
    resp = _retry(ws.clear)
    _invalidate_cache(ws)
    return resp

def ws_batch_clear(ws: gspread.Worksheet, ranges: List[str]):
    resp = _retry(ws.batch_clear, ranges)
    _invalidate_cache(ws)
    return resp

def ws_add_rows(ws: gspread.Worksheet, n: int):
    resp = _retry(ws.add_rows, n)
    _invalidate_cache(ws)
    return resp

# ===================== gspread 헬퍼 =====================
def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def ws_add_cols(ws: gspread.Worksheet, n: int):
    resp = _retry(ws.add_cols, n)
    _invalidate_cache(ws)
    return resp

def ensure_grid(ws: gspread.Worksheet, min_rows: int, min_cols: int):
    """Ensure worksheet has at least min_rows/min_cols grid size before writing ranges."""
    try:
        if min_rows and ws.row_count < min_rows:
            ws_add_rows(ws, int(min_rows - ws.row_count))
        if min_cols and ws.col_count < min_cols:
            ws_add_cols(ws, int(min_cols - ws.col_count))
    except Exception:
        pass


def values_batch_update(ws: gspread.Worksheet, data: List[Dict]):
    body = {"valueInputOption": "USER_ENTERED", "data": data}
    resp = _retry(ws.spreadsheet.values_batch_update, body=body)
    _invalidate_cache(ws)
    return resp

def batch_format(ws: gspread.Worksheet, requests: List[dict]):
    if not requests:
        return
    return _retry(ws.spreadsheet.batch_update, {"requests": requests})

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
    try:
        grid_range = {
            "sheetId": ws.id,
            "startRowIndex": row_idx - 1,
            "endRowIndex": row_idx,
            "startColumnIndex": 0,
            "endColumnIndex": max(1, last_col_index),
        }
        meta = _retry(ws.spreadsheet.fetch_sheet_metadata)
        existing = [nr for nr in meta.get("namedRanges", []) if nr.get("name") == name]
        reqs = []
        for nr in existing:
            reqs.append({"deleteNamedRange": {"namedRangeId": nr["namedRangeId"]}})
        reqs.append({"addNamedRange": {"namedRange": {"name": name, "range": grid_range}}})
        batch_format(ws, reqs)
    except Exception:
        pass

# ===================== 날짜/년월 정규화(중요) =====================
YM_RE = re.compile(r"(\d{4})년\s*(\d{1,2})월")

def parse_any_date(s: str) -> Optional[date]:
    """'YYYY. M. D' / 'YYYY.M.D' / 'YYYY-02-03' 모두 date로 파싱."""
    if not s:
        return None
    s2 = str(s).strip()
    m = re.search(r"(\d{4})\s*\.\s*(\d{1,2})\s*\.\s*(\d{1,2})", s2)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            return None
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", s2)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            return None
    return None

def ym_norm(ym: str) -> Optional[str]:
    s = str(ym or "").strip()
    if not s:
        return None

    m = re.search(r"\b(20\d{2})\s*[/\-.]\s*(\d{1,2})\b", s)
    if m:
        y = int(m.group(1)); mm = int(m.group(2))
        if 1 <= mm <= 12:
            return f"{y%100:02d}/{mm:02d}"

    m = re.search(r"\b(\d{2})\s*[/\-.]\s*(\d{1,2})\b", s)
    if m:
        yy = int(m.group(1)); mm = int(m.group(2))
        if 1 <= mm <= 12:
            return f"{yy:02d}/{mm:02d}"

    m = YM_RE.search(s)
    if m:
        y = int(m.group(1)); mm = int(m.group(2))
        if 1 <= mm <= 12:
            return f"{y%100:02d}/{mm:02d}"

    return None

def yymm_from_title(title: str) -> Optional[str]:
    m = YM_RE.search(title or "")
    if not m:
        return None
    y, mm = int(m.group(1)), int(m.group(2))
    if not (1 <= mm <= 12):
        return None
    return f"{y%100:02d}/{mm:02d}"

def prev_ym(ym: str) -> str:
    ym2 = ym_norm(ym) or ym
    yy, mm = ym2.split("/")
    y = int(yy); m = int(mm)
    if m == 1:
        return f"{(y-1):02d}/12"
    return f"{y:02d}/{(m-1):02d}"

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
                    med[prov] = round2(se.median())
                    mean[prov] = round2(se.mean())

    seoul = df[df.get("광역", "") == "서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul) > 0:
        se = eok_series(seoul.get("거래금액(만원)", []))
        if not se.empty:
            med["서울"] = round2(se.median())
            mean["서울"] = round2(se.mean())
        if "구" in seoul.columns:
            for gu, sub in seoul.groupby("구"):
                gu = str(gu)
                if gu in counts:
                    counts[gu] += int(len(sub))
                    se2 = eok_series(sub.get("거래금액(만원)", []))
                    if not se2.empty:
                        med[gu] = round2(se2.median())
                        mean[gu] = round2(se2.mean())

    ap = seoul[seoul.get("법정동", "") == "압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap) > 0:
        s = eok_series(ap.get("거래금액(만원)", []))
        if not s.empty:
            med["압구정동"] = round2(s.median())
            mean["압구정동"] = round2(s.mean())

    return counts, med, mean

# ===================== 공통 유틸 =====================
def kdate(d: datetime) -> str:
    return f"{d.year}. {d.month}. {d.day}"

def first_data_date(ws: gspread.Worksheet) -> Optional[date]:
    vals = _get_all_values_cached(ws)
    if len(vals) <= 1:
        return None
    for r in vals[1:]:
        if not r:
            continue
        dd = parse_any_date(str(r[0]).strip())
        if dd:
            return dd
    return None

def latest_data_date(ws: gspread.Worksheet) -> Optional[date]:
    vals = _get_all_values_cached(ws)
    for r in reversed(vals[1:]):
        if not r:
            continue
        dd = parse_any_date(str(r[0]).strip())
        if dd:
            return dd
    return None

def month_sheet_to_frame(ws: gspread.Worksheet) -> pd.DataFrame:
    vals = _get_all_values_cached(ws)
    if not vals or len(vals) < 2:
        return pd.DataFrame()

    raw_header = [str(h) for h in vals[0]]
    rows = []
    col_map: Dict[int, str] = {}
    for i, h in enumerate(raw_header[1:], start=1):
        if not h:
            continue
        col_map[i] = _norm(h)

    for r in vals[1:]:
        if not r or not r[0]:
            continue
        dd = parse_any_date(str(r[0]).strip())
        if not dd:
            continue
        row = {"date": dd}
        for i, hn in col_map.items():
            try:
                row[hn] = int(float(r[i])) if i < len(r) and str(r[i]).strip() else 0
            except Exception:
                row[hn] = 0
        rows.append(row)

    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

def daily_increments(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    inc = df.copy()
    for c in inc.columns:
        if c == "date":
            continue
        inc[c] = inc[c].astype(int)
        inc[c] = inc[c].diff().fillna(inc[c]).astype(int)
        inc[c] = inc[c].clip(lower=0)
    return inc

def is_weekend_or_holiday(d: date) -> bool:
    if d.weekday() >= 5:
        return True
    return d.strftime("%Y-%m-%d") in KR_HOLIDAYS

def weekday_ratio_for_completion(d: date) -> float:
    return 1.12 if is_weekend_or_holiday(d) else 0.95

# ===== 일차별 누적 프레임(단조증가화 포함) =====
def build_cum_by_day(ws: gspread.Worksheet, allow_cols: set, horizon: int = 90) -> Tuple[pd.DataFrame, Optional[date]]:
    df = month_sheet_to_frame(ws)
    if df.empty:
        return pd.DataFrame(), None
    fday = first_data_date(ws)
    if not fday:
        return pd.DataFrame(), None

    cols = ["date"] + [c for c in df.columns if c != "date" and c in allow_cols]
    if len(cols) <= 1:
        return pd.DataFrame(), None

    g = df[cols].copy().sort_values("date").reset_index(drop=True)
    for c in cols:
        if c == "date":
            continue
        g[c] = pd.to_numeric(g[c], errors="coerce").fillna(0).astype(int)
        g[c] = g[c].cummax()

    idx = pd.date_range(fday, fday + pd.Timedelta(days=horizon-1), freq="D")
    g = g.set_index("date").reindex(idx, method="ffill").fillna(0)
    g.index.name = "date"
    g = g.reset_index()
    return g, fday

def is_mature_month(ws: gspread.Worksheet, min_days: int = 85) -> bool:
    f = first_data_date(ws); l = latest_data_date(ws)
    if not f or not l:
        return False
    return (l - f).days + 1 >= min_days

# ===== 학습: d일차 전역 median(C90/Cd) + p99 상한 =====
def train_day_ratios(
    sheets: Dict[str, Dict[str, gspread.Worksheet]],
    level: str,
    start_key: Tuple[int,int],
    end_key_exclusive: Tuple[int,int],
    allow_cols: set,
    min_days: int = 85,
    horizon: int = 90,
) -> Tuple[Dict[str, List[float]], Dict[str, List[float]]]:

    def ym_to_key(ym: str) -> Tuple[int,int]:
        ym2 = ym_norm(ym) or ym
        yy, mm = ym2.split("/")
        return (2000 + int(yy), int(mm))

    pool = {ym: ws for ym, ws in sheets[level].items()
            if ym_to_key(ym) >= start_key and ym_to_key(ym) < end_key_exclusive}

    ratios_by_region_day: Dict[str, List[List[float]]] = {}
    for ym, ws in sorted(pool.items(), key=lambda kv: ym_to_key(kv[0])):
        if not is_mature_month(ws, min_days=min_days):
            continue
        df_cum, _ = build_cum_by_day(ws, allow_cols, horizon=horizon)
        if df_cum.empty:
            continue

        for col in df_cum.columns:
            if col == "date":
                continue
            s = pd.to_numeric(df_cum[col], errors="coerce").fillna(0).astype(int)
            c90 = int(s.iloc[horizon-1])
            if c90 <= 0:
                continue
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

def collect_training_pairs(
    sheets: Dict[str, Dict[str, gspread.Worksheet]],
    level: str,
    start_key: Tuple[int,int],
    end_key_exclusive: Tuple[int,int],
    allow_cols: set,
    min_days: int = 85,
    horizon: int = 90,
) -> Dict[str, Dict[int, List[Tuple[int,int]]]]:

    def ym_to_key(ym: str) -> Tuple[int,int]:
        ym2 = ym_norm(ym) or ym
        yy, mm = ym2.split("/")
        return (2000 + int(yy), int(mm))

    pool = {ym: ws for ym, ws in sheets[level].items()
            if ym_to_key(ym) >= start_key and ym_to_key(ym) < end_key_exclusive}

    out: Dict[str, Dict[int, List[Tuple[int,int]]]] = {}
    for ym, ws in sorted(pool.items(), key=lambda kv: ym_to_key(kv[0])):
        if not is_mature_month(ws, min_days=min_days):
            continue
        df_cum, _ = build_cum_by_day(ws, allow_cols, horizon=horizon)
        if df_cum.empty:
            continue

        for col in df_cum.columns:
            if col == "date":
                continue
            s = pd.to_numeric(df_cum[col], errors="coerce").fillna(0).astype(int)
            c90 = int(s.iloc[horizon-1])
            if c90 <= 0:
                continue
            for d in range(1, horizon+1):
                cd = int(s.iloc[d-1])
                if cd <= 0:
                    continue
                out.setdefault(col, {}).setdefault(d, []).append((cd, c90))
    return out

def local_ratio_for_observed(
    pairs_for_day: List[Tuple[int,int]],
    observed_cd: int,
    k_neighbors: int = 25,
    width_factor: float = 2.0,
) -> Optional[float]:
    if not pairs_for_day:
        return None
    obs = max(1, int(observed_cd))
    lo, hi = int(obs/width_factor), int(obs*width_factor)
    bucket = [(cd, c90) for (cd, c90) in pairs_for_day if lo <= cd <= hi]
    if not bucket:
        bucket = sorted(pairs_for_day, key=lambda t: abs(t[0]-obs))[:k_neighbors]
    if not bucket:
        return None
    ratios = [float(c90)/max(cd,1) for (cd, c90) in bucket if cd > 0 and c90 > 0]
    if not ratios:
        return None
    return float(np.median(ratios))

def predict_by_day_ratio(
    ws: gspread.Worksheet,
    ratios: Dict[str, List[float]],
    allow_cols: set,
    today: date,
    fallback_ratio: float = 2.0,
    horizon: int = 90,
    training_pairs: Optional[Dict[str, Dict[int, List[Tuple[int,int]]]]] = None,
    day_caps: Optional[Dict[str, List[float]]] = None,
) -> Dict[str, int]:

    df_cum, fday = build_cum_by_day(ws, allow_cols, horizon=horizon)
    if df_cum.empty or not fday:
        return {}

    eff_today = today - timedelta(days=1)
    d = min(max(1, (eff_today - fday).days + 1), horizon)
    dow_factor = weekday_ratio_for_completion(eff_today)

    out: Dict[str, int] = {}
    for col in df_cum.columns:
        if col == "date":
            continue
        s = pd.to_numeric(df_cum[col], errors="coerce").fillna(0).astype(int)
        obs = int(s.iloc[d-1])

        loc_ratio = None
        if training_pairs is not None:
            loc_ratio = local_ratio_for_observed(training_pairs.get(col, {}).get(d, []), obs)

        if loc_ratio is None:
            base_ratio = float(ratios.get(col, [0]*(horizon+1))[d]) if col in ratios else fallback_ratio
        else:
            base_ratio = float(loc_ratio)

        ratio = max(1.0, base_ratio * dow_factor)

        if day_caps is not None and col in day_caps and d < len(day_caps[col]):
            cap = day_caps[col][d]
            if cap and cap != float("inf"):
                ratio = min(ratio, cap * 1.2)

        pred = int(round(obs * ratio))
        pred = max(pred, obs)
        out[col] = pred

    keys = [c for c in df_cum.columns if c != "date" and c != TOTAL_N]
    subtotal = int(sum(int(out.get(k, 0)) for k in keys))
    if TOTAL_N in df_cum.columns:
        out[TOTAL_N] = max(int(df_cum[TOTAL_N].iloc[-1]), subtotal)
    else:
        out[TOTAL_N] = subtotal

    return out

# ===================== 시트/요약 라인 유틸 =====================
def find_or_append_date_row(ws: gspread.Worksheet, date_label: str) -> int:
    """기존 로직은 row[0] 문자열이 date_label과 완전일치할 때만 매칭.
       -> 공백/포맷 차이로 같은 날짜가 새 줄로 계속 추가되는 문제가 있었음.
       -> 파싱해서 같은 날짜면 매칭하도록 변경.
    """
    target = parse_any_date(date_label) or None
    vals = _get_all_values_cached(ws)
    if not vals:
        return 2

    for i, row in enumerate(vals[1:], start=2):
        if not row or len(row) == 0:
            continue
        cur = parse_any_date(str(row[0]).strip())
        if target and cur and cur == target:
            return i
        if (not target) and str(row[0]).strip() == str(date_label).strip():
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
        header_len = len(header or [])
        sheet_id_env = os.environ.get("SHEET_ID", "").strip()
        log_focus_link(ws, row_idx, header_len, sheet_id_env)
        upsert_named_range(ws, f"LATEST_{ws.id}", row_idx, header_len)

def ym_from_filename(fname: str):
    """
    파일명에서 (y, m)을 뽑아:
      - 전국 탭 제목
      - 서울 탭 제목
      - ym('YY/MM') 반환

    지원:
      1) YYMM_ / YYMM- / YYMM. / YYMM<space>
      2) YYYYMM
      3) 'YYYY년 M월'
    """
    s = str(fname or "")

    m = re.search(r"\b(\d{2})(\d{2})[_\-\.\s]", s)
    if m:
        yy, mm = int(m.group(1)), int(m.group(2))
        if 1 <= mm <= 12:
            y = 2000 + yy
            return f"전국 {y}년 {mm}월", f"서울 {y}년 {mm}월", f"{yy:02d}/{mm:02d}"

    m = re.search(r"\b(\d{2})(\d{2})(?=\D|$)", s)
    if m:
        yy, mm = int(m.group(1)), int(m.group(2))
        if 1 <= mm <= 12:
            y = 2000 + yy
            return f"전국 {y}년 {mm}월", f"서울 {y}년 {mm}월", f"{yy:02d}/{mm:02d}"

    m = re.search(r"\b(20\d{2})(\d{2})\b", s)
    if m:
        y, mm = int(m.group(1)), int(m.group(2))
        if 1 <= mm <= 12:
            return f"전국 {y}년 {mm}월", f"서울 {y}년 {mm}월", f"{y%100:02d}/{mm:02d}"

    m = re.search(r"(20\d{2})\s*년\s*(\d{1,2})\s*월", s)
    if m:
        y, mm = int(m.group(1)), int(m.group(2))
        if 1 <= mm <= 12:
            return f"전국 {y}년 {mm}월", f"서울 {y}년 {mm}월", f"{y%100:02d}/{mm:02d}"

    return None, None, None

def find_summary_row(ws, ym: str, label: str) -> int:
    ym2 = ym_norm(ym) or ym
    vals = _get_all_values_cached(ws)
    if not vals:
        return 2
    for i, row in enumerate(vals[1:], start=2):
        a = str(row[0]).strip() if len(row) > 0 else ""
        b = str(row[1]).strip() if len(row) > 1 else ""
        if ym_norm(a) == ym2 and b == label:
            return i
    return len(vals) + 1

def put_summary_line(ws, row_idx: int, ym: str, label: str, line_map: dict):
    ym2 = ym_norm(ym) or ym
    header = _retry(ws.row_values, 1)
    if not header:
        default_cols = [c for c in SUMMARY_COLS if c != "서울"]
        ws_update(ws, [["년월", "구분"] + default_cols], "A1")
        header = ["년월", "구분"] + default_cols

    hmap_norm = {_norm(h): i + 1 for i, h in enumerate(header)}
    sheet_prefix = f"'{ws.title}'!"
    payload = [
        {"range": f"{sheet_prefix}A{row_idx}", "values": [[ym2]]},
        {"range": f"{sheet_prefix}B{row_idx}", "values": [[label]]},
    ]

    for k_raw, v in (line_map or {}).items():
        k = "서울특별시" if k_raw == "서울" else k_raw
        if _norm(k) in hmap_norm:
            payload.append({"range": f"{sheet_prefix}{a1_col(hmap_norm[_norm(k)])}{row_idx}", "values": [[v]]})

    for possible in ("총합계", "합계"):
        pn = _norm(possible)
        if pn in hmap_norm:
            vv = (line_map.get("총합계", "") if isinstance(line_map, dict) else "")
            if vv == "":
                vv = line_map.get("전국", line_map.get("서울특별시", "")) if isinstance(line_map, dict) else ""
            payload.append({"range": f"{sheet_prefix}{a1_col(hmap_norm[pn])}{row_idx}", "values": [[vv if vv != "" else 0]]})
            break

    if payload:
        values_batch_update(ws, payload)

def color_diff_line(ws, row_idx: int, diff_line: dict, header: List[str]):
    hmap = {h: i + 1 for i, h in enumerate(header)}
    reqs = []
    for k, v in diff_line.items():
        if k not in hmap or v in ("", "0"):
            continue
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
    ym = f"{(y%100):02d}/{m:02d}"

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

    header_now = _retry(ws.row_values, 1)
    sheet_id_env = os.environ.get("SHEET_ID", "").strip()
    log_focus_link(ws, r4, len(header_now or []), sheet_id_env)
    upsert_named_range(ws, f"LATEST_{ws.id}", r4, len(header_now or []))

def write_predicted_line(ws_sum: gspread.Worksheet, ym: str, pred_map: dict):
    ym2 = ym_norm(ym) or ym

    if pred_map and "서울" in pred_map:
        if (pred_map.get("서울특별시") in ("", None)) and (pred_map["서울"] not in ("", None)):
            pred_map["서울특별시"] = pred_map["서울"]
        pred_map.pop("서울", None)

    r = find_summary_row(ws_sum, ym2, "예상건수")
    put_summary_line(ws_sum, r, ym2, "예상건수", pred_map)
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
    log(f"[summary] {ym2} 예상건수 -> row={r} (green bold) filled={filled}")

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

APGU_KEY_COLS = [
    "광역","구","법정동","번지","단지명","전용면적(㎡)",
    "계약년","계약월","계약일","거래금액(만원)","동","층"
]

def _apgu_norm(v) -> str:
    return "" if v is None else str(v).strip()

def _apgu_key_from_row_values(values: List[str], header: List[str], key_cols: Optional[List[str]] = None) -> str:
    idx = {h: i for i, h in enumerate(header)}
    use_cols = key_cols or APGU_KEY_COLS
    parts = []
    for h in use_cols:
        i = idx.get(h, None)
        parts.append(_apgu_norm(values[i] if (i is not None and i < len(values)) else ""))
    return "|".join(parts)

def _ensure_rows(ws: gspread.Worksheet, need_end_row: int):
    if need_end_row > ws.row_count:
        ws_add_rows(ws, need_end_row - ws.row_count)

def fmt_kdate(d: date) -> str:
    return f"{d.year}. {d.month}. {d.day}"

def upsert_apgu_verbatim(ws: gspread.Worksheet, df_all: pd.DataFrame, run_day: date):
    df = df_all[(df_all.get("광역", "") == "서울특별시") & (df_all.get("법정동", "") == "압구정동")].copy()
    if df.empty:
        log("[압구정동] no rows")
        return

    for c in APGU_BASE_COLS:
        if c not in df.columns:
            df[c] = ""

    df = df.sort_values(["계약년", "계약월", "계약일"], ascending=[True, True, True], kind="mergesort")

    _invalidate_cache(ws)
    vals = _retry(ws.get_all_values) or []
    _invalidate_cache(ws)

    if not vals:
        ws_update(ws, [APGU_BASE_COLS], "A1")
        vals = [APGU_BASE_COLS]

    if vals[0] != APGU_BASE_COLS:
        ws_update(ws, [APGU_BASE_COLS], "A1")
        vals[0] = APGU_BASE_COLS

    header = APGU_BASE_COLS
    body = vals[1:]

    def _is_empty_row(r: List[str]) -> bool:
        return (not r) or all(_apgu_norm(x) == "" for x in r)

    while body and _is_empty_row(body[-1]):
        body.pop()

    change_header = ["변경구분", "변경일"] + APGU_BASE_COLS
    hist_start_idx = None
    for i, r in enumerate(body, start=2):
        if r and _apgu_norm(r[0]) == _apgu_norm("변경구분"):
            hist_start_idx = i
            break

    if hist_start_idx is None:
        base_rows_old = [ (r + [""]*len(header))[:len(header)] for r in body if not _is_empty_row(r) ]
        hist_rows_old = []
    else:
        base_part = body[:hist_start_idx-2]
        hist_part = body[hist_start_idx-2:]
        base_rows_old = [ (r + [""]*len(header))[:len(header)] for r in base_part if not _is_empty_row(r) ]
        hist_rows_old = hist_part[:]

    base_rows_new = [[_apgu_norm(row.get(c, "")) for c in APGU_BASE_COLS] for _, row in df.iterrows()]

    old_keys = {_apgu_key_from_row_values(r, header, APGU_KEY_COLS) for r in base_rows_old}
    new_keys = {_apgu_key_from_row_values(r, header, APGU_KEY_COLS) for r in base_rows_new}

    hist_body_old = []
    if hist_rows_old:
        if hist_rows_old[0] and _apgu_norm(hist_rows_old[0][0]) == _apgu_norm("변경구분"):
            hist_body_old = [ (r + [""]*len(change_header))[:len(change_header)]
                              for r in hist_rows_old[1:] if not _is_empty_row(r) ]

    def _key_from_hist_row(hr: List[str]) -> str:
        base_part = (hr[2:] + [""] * len(APGU_BASE_COLS))[:len(APGU_BASE_COLS)]
        return _apgu_key_from_row_values(base_part, header, APGU_KEY_COLS)

    last_status: Dict[str, str] = {}
    existing_events = set()

    for hr in hist_body_old:
        chg = _apgu_norm(hr[0])
        day = _apgu_norm(hr[1])
        if chg not in ("(신규)", "(삭제)", "(재등장)"):
            continue
        k = _key_from_hist_row(hr)
        last_status[k] = chg
        existing_events.add((chg, day, k))

    inactive_keys = {k for k, st in last_status.items() if st == "(삭제)"}
    active_old_keys = old_keys - inactive_keys

    added_all = sorted(list(new_keys - active_old_keys))
    removed_keys = sorted(list(active_old_keys - new_keys))

    new_map = {_apgu_key_from_row_values(r, header, APGU_KEY_COLS): r for r in base_rows_new}
    old_map = {_apgu_key_from_row_values(r, header, APGU_KEY_COLS): r for r in base_rows_old}

    brand_new_keys = [k for k in added_all if k not in old_keys]
    reappear_keys  = [k for k in added_all if k in old_keys]

    base_append = [new_map[k] for k in brand_new_keys if k in new_map]
    base_rows_updated = base_rows_old + base_append

    today_str = fmt_kdate(run_day)

    hist_append_rows = []
    for k in brand_new_keys:
        evt = ("(신규)", today_str, k)
        if evt not in existing_events and k in new_map:
            hist_append_rows.append(["(신규)", today_str] + new_map[k])

    for k in reappear_keys:
        evt = ("(재등장)", today_str, k)
        if evt not in existing_events and k in new_map:
            hist_append_rows.append(["(재등장)", today_str] + new_map[k])

    for k in removed_keys:
        evt = ("(삭제)", today_str, k)
        if evt not in existing_events and k in old_map:
            hist_append_rows.append(["(삭제)", today_str] + old_map[k])

    added_cnt = len(brand_new_keys) + len(reappear_keys)

    out = [APGU_BASE_COLS]
    out.extend(base_rows_updated)

    if hist_body_old or hist_append_rows or hist_start_idx is not None:
        out.append([""] * len(APGU_BASE_COLS))
        out.append(change_header)
        out.extend(hist_body_old)
        out.extend(hist_append_rows)

    max_cols = max(len(APGU_BASE_COLS), len(change_header))
    end_row = len(out)
    end_col = a1_col(max_cols)

    _ensure_rows(ws, end_row)
    ws_update(ws, out, f"A1:{end_col}{end_row}")

    prev_total_rows = len(vals)
    if prev_total_rows > end_row:
        blanks = [[""] * max_cols for _ in range(prev_total_rows - end_row)]
        ws_update(ws, blanks, f"A{end_row+1}:{end_col}{prev_total_rows}")

    if hist_append_rows:
        sep_row = 1 + 1 + len(base_rows_updated)
        hist_header_row = sep_row + 1
        append_start_row = hist_header_row + 1 + len(hist_body_old)

        reqs = []
        if added_cnt > 0:
            reqs.append({
                "repeatCell": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": append_start_row - 1,
                        "endRowIndex": append_start_row - 1 + added_cnt,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(change_header),
                    },
                    "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red": 0.0, "green": 0.0, "blue": 1.0}}}},
                    "fields": "userEnteredFormat.textFormat.foregroundColor",
                }
            })

        if len(removed_keys) > 0:
            del_start = append_start_row + added_cnt
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

    log(f"[압구정동] base append={len(base_append)} / history append 신규+재등장={added_cnt} 삭제={len(removed_keys)}")

# ===================== 패턴 분석 차트 빌더(안전) =====================
def build_line_chart_request(
    sheet_id: int,
    title: str,
    nrows: int,
    nseries: int,
    anchor_row: int = 0,
    anchor_col: int = 10,
    width: int = 800,
    height: int = 320,
) -> dict:
    domain = {
        "domain": {
            "sourceRange": {
                "sources": [{
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": nrows,
                    "startColumnIndex": 0,
                    "endColumnIndex": 1
                }]
            }
        }
    }

    series = []
    for j in range(nseries):
        series.append({
            "series": {
                "sourceRange": {
                    "sources": [{
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": nrows,
                        "startColumnIndex": 1 + j,
                        "endColumnIndex": 2 + j
                    }]
                }
            },
            "targetAxis": "LEFT_AXIS"
        })

    chart = {
        "spec": {
            "title": title,
            "basicChart": {
                "chartType": "LINE",
                "legendPosition": "BOTTOM_LEGEND",
                "axis": [
                    {"position": "BOTTOM_AXIS", "title": "Date"},
                    {"position": "LEFT_AXIS", "title": "Cumulative"}
                ],
                "domains": [domain],
                "series": series,
                "headerCount": 1
            }
        },
        "position": {
            "overlayPosition": {
                "anchorCell": {"sheetId": sheet_id, "rowIndex": anchor_row, "columnIndex": anchor_col},
                "offsetXPixels": 0,
                "offsetYPixels": 0,
                "widthPixels": width,
                "heightPixels": height
            }
        }
    }

    return {"addChart": {"chart": chart}}

# ===== 패턴 분석 시트 (표 + 라인차트) =====
def render_pattern_analysis(sh: gspread.Spreadsheet, month_title: str, df_cum: pd.DataFrame, df_inc: pd.DataFrame, targets: List[str]):
    ws = get_or_create_ws(sh, "거래량 패턴분석", rows=1000, cols=40)
    ws_clear(ws)

    header1 = ["[누적] 날짜"] + targets
    header2 = ["[일증분] 날짜"] + targets

    cum_rows = [header1]
    for i in range(len(df_cum)):
        d = df_cum.iloc[i]["date"]
        row = [d.strftime("%Y-%m-%d")] + [int(df_cum.iloc[i].get(_norm(t), 0)) for t in targets]
        cum_rows.append(row)

    inc_rows = [header2]
    for i in range(len(df_inc)):
        d = df_inc.iloc[i]["date"]
        row = [d.strftime("%Y-%m-%d")] + [int(df_inc.iloc[i].get(_norm(t), 0)) for t in targets]
        inc_rows.append(row)

    ws_update(ws, cum_rows, "A1")
    start_inc = len(cum_rows) + 2
    ws_update(ws, inc_rows, f"A{start_inc}")

    nrows = len(cum_rows)
    nseries = len(targets)

    req = build_line_chart_request(
        sheet_id=ws.id,
        title=f"{month_title} - 누적 등록 추이",
        nrows=nrows,
        nseries=nseries,
        anchor_row=0,
        anchor_col=10,
        width=800,
        height=320,
    )

    _retry(ws.spreadsheet.batch_update, {"requests": [req]})
    log("[pattern] 분석 탭 작성 및 차트 추가")

# ===== 월 시트 누적으로 거래건수 읽기 =====
def read_counts_from_month_sheet(ws: gspread.Worksheet) -> Dict[str, int]:
    df_cum = month_sheet_to_frame(ws)
    if df_cum.empty:
        return {}
    last = df_cum.iloc[-1]
    out: Dict[str, int] = {}
    for col in list(df_cum.columns):
        if col == "date":
            continue
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

def is_no_data_month(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return True
    need = {"계약년", "계약월"}
    if not need.issubset(set(df.columns)):
        return True
    yy = pd.to_numeric(df["계약년"], errors="coerce")
    mm = pd.to_numeric(df["계약월"], errors="coerce")
    return (yy.dropna().empty or mm.dropna().empty)

def list_month_sheets(sh: gspread.Spreadsheet):
    out = {"전국": {}, "서울": {}}
    for ws in sh.worksheets():
        t = ws.title.strip()
        if t.startswith("전국 ") and YM_RE.search(t):
            ym = yymm_from_title(t)
            if ym:
                out["전국"][ym] = ws
        elif t.startswith("서울 ") and YM_RE.search(t):
            ym = yymm_from_title(t)
            if ym:
                out["서울"][ym] = ws
    return out

def _clear_values_below_header(ws: gspread.Worksheet, max_rows: int = 800):
    try:
        vals = _retry(ws.get_all_values) or []
        _invalidate_cache(ws)
        used_rows = max(2, min(len(vals), max_rows))
        used_cols = 0
        for r in vals[:used_rows]:
            used_cols = max(used_cols, len(r))
        used_cols = max(1, used_cols)
    except Exception:
        used_rows = max_rows
        used_cols = max(1, min(40, getattr(ws, "col_count", 40)))

    end_col = a1_col(used_cols)
    rng = f"A2:{end_col}{used_rows}"

    try:
        ws_batch_clear(ws, [rng])
        return
    except Exception:
        pass

    blanks = [[""] * used_cols for _ in range(max(0, used_rows - 1))]
    ws_update(ws, blanks, rng)

def ensure_month_ws(sh: gspread.Spreadsheet, title: str, level: str, ym: str) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, title)
    if ws is not None:
        return ws

    sheets_map = list_month_sheets(sh)
    candidates = sheets_map.get(level, {})

    tpl = None
    cur = ym_norm(ym) or ym
    for _ in range(36):
        cur = prev_ym(cur)
        if cur in candidates:
            tpl = candidates[cur]
            break

    if tpl is not None:
        try:
            _retry(sh.duplicate_sheet, tpl.id, new_sheet_name=title)
        except TypeError:
            _retry(sh.duplicate_sheet, tpl.id, None, None, title)

        ws = fuzzy_ws(sh, title)
        if ws is None:
            ws = _retry(sh.worksheet, title)

        _clear_values_below_header(ws)
        _invalidate_cache(ws)
        log(f"[ws] created by duplication: {title} (from={tpl.title})")
        return ws

    ws = get_or_create_ws(sh, title, rows=800, cols=40)
    header = ["날짜"] + (NATION_REGIONS if level == "전국" else SEOUL_REGIONS)
    ws_update(ws, [header], "A1")
    _invalidate_cache(ws)
    log(f"[ws] created from scratch: {title}")
    return ws

SUMMARY_MONTH_LABELS = [
    "거래건수",
    "중앙값(단위:억)",
    "평균가(단위:억)",
    "전월대비 건수증감",
    "예상건수",
]

def ensure_summary_month_rows(ws_sum: gspread.Worksheet, ym: str):
    ym2 = ym_norm(ym) or ym
    for label in SUMMARY_MONTH_LABELS:
        r = find_summary_row(ws_sum, ym2, label)
        put_summary_line(ws_sum, r, ym2, label, {})

def momentum_factor_from_increments(df_cum: pd.DataFrame, region_col_n: str, day_idx_for_window: int, k: int = 3, gamma: float = 0.20) -> float:
    if region_col_n not in df_cum.columns:
        return 1.0
    s = pd.to_numeric(df_cum[region_col_n], errors="coerce").fillna(0).astype(int)
    inc = s.diff().fillna(s).clip(lower=0)
    end = max(1, min(day_idx_for_window, len(inc)))
    start_recent = max(1, end - k + 1)
    start_prev   = max(1, start_recent - k)
    recent_sum = float(inc.iloc[start_recent-1:end].sum()) if end >= start_recent else 0.0
    prev_sum   = float(inc.iloc[start_prev-1:start_recent-1].sum()) if start_recent-1 >= start_prev else 0.0
    momentum = (recent_sum - prev_sum) / max(prev_sum, 1.0)
    factor = 1.0 + gamma * momentum
    return float(max(0.85, min(1.15, factor)))

# ===================== 메인 =====================
def main():
    try:
        RUN_LOG.write_text("", encoding="utf-8")
    except Exception:
        pass

    log("[MAIN]")
    log(f"artifacts_dir={ARTIFACTS_DIR}")

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

    files = sorted(Path(ARTIFACTS_DIR).rglob("전국*.xlsx"))
    log(f"[collect] found {len(files)} xlsx files")
    if len(files) == 0:
        log("[ERROR] 입력파일(전국*.xlsx) 0개 - artifacts 폴더 경로/업로드를 확인하세요.")
        return

    month_cache = {}
    today_label = kdate(datetime.now())
    run_day = datetime.now().date()
    apgu_all: List[pd.DataFrame] = []

    for p in files:
        nat_title, se_title, ym = ym_from_filename(p.name)
        if not ym:
            log(f"[skip] cannot parse ym from filename: {p.name}")
            continue
        ym = ym_norm(ym) or ym
        log(f"[file] {p.name} -> nat='{nat_title}' / seoul='{se_title}' / ym={ym}")

        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        if is_no_data_month(df):
            log(f"[no-data] {p.name} -> 0건으로 기록")
            zero_counts = {c: 0 for c in SUMMARY_COLS}
            empty_med   = {c: "" for c in SUMMARY_COLS}
            empty_mean  = {c: "" for c in SUMMARY_COLS}
            month_cache[ym] = {"counts": zero_counts, "med": empty_med, "mean": empty_mean}

            ws_nat = ensure_month_ws(sh, nat_title, level="전국", ym=ym)
            header_nat = _retry(ws_nat.row_values, 1)
            values_nat = {}
            for h in header_nat:
                if not h or h == "날짜":
                    continue
                if h == "총합계":
                    values_nat["총합계"] = 0
                elif h in zero_counts:
                    values_nat[h] = 0
            write_month_sheet(ws_nat, today_label, header_nat, values_nat)

            ws_se = ensure_month_ws(sh, se_title, level="서울", ym=ym)
            header_se = _retry(ws_se.row_values, 1)
            values_se = {}
            for h in header_se:
                if not h or h == "날짜":
                    continue
                if h == "총합계":
                    values_se["총합계"] = 0
                elif h in zero_counts:
                    values_se[h] = 0
            write_month_sheet(ws_se, today_label, header_se, values_se)
            continue

        counts, med, mean = agg_all_stats(df)
        month_cache[ym] = {"counts": counts, "med": med, "mean": mean}

        ws_nat = ensure_month_ws(sh, nat_title, level="전국", ym=ym)
        header_nat = _retry(ws_nat.row_values, 1)
        values_nat: Dict[str, int] = {}
        for h in header_nat:
            if not h or h == "날짜":
                continue
            if h == "총합계":
                values_nat["총합계"] = int(counts.get("전국", 0))
            elif h in counts:
                values_nat[h] = int(counts[h])
        write_month_sheet(ws_nat, today_label, header_nat, values_nat)

        ws_se = ensure_month_ws(sh, se_title, level="서울", ym=ym)
        header_se = _retry(ws_se.row_values, 1)
        values_se: Dict[str, int] = {}
        for h in header_se:
            if not h or h == "날짜":
                continue
            if h == "총합계":
                values_se["총합계"] = int(counts.get("서울", 0))
            elif h in counts:
                values_se[h] = int(counts[h])
        write_month_sheet(ws_se, today_label, header_se, values_se)

        ap = df[(df.get("광역", "") == "서울특별시") & (df.get("법정동", "") == "압구정동")].copy()
        if not ap.empty:
            apgu_all.append(ap)

    ws_sum = fuzzy_ws(sh, SUMMARY_SHEET_NAME)
    if ws_sum:
        if month_cache:
            def ym_key(ymx: str):
                ym2 = ym_norm(ymx) or ymx
                yy, mm = ym2.split("/")
                return (int(yy), int(mm))

            for ym in sorted(month_cache.keys(), key=ym_key):
                cur = month_cache[ym]
                prv = month_cache.get(prev_ym(ym))
                ensure_summary_month_rows(ws_sum, ym)
                y = 2000 + int(ym.split("/")[0])
                m = int(ym.split("/")[1])
                write_month_summary(ws_sum, y, m, cur["counts"], cur["med"], cur["mean"], prv["counts"] if prv else None)
                time.sleep(0.15)

        # 최근 12개월 보정 기록(월 시트 누적)
        today = datetime.now().date()

        def add_months(y, m, delta):
            nm = m + delta
            y += (nm - 1) // 12
            nm = ((nm - 1) % 12) + 1
            return y, nm

        def yM_to_ym(y: int, m: int) -> str:
            return f"{(y%100):02d}/{m:02d}"

        cur_y, cur_m = today.year, today.month
        last12_yM = [add_months(cur_y, cur_m, -i) for i in range(0, 12)]
        last12_ym = [yM_to_ym(y, m) for (y, m) in last12_yM]

        for ym in last12_ym:
            counts = None; med = None; mean = None; prv_counts = None
            ensure_summary_month_rows(ws_sum, ym)

            if ym in month_cache:
                counts = month_cache[ym]["counts"]
                med = month_cache[ym]["med"]
                mean = month_cache[ym]["mean"]
                prv = month_cache.get(prev_ym(ym))
                prv_counts = prv["counts"] if prv else None
            else:
                ws_nat = None; ws_se = None
                for w in sh.worksheets():
                    if re.search(r"^전국\s+\d{4}년\s+\d{1,2}월$", w.title) and yymm_from_title(w.title) == ym:
                        ws_nat = w
                    if re.search(r"^서울\s+\d{4}년\s+\d{1,2}월$", w.title) and yymm_from_title(w.title) == ym:
                        ws_se = w

                c_nat = read_counts_from_month_sheet(ws_nat) if ws_nat else {}
                c_se  = read_counts_from_month_sheet(ws_se) if ws_se else {}

                if c_nat or c_se:
                    merged_n = {k: c_nat.get(k, 0) for k in c_nat.keys()}
                    for k, v in c_se.items():
                        merged_n[k] = v

                    counts = {}
                    for k_n, v in merged_n.items():
                        human = next((orig for orig in SUMMARY_COLS if _norm(orig) == k_n), k_n)
                        if human == "서울":
                            human = "서울특별시"
                        counts[human] = v

                    ym_prev = prev_ym(ym)
                    ws_nat_prev = None; ws_se_prev = None
                    for w in sh.worksheets():
                        if re.search(r"^전국\s+\d{4}년\s+\d{1,2}월$", w.title) and yymm_from_title(w.title) == ym_prev:
                            ws_nat_prev = w
                        if re.search(r"^서울\s+\d{4}년\s+\d{1,2}월$", w.title) and yymm_from_title(w.title) == ym_prev:
                            ws_se_prev = w

                    if ws_nat_prev or ws_se_prev:
                        c_nat_prev = read_counts_from_month_sheet(ws_nat_prev) if ws_nat_prev else {}
                        c_se_prev  = read_counts_from_month_sheet(ws_se_prev) if ws_se_prev else {}

                        prv_merged = {k: c_nat_prev.get(k, 0) for k in c_nat_prev.keys()}
                        for k, v in c_se_prev.items():
                            prv_merged[k] = v

                        prv_counts = {}
                        for k_n, v in prv_merged.items():
                            human = next((orig for orig in SUMMARY_COLS if _norm(orig) == k_n), k_n)
                            if human == "서울":
                                human = "서울특별시"
                            prv_counts[human] = v

            if counts:
                r_cnt = find_summary_row(ws_sum, ym, "거래건수")
                put_summary_line(ws_sum, r_cnt, ym, "거래건수", counts)

                if med:
                    r_med = find_summary_row(ws_sum, ym, "중앙값(단위:억)")
                    put_summary_line(ws_sum, r_med, ym, "중앙값(단위:억)", med)
                if mean:
                    r_mean = find_summary_row(ws_sum, ym, "평균가(단위:억)")
                    put_summary_line(ws_sum, r_mean, ym, "평균가(단위:억)", mean)

                if prv_counts:
                    diffs = {}
                    for c in SUMMARY_COLS:
                        key = "서울특별시" if c == "서울" else c
                        curv = int(counts.get(key, 0) or 0)
                        prvv = int(prv_counts.get(key, 0) or 0)
                        d = curv - prvv
                        diffs[key] = f"+{d}" if d > 0 else (str(d) if d < 0 else "0")
                else:
                    diffs = {("서울특별시" if c == "서울" else c): "" for c in SUMMARY_COLS}

                r_diff = find_summary_row(ws_sum, ym, "전월대비 건수증감")
                put_summary_line(ws_sum, r_diff, ym, "전월대비 건수증감", diffs)
                header_sum = _retry(ws_sum.row_values, 1)
                color_diff_line(ws_sum, r_diff, diffs, header_sum)

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
            log("[predict] no month sheets found")
            return

        today = datetime.now().date()

        def add_months(y, m, delta):
            nm = m + delta
            y += (nm - 1) // 12
            nm = ((nm - 1) % 12) + 1
            return y, nm

        def key_to_ym(y, m) -> str:
            return f"{(y%100):02d}/{m:02d}"

        START_KEY = (2024, 10)
        cur_y, cur_m = today.year, today.month
        end_y, end_m = add_months(cur_y, cur_m, -3)
        END_KEY_EXCL = add_months(end_y, end_m, +1)

        allow_seoul = SEOUL_SET_N | {TOTAL_N}
        allow_nat   = NATION_SET_N | {TOTAL_N}

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

            ws_nat = sheets["전국"].get(ym)
            if ws_nat:
                pred_nat_n = predict_by_day_ratio(ws_nat, ratios_nat, allow_nat, today,
                                                  fallback_ratio=2.0, horizon=90,
                                                  training_pairs=pairs_nat, day_caps=caps_nat)
                df_cum_nat, fday_nat = build_cum_by_day(ws_nat, allow_nat, horizon=90)
                if not df_cum_nat.empty and fday_nat and TOTAL_N in df_cum_nat.columns:
                    mf = momentum_factor_from_increments(df_cum_nat, TOTAL_N, 1, k=3, gamma=0.20)
                    pred_nat_n[TOTAL_N] = int(round(pred_nat_n[TOTAL_N] * mf))

                for k_n, v in pred_nat_n.items():
                    human = next((orig for orig in SUMMARY_COLS if _norm(orig) == k_n), k_n)
                    if human == "서울":
                        human = "서울특별시"
                    merged_pred[human] = int(v)
                if "전국" in SUMMARY_COLS and TOTAL_N in pred_nat_n:
                    merged_pred["전국"] = int(pred_nat_n[TOTAL_N])

            ws_se = sheets["서울"].get(ym)
            if ws_se:
                pred_seoul_n = predict_by_day_ratio(ws_se, ratios_seou, allow_seoul, today,
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
                    if human == "서울":
                        human = "서울특별시"
                    merged_pred[human] = int(v)
                if "서울특별시" in SUMMARY_COLS and TOTAL_N in pred_seoul_n:
                    merged_pred["서울특별시"] = int(pred_seoul_n[TOTAL_N])

            write_predicted_line(ws_sum, ym, merged_pred)

        if targets:
            latest_ym = targets[-1]
            ws_latest = sheets["서울"].get(latest_ym) or sheets["전국"].get(latest_ym)
            if ws_latest:
                df_cum = month_sheet_to_frame(ws_latest)
                df_inc = daily_increments(df_cum)
                targets_cols: List[str] = []
                if _norm("총합계") in df_cum.columns:
                    targets_cols.append("총합계")
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
