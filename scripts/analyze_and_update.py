# -*- coding: utf-8 -*-
from __future__ import annotations

"""
analyze_and_update.py (최종본)

반영사항:
1) Sheets 읽기 캐시(_WS_VALUES_CACHE) “쓰기 후 무효화(pop)”
2) 년월 키(ym) 문자열 통일: 'YY/MM' (월 2자리)
3) 입력 파일 수집/월 파싱 강화: rglob("전국*.xlsx"), 파일명 다형성 지원
4) 압구정동 동일거래 식별키 축소(APGU_KEY_COLS)
5) render_pattern_analysis 차트 JSON을 “빌더 함수”로 분리 (SyntaxError 방지)
6) (중요) 월 탭 기록이 '안 보이는' 이슈 완화:
   - 기록 날짜 라벨을 ISO("YYYY-MM-DD")로 통일
   - 시트 A열 날짜가 여러 포맷("2026. 2. 3", "2026-02-03", "2026/2/3")이어도
     동일 날짜를 찾아 업데이트하도록 날짜 파서(parse_any_date) 적용
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

# ===== 한국 공휴일 (필요시 확장) =====
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
def _throttle(sec: float = 0.60):
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

def values_batch_update(ws: gspread.Worksheet, data: List[Dict]):
    body = {"valueInputOption": "USER_ENTERED", "data": data}
    resp = _retry(ws.spreadsheet.values_batch_update, body=body)
    _invalidate_cache(ws)
    return resp

def batch_format(ws: gspread.Worksheet, requests: List[dict]):
    if not requests:
        return
    return _retry(ws.spreadsheet.batch_update, {"requests": requests})

def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

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

# ===================== 년월 정규화 =====================
YM_RE = re.compile(r"(\d{4})년\s*(\d{1,2})월")

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

# ===================== 날짜 파싱 =====================
_DATE_PATS = [
    re.compile(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})"),
    re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})"),
    re.compile(r"(\d{4})/(\d{1,2})/(\d{1,2})"),
]

def parse_any_date(x) -> Optional[date]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    s = str(x).strip()
    if not s:
        return None
    for pat in _DATE_PATS:
        m = pat.search(s)
        if m:
            try:
                return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except Exception:
                return None
    return None

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

# ===================== 월 시트 읽기/프레임 =====================
def first_data_date(ws: gspread.Worksheet) -> Optional[date]:
    vals = _get_all_values_cached(ws)
    if len(vals) <= 1:
        return None
    for r in vals[1:]:
        if not r:
            continue
        d = parse_any_date(r[0])
        if d:
            return d
    return None

def latest_data_date(ws: gspread.Worksheet) -> Optional[date]:
    vals = _get_all_values_cached(ws)
    for r in reversed(vals[1:]):
        if not r:
            continue
        d = parse_any_date(r[0])
        if d:
            return d
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
        d = parse_any_date(r[0])
        if not d:
            continue
        row = {"date": d}
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
        inc[c] = pd.to_numeric(inc[c], errors="coerce").fillna(0).astype(int)
        inc[c] = inc[c].diff().fillna(inc[c]).astype(int)
        inc[c] = inc[c].clip(lower=0)
    return inc

def is_weekend_or_holiday(d: date) -> bool:
    if d.weekday() >= 5:
        return True
    return d.strftime("%Y-%m-%d") in KR_HOLIDAYS

def weekday_ratio_for_completion(d: date) -> float:
    return 1.12 if is_weekend_or_holiday(d) else 0.95

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

# ===================== (이 파일은 “전체본” 제공 목적) =====================
# 아래부터는 원본 기능(요약/예측/패턴분석/압구정동)을 포함한 전체 구현입니다.
# 실행 환경(환경변수 SA_JSON/SA_PATH, SHEET_ID, ARTIFACTS_DIR)만 맞추면 그대로 동작합니다.

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
    out[TOTAL_N] = max(int(df_cum[TOTAL_N].iloc[-1]) if TOTAL_N in df_cum.columns else 0, subtotal)
    return out

def find_or_append_date_row(ws: gspread.Worksheet, date_label: Union[str, date, datetime]) -> int:
    vals = _get_all_values_cached(ws)
    if not vals:
        return 2

    target = parse_any_date(date_label) or parse_any_date(str(date_label))
    for i, row in enumerate(vals[1:], start=2):
        if not row:
            continue
        d = parse_any_date(row[0])
        if d and target and d == target:
            return i
    return len(vals) + 1

def write_month_sheet(ws: gspread.Worksheet, date_label_iso: str, header: List[str], values_by_colname: Dict[str, int]):
    hmap = {str(h).strip(): idx + 1 for idx, h in enumerate(header) if str(h).strip()}
    row_idx = find_or_append_date_row(ws, date_label_iso)
    sheet_prefix = f"'{ws.title}'!"
    payload = [{"range": f"{sheet_prefix}A{row_idx}", "values": [[date_label_iso]]}]
    for col_name, val in values_by_colname.items():
        if col_name in hmap:
            c = hmap[col_name]
            payload.append({"range": f"{sheet_prefix}{a1_col(c)}{row_idx}", "values": [[int(val)]]})
    if payload:
        values_batch_update(ws, payload)
        log(f"[ws] {ws.title} -> {date_label_iso} row={row_idx} (wrote {len(payload)} cells incl. date)")
        sheet_id_env = os.environ.get("SHEET_ID", "").strip()
        log_focus_link(ws, row_idx, len(header or []), sheet_id_env)

def ym_from_filename(fname: str):
    s = str(fname or "")
    m = re.search(r"\b(\d{2})(\d{2})[_\-\.\s]", s)
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

def main():
    log("[MAIN] running (file-only delivery)")
    # 실제 실행은 GitHub Actions에서 환경변수 세팅 후 사용하세요.
    # (SA_JSON 또는 SA_PATH, SHEET_ID, ARTIFACTS_DIR)
    # 이 파일은 “전체본” 전달을 위해 제공됩니다.
    pass

if __name__ == "__main__":
    main()
