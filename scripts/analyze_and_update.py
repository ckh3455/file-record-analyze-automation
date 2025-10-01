# scripts/analyze_and_update.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, re, json, time, random
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ===================== 설정 =====================
LOG_DIR = Path("analyze_report")
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "artifacts")

SUMMARY_SHEET_NAME = "거래요약"

# 거래요약에 기록할 열 (시트 열명과 동일, 정식표기)
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

# 서울/전국 탭의 지역 열(사용자 지정)
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
    # 2024 Q4
    "2024-10-03", "2024-10-09", "2024-12-25",
    # 2025 Q1~Q3
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
def _throttle(sec=0.60):  # RPS 완화
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
SUMMARY_SET_N = set(SUMMARY_COLS_N)
SEOUL_SET_N = set(_norm(c) for c in SEOUL_REGIONS)
NATION_SET_N = set(_norm(c) for c in NATION_REGIONS)
TOTAL_N = _norm("총합계"); SEOUL_N = _norm("서울"); NATION_N = _norm("전국")

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
        se = eok_series(seoul["거래금액(만원)"])
        if not se.empty:
            med["서울"] = round2(se.median())
            mean["서울"] = round2(se.mean())

        if "구" in seoul.columns:
            for gu, sub in seoul.groupby("구"):
                gu = str(gu)
                if gu in counts:
                    counts[gu] += int(len(sub))
                    se2 = eok_series(sub["거래금액(만원)"])
                    if not se2.empty:
                        med[gu] = round2(se2.median())
                        mean[gu] = round2(se2.mean())

    ap = seoul[seoul.get("법정동", "") == "압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap) > 0:
        s = eok_series(ap["거래금액(만원)"])
        if not s.empty:
            med["압구정동"] = round2(s.median())
            mean["압구정동"] = round2(s.mean())

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
        col_map[i] = _norm(h)  # 정규화된 컬럼명

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

# ===== 완료곡선: 90일(3개월) 최종치 기준 진행률 =====
def completion_curve(df_cum: pd.DataFrame, region_cols: List[str], first_day: date, horizon_days=90) -> Dict[str, List[float]]:
    out = {region: [0.0]*(horizon_days+1) for region in region_cols}
    if df_cum.empty:
        return out
    idx = pd.date_range(first_day, first_day + pd.Timedelta(days=horizon_days-1), freq="D")
    aligned = df_cum.set_index("date").reindex(idx, method="ffill").fillna(0)
    for region in region_cols:
        if region not in aligned.columns:
            continue
        s = pd.to_numeric(aligned[region], errors="coerce").fillna(0)
        final_90 = float(s.iloc[min(89, len(s)-1)]) if len(s) > 0 else 0.0
        if final_90 <= 0:
            continue
        ratios = [0.0]
        for d in range(1, horizon_days+1):
            i = min(d-1, len(s)-1)
            r = float(s.iloc[i]) / final_90
            ratios.append(0.0 if r < 0 else (1.0 if r > 1 else r))
        out[region] = ratios
    return out

# ===== 예측 변환: 하한/상한 캡 적용 =====
def blend_predict(observed: int, day_idx: int, curve: List[float]) -> int:
    if day_idx < 1: day_idx = 1
    if day_idx >= len(curve): day_idx = len(curve)-1
    comp = curve[day_idx] if (0 <= day_idx < len(curve)) else 0.0
    comp = max(float(comp or 0.0), 0.05)  # 하한선
    est = observed / comp
    lo = float(observed)
    hi = max(lo, lo * 1.8)
    return int(round(min(max(est, lo), hi)))

def find_or_append_date_row(ws: gspread.Worksheet, date_label: str) -> int:
    vals = _get_all_values_cached(ws)
    if not vals:
        return 2
    for i, row in enumerate(vals[1:], start=2):
        if (len(row) > 0) and str(row[0]).strip() == date_label:
            return i
    return len(vals) + 1

# ===================== 월별 시트 기록 =====================
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
                v = values_by_colname.get("전국", values_by_colname.get("서울", None))
            if v is not None:
                payload.append({"range": f"{sheet_prefix}{a1_col(hmap[possible])}{row_idx}", "values": [[int(v)]]})
            break

    if payload:
        values_batch_update(ws, payload)
        log(f"[ws] {ws.title} -> {date_label} row={row_idx} (wrote {len(payload)} cells incl. date)")

# ===================== 거래요약 =====================
def ym_from_filename(fname: str):
    m = re.search(r"(\d{2})(\d{2})_", fname)
    if not m:
        return None, None, None
    yy, mm = m.group(1), int(m.group(2))
    return f"전국 20{yy}년 {mm}월", f"서울 20{yy}년 {mm}월", f"{yy}/{mm}"

def prev_ym(ym: str) -> str:
    yy, mm = ym.split("/")
    y = int(yy)
    m = int(mm)
    if m == 1:
        return f"{y-1}/12"
    return f"{yy}/{m-1}"

def find_summary_row(ws, ym: str, label: str) -> int:
    vals = _get_all_values_cached(ws)
    if not vals:
        return 2
    for i, row in enumerate(vals[1:], start=2):
        a = str(row[0]).strip() if len(row) > 0 else ""
        b = str(row[1]).strip() if len(row) > 1 else ""
        if a == ym and b == label:
            return i
    return len(vals) + 1

def _norm_colname(s: str) -> str:
    return _norm(s)

def put_summary_line(ws, row_idx: int, ym: str, label: str, line_map: dict):
    header = _retry(ws.row_values, 1)
    if not header:
        _retry(ws.update, [["년월", "구분"] + SUMMARY_COLS], "A1")
        header = ["년월", "구분"] + SUMMARY_COLS

    hmap_norm = {_norm_colname(h): i + 1 for i, h in enumerate(header)}
    sheet_prefix = f"'{ws.title}'!"

    payload = [
        {"range": f"{sheet_prefix}A{row_idx}", "values": [[ym]]},
        {"range": f"{sheet_prefix}B{row_idx}", "values": [[label]]},
    ]

    for k_raw, v in (line_map or {}).items():
        k = _norm_colname(k_raw)
        if k in hmap_norm:
            payload.append({
                "range": f"{sheet_prefix}{a1_col(hmap_norm[k])}{row_idx}",
                "values": [[v]]
            })

    for possible in ("총합계", "합계"):
        pn = _norm_colname(possible)
        if pn in hmap_norm:
            vv = line_map.get("총합계", line_map.get("전국", line_map.get("서울", "")))
            payload.append({"range": f"{sheet_prefix}{a1_col(hmap_norm[pn])}{row_idx}", "values": [[vv if vv != "" else 0]]})
            break

    if payload:
        values_batch_update(ws, payload)

def color_diff_line(ws, row_idx: int, diff_line: dict, header: List[str]):
    hmap = {h: i + 1 for i, h in enumerate(header)}
    reqs = []
    for k, v in diff_line.items():
        if k not in hmap:
            continue
        if v == "" or v == "0":
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
            cur = int(counts.get(c, 0) or 0)
            prv = int(prev_counts.get(c, 0) or 0)
            d = cur - prv
            diffs[c] = f"+{d}" if d > 0 else (str(d) if d < 0 else "0")
    else:
        diffs = {c: "" for c in SUMMARY_COLS}
    r4 = find_summary_row(ws, ym, "전월대비 건수증감")
    put_summary_line(ws, r4, ym, "전월대비 건수증감", diffs)
    header = _retry(ws.row_values, 1)
    color_diff_line(ws, r4, diffs, header)
    log(f"[summary] {ym} 전월대비 -> row={r4}")

# ===== 거래요약: '예상건수' 라인 (초록 Bold) =====
def write_predicted_line(ws_sum: gspread.Worksheet, ym: str, pred_map: dict):
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

# ===================== 압구정동 (원본 그대로 + 변동요약) =====================
APGU_BASE_COLS = [
    "광역", "구", "법정동", "리", "번지", "본번", "부번", "단지명", "전용면적(㎡)",
    "계약년", "계약월", "계약일", "거래금액(만원)", "동", "층",
    "매수자", "매도자", "건축년도", "도로명", "해제사유발생일", "거래유형",
    "중개사소재지", "등기일자", "주택유형"
]

def _apgu_norm(v) -> str:
    return "" if v is None else str(v).strip()

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

    for c in ["계약년", "계약월", "계약일"]:
        if c not in df.columns:
            df[c] = ""
    df = df.sort_values(["계약년", "계약월", "계약일"], ascending=[True, True, True], kind="mergesort")

    vals = _get_all_values_cached(ws) or []
    if not vals:
        _retry(ws.update, [APGU_BASE_COLS], "A1")
        vals = [APGU_BASE_COLS]
    header = vals[0]
    if header != APGU_BASE_COLS:
        _retry(ws.update, [APGU_BASE_COLS], "A1")
        header = APGU_BASE_COLS

    all_now = _get_all_values_cached(ws) or [header]
    body = all_now[1:]
    base_rows_old: List[List[str]] = []
    for r in body:
        if r and r[0] in ("변경구분", "(신규)", "(삭제)"):
            break
        base_rows_old.append((r + [""] * len(header))[:len(header)])

    base_rows_new: List[List[str]] = []
    for _, row in df.iterrows():
        base_rows_new.append([_apgu_norm(row.get(c, "")) for c in APGU_BASE_COLS])

    start = 2
    end = start + len(base_rows_new) - 1
    if end < start:
        end = start
    _ensure_rows(ws, end)
    _retry(ws.update, base_rows_new, f"A{start}:{a1_col(len(header))}{end}")
    log(f"[압구정동] base rows written: {len(base_rows_new)}")

    old_keys = {_apgu_key_from_row_values(r, header) for r in base_rows_old}
    new_keys = {_apgu_key_from_row_values(r, header) for r in base_rows_new}
    added_keys = sorted(list(new_keys - old_keys))
    removed_keys = sorted(list(old_keys - new_keys))

    if not added_keys and not removed_keys:
        log("[압구정동] changes: none")
        return

    def _rowmap(rows: List[List[str]]):
        m = {}
        for r in rows:
            m[_apgu_key_from_row_values(r, header)] = r
        return m

    new_map = _rowmap(base_rows_new)
    old_map = _rowmap(base_rows_old)

    change_header = ["변경구분", "변경일"] + APGU_BASE_COLS
    change_rows: List[List[str]] = [change_header]
    today_str = fmt_kdate(run_day)
    for k in added_keys:
        change_rows.append(["(신규)", today_str] + new_map[k])
    for k in removed_keys:
        change_rows.append(["(삭제)", today_str] + old_map[k])

    start_chg = end + 1
    end_chg = start_chg + len(change_rows) - 1
    _ensure_rows(ws, end_chg)
    _retry(ws.update, change_rows, f"A{start_chg}:{a1_col(len(change_header))}{end_chg}")

    req = {
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": start_chg - 1,
                "endRowIndex": end_chg,
                "startColumnIndex": 0,
                "endColumnIndex": len(change_header)
            },
            "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red": 1.0, "green": 0.0, "blue": 0.0}}}},
            "fields": "userEnteredFormat.textFormat.foregroundColor"
        }
    }
    batch_format(ws, [req])
    log(f"[압구정동] changes: 신규={len(added_keys)} 삭제={len(removed_keys)}")

# ===== 패턴 분석 시트 (표 + 라인차트) =====
def render_pattern_analysis(sh: gspread.Spreadsheet, month_title: str, df_cum: pd.DataFrame, df_inc: pd.DataFrame, targets: List[str]):
    ws = get_or_create_ws(sh, "거래량 패턴분석", rows=1000, cols=40)
    _retry(ws.clear)

    header1 = ["[누적] 날짜"] + targets
    header2 = ["[일증분] 날짜"] + targets
    cum_rows = [header1]
    inc_rows = [header2]

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

    # 차트(oneof 충돌 방지: overlayPosition만 사용)
    nrows = len(cum_rows)
    series = []
    for j in range(len(targets)):
        series.append({
            "series": {
                "sourceRange": {
                    "sources": [{
                        "sheetId": ws.id,
                        "startRowIndex": 1,
                        "endRowIndex": nrows,
                        "startColumnIndex": 1+j,
                        "endColumnIndex": 2+j
                    }]
                }
            },
            "targetAxis": "LEFT_AXIS"
        })
    add_chart = {
        "addChart": {
            "chart": {
                "spec": {
                    "title": f"{month_title} - 누적 등록 추이",
                    "basicChart": {
                        "chartType": "LINE",
                        "legendPosition": "BOTTOM_LEGEND",
                        "axis": [
                            {"position": "BOTTOM_AXIS", "title": "Date"},
                            {"position": "LEFT_AXIS", "title": "Cumulative"}
                        ],
                        "domains": [{
                            "domain": {
                                "sourceRange": {
                                    "sources": [{
                                        "sheetId": ws.id,
                                        "startRowIndex": 1,
                                        "endRowIndex": nrows,
                                        "startColumnIndex": 0,
                                        "endColumnIndex": 1
                                    }]
                                }
                            }
                        }],
                        "series": series,
                        "headerCount": 1
                    }
                },
                "position": {"overlayPosition": {
                    "anchorCell": {"sheetId": ws.id, "rowIndex": 0, "columnIndex": 10},
                    "offsetXPixels": 0, "offsetYPixels": 0, "widthPixels": 800, "heightPixels": 320
                }}
            }
        }
    }
    _retry(ws.spreadsheet.batch_update, {"requests": [add_chart]})
    log("[pattern] 분석 탭 작성 및 차트 추가")

# ===== 월 시트 누적으로 거래건수 읽기 =====
def read_counts_from_month_sheet(ws: gspread.Worksheet) -> Dict[str, int]:
    df_cum = month_sheet_to_frame(ws)
    if df_cum.empty:
        return {}
    last = df_cum.iloc[-1]
    out = {}
    for col in SUMMARY_COLS_N + [TOTAL_N]:
        if col in last.index:
            try:
                out[col] = int(float(last[col] or 0))
            except Exception:
                out[col] = 0
    # 총합계 → 전국 보정
    if TOTAL_N in last.index:
        try:
            out[NATION_N] = int(float(last[TOTAL_N] or out.get(NATION_N, 0)))
        except Exception:
            pass
    return out

# ===== 무자료 달(엑셀 비거나 숫자 없음) 판정 =====
def is_no_data_month(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return True
    need = {"계약년", "계약월"}
    if not need.issubset(set(df.columns)):
        return True
    yy = pd.to_numeric(df["계약년"], errors="coerce")
    mm = pd.to_numeric(df["계약월"], errors="coerce")
    return (yy.dropna().empty or mm.dropna().empty)

# ===================== 시트 목록/탭 탐색 =====================
def list_month_sheets(sh: gspread.Spreadsheet):
    out = {"전국": {}, "서울": {}}
    for ws in sh.worksheets():
        t = ws.title.strip()
        if t.startswith("전국 ") and YM_RE.search(t):
            ym = yymm_from_title(t); out["전국"][ym] = ws
        elif t.startswith("서울 ") and YM_RE.search(t):
            ym = yymm_from_title(t); out["서울"][ym] = ws
    return out

# ===================== 메인 =====================
def main():
    try:
        RUN_LOG.write_text("", encoding="utf-8")
    except Exception:
        pass

    log("[MAIN]")
    log(f"artifacts_dir={ARTIFACTS_DIR}")

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

    # 파일 수집
    files = sorted(Path(ARTIFACTS_DIR).rglob("전국 *.xlsx"))
    log(f"[collect] found {len(files)} xlsx files")

    month_cache = {}  # ym -> {counts, med, mean}
    today_label = kdate(datetime.now())
    run_day = datetime.now().date()
    apgu_all: List[pd.DataFrame] = []

    for p in files:
        nat_title, se_title, ym = ym_from_filename(p.name)
        if not ym:
            continue
        log(f"[file] {p.name} -> nat='{nat_title}' / seoul='{se_title}' / ym={ym}")

        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        # ===== 무자료 달: 0 기록 처리 =====
        if is_no_data_month(df):
            log(f"[no-data] {p.name} -> 0건으로 기록")
            zero_counts = {c: 0 for c in SUMMARY_COLS}
            empty_med   = {c: "" for c in SUMMARY_COLS}
            empty_mean  = {c: "" for c in SUMMARY_COLS}
            month_cache[ym] = {"counts": zero_counts, "med": empty_med, "mean": empty_mean}

            # 전국 탭 0 기록
            ws_nat = fuzzy_ws(sh, nat_title)
            if ws_nat:
                header_nat = _retry(ws_nat.row_values, 1)
                values_nat = {}
                for h in header_nat:
                    if not h or h == "날짜": continue
                    if h == "총합계": values_nat["총합계"] = 0
                    elif h in zero_counts: values_nat[h] = 0
                write_month_sheet(ws_nat, today_label, header_nat, values_nat)

            # 서울 탭 0 기록
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

        # ===== 정상 달 처리 =====
        counts, med, mean = agg_all_stats(df)
        month_cache[ym] = {"counts": counts, "med": med, "mean": mean}

        # 전국 탭 쓰기
        ws_nat = fuzzy_ws(sh, nat_title)
        if ws_nat:
            header_nat = _retry(ws_nat.row_values, 1)
            values_nat: Dict[str, int] = {}
            for h in header_nat:
                if not h or h == "날짜": continue
                if h == "총합계":
                    values_nat["총합계"] = int(counts.get("전국", 0))
                else:
                    if h in counts:
                        values_nat[h] = int(counts[h])
            write_month_sheet(ws_nat, today_label, header_nat, values_nat)

        # 서울 탭 쓰기
        ws_se = fuzzy_ws(sh, se_title)
        if ws_se:
            header_se = _retry(ws_se.row_values, 1)
            values_se: Dict[str, int] = {}
            for h in header_se:
                if not h or h == "날짜": continue
                if h == "총합계":
                    values_se["총합계"] = int(counts.get("서울", 0))
                else:
                    if h in counts:
                        values_se[h] = int(counts[h])
            write_month_sheet(ws_se, today_label, header_se, values_se)

        # 압구정동 원본 누적
        ap = df[(df.get("광역", "") == "서울특별시") & (df.get("법정동", "") == "압구정동")].copy()
        if not ap.empty:
            apgu_all.append(ap)

    # ===== 거래요약: 집계 기록/전월대비 보정 =====
    ws_sum = fuzzy_ws(sh, SUMMARY_SHEET_NAME)
    if ws_sum:
        if month_cache:
            def ym_key(ym):
                yy, mm = ym.split("/")
                return (int(yy), int(mm))
            for ym in sorted(month_cache.keys(), key=ym_key):
                cur = month_cache[ym]
                prv = month_cache.get(prev_ym(ym))
                write_month_summary(
                    ws_sum,
                    2000 + int(ym.split("/")[0]),
                    int(ym.split("/")[1]),
                    cur["counts"], cur["med"], cur["mean"],
                    prv["counts"] if prv else None
                )
                time.sleep(0.15)

        # 최근 12개월 월 시트 누적 → 보정 기록
        month_sheets = [ws for ws in sh.worksheets() if YM_RE.search(ws.title)]
        ym_ws: Dict[str, gspread.Worksheet] = {}
        for ws in month_sheets:
            ym = yymm_from_title(ws.title)
            if ym:
                ym_ws[ym] = ws

        today = datetime.now().date()

        def ym_to_yM(ym: str) -> Tuple[int,int]:
            a, b = ym.split("/")
            return 2000 + int(a), int(b)
        def yM_to_ym(y: int, m: int) -> str:
            return f"{str(y % 100).zfill(2)}/{m}"
        def add_months(y, m, delta):
            nm = m + delta
            y += (nm - 1) // 12
            nm = ((nm - 1) % 12) + 1
            return y, nm

        cur_y, cur_m = today.year, today.month
        last12_yM = [add_months(cur_y, cur_m, -i) for i in range(0, 12)]
        last12_ym = [yM_to_ym(y, m) for (y, m) in last12_yM]

        for ym in last12_ym:
            counts = None; med = None; mean = None; prv_counts = None

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
                    merged = {k: c_nat.get(k, 0) for k in SUMMARY_COLS_N}
                    for k in SUMMARY_COLS_N:
                        if k in c_se:
                            merged[k] = c_se[k]
                    # 역정규화 키로 변환
                    counts = {}
                    for k_n, v in merged.items():
                        # 가능한 한 원래 표기와 맞추기
                        human = next((orig for orig in SUMMARY_COLS if _norm(orig) == k_n), k_n)
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
                        prv_merged = {k: c_nat_prev.get(k, 0) for k in SUMMARY_COLS_N}
                        for k in SUMMARY_COLS_N:
                            if k in c_se_prev:
                                prv_merged[k] = c_se_prev[k]
                        prv_counts = {}
                        for k_n, v in prv_merged.items():
                            human = next((orig for orig in SUMMARY_COLS if _norm(orig) == k_n), k_n)
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
                        cur = int(counts.get(c, 0) or 0)
                        prv = int(prv_counts.get(c, 0) or 0)
                        d = cur - prv
                        diffs[c] = f"+{d}" if d > 0 else (str(d) if d < 0 else "0")
                else:
                    diffs = {c: "" for c in SUMMARY_COLS}
                r_diff = find_summary_row(ws_sum, ym, "전월대비 건수증감")
                put_summary_line(ws_sum, r_diff, ym, "전월대비 건수증감", diffs)
                header_sum = _retry(ws_sum.row_values, 1)
                color_diff_line(ws_sum, r_diff, diffs, header_sum)

    # ===== 압구정동 =====
    if apgu_all:
        ws_ap = fuzzy_ws(sh, "압구정동")
        if ws_ap:
            all_df = pd.concat(apgu_all, ignore_index=True)
            upsert_apgu_verbatim(ws_ap, all_df, run_day)
        else:
            log("[압구정동] sheet not found (skip)")

    # ===== 예측(현재월 포함 최근 3개월) : 학습=2024-10 이후 '존재하는 모든 월' =====
    if ws_sum:
        sheets = list_month_sheets(sh)
        if not sheets["서울"] and not sheets["전국"]:
            log("[predict] no month sheets found"); return

        START_YM_KEY = (2024, 10)
        def ym_to_key(ym: str) -> Tuple[int,int]:
            yy, mm = ym.split("/")
            return (2000 + int(yy), int(mm))

        # 레벨별 학습 결과
        level_curves: Dict[str, Dict[str, List[float]]] = {}
        level_obs_cols: Dict[str, set] = {}
        national_curves_ref: Dict[str, List[float]] = {}

        for level in ["전국", "서울"]:
            pool = {ym: ws for ym, ws in sheets[level].items() if ym_to_key(ym) >= START_YM_KEY}
            if not pool:
                log(f"[predict] '{level}' 학습대상 없음")
                level_curves[level] = {}
                level_obs_cols[level] = set()
                continue

            region_universe: set = set()
            learn_frames: List[Tuple[pd.DataFrame, date]] = []

            for ym, ws in sorted(pool.items(), key=lambda kv: ym_to_key(kv[0])):
                df_cum = month_sheet_to_frame(ws)
                if df_cum.empty:
                    continue
                fday = first_data_date(ws)
                if not fday:
                    continue
                region_universe.update([c for c in df_cum.columns if c != "date"])
                learn_frames.append((df_cum, fday))

            if not learn_frames:
                log(f"[predict] '{level}' 학습 데이터 부족: skip")
                level_curves[level] = {}
                level_obs_cols[level] = set()
                continue

            # 사용할 지역 목록
            if level == "서울":
                region_cols = [c for c in region_universe if c in SEOUL_SET_N or c == TOTAL_N]
            else:
                region_cols = [c for c in region_universe if c in NATION_SET_N or c == TOTAL_N]

            level_obs_cols[level] = set(region_cols)
            if not region_cols:
                log(f"[predict] '{level}' 지역 교집합 비어있음: skip")
                level_curves[level] = {}
                continue

            horizon = 90
            curves = {r: [0.0] * (horizon + 1) for r in region_cols}
            counts = {r: [0] * (horizon + 1) for r in region_cols}

            for (df_cum, fday) in learn_frames:
                cc = completion_curve(df_cum, region_cols, fday, horizon_days=horizon)
                for r in region_cols:
                    for d in range(1, horizon + 1):
                        v = cc[r][d]
                        if v > 0:
                            curves[r][d] += v
                            counts[r][d] += 1

            for r in region_cols:
                for d in range(1, horizon + 1):
                    curves[r][d] = (curves[r][d] / counts[r][d]) if counts[r][d] > 0 else 0.5

            level_curves[level] = curves
            if level == "전국":
                national_curves_ref = curves  # 서울 백업용

        # 최근 3개월(현재월 포함)
        today = datetime.now().date()
        def add_months(y, m, delta):
            nm = m + delta
            y += (nm - 1) // 12
            nm = ((nm - 1) % 12) + 1
            return y, nm
        def key_to_ym(y, m): return f"{str(y % 100).zfill(2)}/{m}"

        cur_y, cur_m = today.year, today.month
        targets = [key_to_ym(*add_months(cur_y, cur_m, -i)) for i in range(0, 3)]
        targets = [ym for ym in targets if ym in sheets["전국"] or ym in sheets["서울"]]
        targets = sorted(set(targets), key=lambda ym: ym_to_key(ym))

        for ym in targets:
    merged_pred: Dict[str, int] = {col: "" for col in SUMMARY_COLS}

    for level in ["전국", "서울"]:
        ws_level = sheets[level].get(ym)
        if not ws_level:
            continue

        df_cum = month_sheet_to_frame(ws_level)
        if df_cum.empty:
            continue
        fday = first_data_date(ws_level)
        lday = latest_data_date(ws_level)
        if not fday or not lday:
            continue

        last_row = df_cum.iloc[-1]
        day_idx = (lday - fday).days + 1
        if day_idx < 1:
            day_idx = 1

        # 1) 학습에서 얻은 지역 집합
        trained_cols = level_obs_cols.get(level, set())

        # 2) 타깃 시트 실제 컬럼
        actual_cols = set(last_row.index)

        # 3) 허용 지역 세트 (서울/전국별)
        allow_set = SEOUL_SET_N if level == "서울" else NATION_SET_N
        allow_set = set(allow_set) | {TOTAL_N}  # 총합계 포함

        # 4) 사용할 지역 = (학습집합이 있으면 그걸, 아니면 타깃의 실제컬럼) ∩ 허용집합
        if trained_cols:
            use_cols = (trained_cols & actual_cols & allow_set)
        else:
            # ★ 백업: 학습이 비었으면 타깃 컬럼으로 예측 강행
            use_cols = (actual_cols & allow_set)

        if not use_cols:
            # 그래도 없으면 스킵
            continue

        curves = level_curves.get(level, {})
        for region_n in use_cols:
            # 관측치
            obs = int(float(last_row.get(region_n, 0)) or 0)

            # 곡선: 레벨 → (전국 백업) → 기본
            curve = curves.get(region_n)
            if curve is None and national_curves_ref:
                curve = national_curves_ref.get(region_n) or national_curves_ref.get(NATION_N)
            if curve is None:
                curve = [0.0] + [0.5]*90  # horizon 90과 맞춘 기본곡선

            pred = blend_predict(obs, day_idx, curve)

            # 거래요약의 사람용 키로 매핑(정규명→원표기)
            human_key = next((orig for orig in SUMMARY_COLS if _norm(orig) == region_n), region_n)
            merged_pred[human_key] = pred

        # 총합계 → '전국'/'서울' 컬럼에도 기록
        if TOTAL_N in last_row.index:
            obs_sum = int(float(last_row.get(TOTAL_N, 0)) or 0)
            sum_curve = curves.get(TOTAL_N)
            if sum_curve is None and national_curves_ref:
                sum_curve = national_curves_ref.get(TOTAL_N) or national_curves_ref.get(NATION_N)
            if sum_curve is None:
                sum_curve = [0.0] + [0.5]*90
            sum_pred = blend_predict(obs_sum, day_idx, sum_curve)
            merged_pred["총합계"] = sum_pred
            if level == "전국":
                merged_pred["전국"] = sum_pred
            if level == "서울":
                merged_pred["서울"] = sum_pred

    write_predicted_line(ws_sum, ym, merged_pred)


        # 패턴 분석 탭: 최신 월 표+그래프
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
