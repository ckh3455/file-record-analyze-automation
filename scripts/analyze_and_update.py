# -*- coding: utf-8 -*-
from __future__ import annotations

"""
analyze_and_update.py (전체본/수정본)

핵심 수정(이번 이슈 대응):
1) '오늘 날짜' 라벨을 항상 ISO("YYYY-MM-DD")로 기록 (월탭/요약탭 공통)
2) 날짜 행 탐색을 A2:A{MAX_SCAN_ROWS} 고정 범위로 스캔:
   - 시트에 미리 채워진 날짜(혹은 중간 빈칸) 때문에 get_all_values() 길이에 의존하면
     '엉뚱한 아래쪽(row 94 같은 곳)에 추가'되거나, 필터/정렬 상태에서 '안 보이는' 문제가 생김
   - 같은 날짜가 이미 있으면 그 행을 업데이트
   - 없으면 '첫 빈 행'에 추가 (없으면 마지막+1)
3) 쓰기 직후 해당 행을 다시 읽어(verify) 로그에 남김 → "썼는데 값이 안 보인다"를 즉시 판별
4) 집계 시 지역/구 값 strip() 적용(공백/특수공백으로 인한 키 미매칭 방지)

환경변수(깃허브 액션):
- SHEET_ID: 대상 구글시트 ID
- SA_JSON 또는 SA_PATH: 서비스계정 JSON(문자열 또는 파일경로)
- ARTIFACTS_DIR: 기본 'artifacts' (다운로드된 xlsx가 들어있는 루트)
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
ARTIFACTS_DIR = Path(os.environ.get("ARTIFACTS_DIR", "artifacts"))
SUMMARY_SHEET_NAME = "거래요약"
APGU_SHEET_NAME = "압구정동"

MAX_SCAN_ROWS = int(os.environ.get("MAX_SCAN_ROWS", "900"))

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
    print(line, flush=True)
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

TOTAL_N = _norm("총합계")

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

def ws_batch_clear(ws: gspread.Worksheet, ranges: List[str]):
    resp = _retry(ws.batch_clear, ranges)
    _invalidate_cache(ws)
    return resp

def values_batch_update(ws: gspread.Worksheet, data: List[Dict]):
    body = {"valueInputOption": "USER_ENTERED", "data": data}
    resp = _retry(ws.spreadsheet.values_batch_update, body=body)
    _invalidate_cache(ws)
    return resp

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

# ===================== 년월 정규화 =====================
YM_RE = re.compile(r"(\d{4})년\s*(\d{1,2})월")

def yymm_from_title(title: str) -> Optional[str]:
    m = YM_RE.search(title or "")
    if not m:
        return None
    y, mm = int(m.group(1)), int(m.group(2))
    if not (1 <= mm <= 12):
        return None
    return f"{y%100:02d}/{mm:02d}"

def ym_norm(ym: str) -> Optional[str]:
    """Normalize various year-month strings to 'YY/MM' (MM 2-digit)."""
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

def prev_ym(ym: str) -> str:
    ym2 = ym_norm(ym) or ym
    yy, mm = ym2.split("/")
    y = int(yy); m = int(mm)
    if m == 1:
        return f"{(y-1):02d}/12"
    return f"{y:02d}/{(m-1):02d}"



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
    # artifacts xlsx는 통상 sheet_name='data'
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

def _strip_col(df: pd.DataFrame, col: str):
    if col in df.columns:
        df[col] = df[col].astype(str).map(lambda x: str(x).replace("\u3000"," ").strip())
    return df


def _strip_col(df: pd.DataFrame, col: str):
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()


def agg_all_stats(df: pd.DataFrame):
    """월 파일(data 시트)에서 전국/서울/각 광역/각 구 단위 거래건수 + (억) 중앙값/평균을 계산."""
    counts = {col: 0 for col in SUMMARY_COLS}
    med = {col: "" for col in SUMMARY_COLS}
    mean = {col: "" for col in SUMMARY_COLS}

    if df is None or df.empty:
        return counts, med, mean

    df = df.copy()
    _strip_col(df, "광역")
    _strip_col(df, "구")
    _strip_col(df, "법정동")

    counts["전국"] = int(len(df))
    all_eok = eok_series(df.get("거래금액(만원)", []))
    if not all_eok.empty:
        med["전국"] = round2(all_eok.median())
        mean["전국"] = round2(all_eok.mean())

    # 광역 단위
    if "광역" in df.columns:
        for prov, sub in df.groupby("광역"):
            prov = str(prov).strip()
            if prov in counts:
                counts[prov] += int(len(sub))
                se = eok_series(sub.get("거래금액(만원)", []))
                if not se.empty:
                    med[prov] = round2(se.median())
                    mean[prov] = round2(se.mean())

    # 서울 + 구
    seoul = df[df.get("광역", "") == "서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul) > 0:
        se = eok_series(seoul.get("거래금액(만원)", []))
        if not se.empty:
            med["서울"] = round2(se.median())
            mean["서울"] = round2(se.mean())
        if "구" in seoul.columns:
            for gu, sub in seoul.groupby("구"):
                gu = str(gu).strip()
                if gu in counts:
                    counts[gu] += int(len(sub))
                    se2 = eok_series(sub.get("거래금액(만원)", []))
                    if not se2.empty:
                        med[gu] = round2(se2.median())
                        mean[gu] = round2(se2.mean())

    # 압구정동
    ap = seoul[seoul.get("법정동", "") == "압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap) > 0:
        s = eok_series(ap.get("거래금액(만원)", []))
        if not s.empty:
            med["압구정동"] = round2(s.median())
            mean["압구정동"] = round2(s.mean())

    return counts, med, mean



# ===================== 압구정동 탭(스냅샷 + 변동사항) =====================
APGU_FILTER_COL = "법정동"
APGU_FILTER_VALUE = "압구정동"

# '동일거래' 식별키 후보(존재하는 컬럼만 사용)
APGU_KEY_CANDIDATES = [
    "계약년","계약월","계약일",
    "거래금액(만원)",
    "아파트","전용면적","층",
    "도로명","지번",
]

# 탭에 표시할 컬럼 우선순위(존재하는 것만 사용)
APGU_DISPLAY_COLS = [
    "계약년","계약월","계약일",
    "거래금액(만원)",
    "아파트","전용면적","층",
    "도로명","지번",
    "건축년도","해제사유발생일","해제여부","중개사소재지",
    "광역","시군구","법정동",
]

def _safe_col_list(df: pd.DataFrame, cols: List[str]) -> List[str]:
    return [c for c in cols if c in df.columns]

def _build_apgu_key_cols(df: pd.DataFrame) -> List[str]:
    cols = _safe_col_list(df, APGU_KEY_CANDIDATES)
    # 최소한 날짜+금액 정도는 있어야 의미 있음. 부족하면 가능한 모든 컬럼을 키로 사용.
    if len(cols) < 4:
        cols = list(df.columns)
    return cols

def _row_key_from_series(row: pd.Series, key_cols: List[str]) -> str:
    # 문자열 정규화 후 '|' 로 결합
    parts = []
    for c in key_cols:
        v = row.get(c, "")
        parts.append(_norm(str(v)))
    return "|".join(parts)

def _sheet_values_to_df_snapshot(vals: List[List[str]]) -> Tuple[pd.DataFrame, List[str]]:
    """압구정동 시트의 '스냅샷' 영역만 DataFrame으로 복원.
    레이아웃:
      1행: 헤더
      2행~: 스냅샷 데이터
      (빈행) 이후: 변동사항 영역(구분 헤더 등)
    """
    if not vals:
        return pd.DataFrame(), []
    header = [str(x or "").strip() for x in vals[0]]
    if not any(header):
        return pd.DataFrame(), []
    # 스냅샷 종료점 찾기(빈행 또는 '구분' 헤더)
    cut = len(vals)
    for i, r in enumerate(vals[1:], start=2):
        if not r or not any(str(x).strip() for x in r):
            cut = i - 1
            break
        first = str(r[0]).strip()
        if first in ("구분", "변경구분", "변경사항"):
            cut = i - 1
            break
    rows = []
    for r in vals[1:cut]:
        if not r or not any(str(x).strip() for x in r):
            continue
        # 길이 맞추기
        r2 = (r + [""] * len(header))[:len(header)]
        rows.append(r2)
    if not rows:
        return pd.DataFrame(columns=header), header
    df = pd.DataFrame(rows, columns=header)
    return df, header

def _format_change_rows(ws: gspread.Worksheet, start_row: int, n_rows: int, n_cols: int, color_rgb: Tuple[float,float,float]):
    if n_rows <= 0 or n_cols <= 0:
        return
    req = {
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": start_row - 1,
                "endRowIndex": start_row - 1 + n_rows,
                "startColumnIndex": 0,
                "endColumnIndex": n_cols,
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "foregroundColor": {"red": color_rgb[0], "green": color_rgb[1], "blue": color_rgb[2]},
                        "bold": False,
                    }
                }
            },
            "fields": "userEnteredFormat.textFormat.foregroundColor",
        }
    }
    batch_format(ws, [req])

def update_apgu_sheet(sh: gspread.Spreadsheet, df_all: pd.DataFrame, today: date):
    """압구정동 탭을 '스냅샷(전체)'로 매번 재작성하고,
    직전 스냅샷 대비 '신규/삭제'만 변동사항 영역에 색상(파랑/빨강)으로 기록.
    - 변동사항은 누적하지 않음(매번 지우고 새로 작성).
    """
    if df_all is None or df_all.empty or APGU_FILTER_COL not in df_all.columns:
        log("[apgu] no data")
        return

    df = df_all[df_all[APGU_FILTER_COL].astype(str).str.strip() == APGU_FILTER_VALUE].copy()
    if df.empty:
        log("[apgu] filtered empty")
        return

    # 최근 1년 필터(계약년/월/일 기반)
    for c in ["계약년","계약월","계약일"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if all(c in df.columns for c in ["계약년","계약월","계약일"]):
        dt = pd.to_datetime(dict(year=df["계약년"], month=df["계약월"], day=df["계약일"]), errors="coerce")
        df["_dt"] = dt
        cutoff = pd.Timestamp(today - timedelta(days=365))
        df = df[df["_dt"].notna() & (df["_dt"] >= cutoff)].copy()
    if df.empty:
        log("[apgu] last 1y empty")
        return

    # 표시 컬럼 확정
    disp_cols = _safe_col_list(df, APGU_DISPLAY_COLS)
    if not disp_cols:
        disp_cols = list(df.columns)

    # 기존 시트 스냅샷 로드(비교용)
    ws = get_or_create_ws(sh, APGU_SHEET_NAME, rows=3000, cols=max(20, len(disp_cols)+5))
    old_vals = _retry(ws.get_all_values) or []
    _invalidate_cache(ws)
    old_df, old_header = _sheet_values_to_df_snapshot(old_vals)

    # 기존 헤더가 다르면, 비교는 가능한 컬럼 교집합으로
    old_df = old_df.copy()
    if not old_df.empty:
        # 문자열로 통일
        for c in old_df.columns:
            old_df[c] = old_df[c].astype(str)

    # 신규 스냅샷(문자열로 통일)
    snap = df[disp_cols].copy()
    for c in snap.columns:
        snap[c] = snap[c].astype(str).fillna("")

    # 정렬(가능하면 날짜 내림차순)
    if all(c in snap.columns for c in ["계약년","계약월","계약일"]):
        try:
            snap["_sort"] = (
                pd.to_numeric(snap["계약년"], errors="coerce").fillna(0)*10000
                + pd.to_numeric(snap["계약월"], errors="coerce").fillna(0)*100
                + pd.to_numeric(snap["계약일"], errors="coerce").fillna(0)
            )
            snap = snap.sort_values("_sort", ascending=False).drop(columns=["_sort"])
        except Exception:
            pass

    # 키 생성(비교)
    key_cols_new = _build_apgu_key_cols(snap)
    # old 쪽은 key_cols_new 중 존재하는 컬럼으로만 비교
    key_cols_old = [c for c in key_cols_new if c in old_df.columns] if not old_df.empty else key_cols_new

    new_keys = set(_row_key_from_series(r, key_cols_new) for _, r in snap.iterrows())
    old_keys = set(_row_key_from_series(r, key_cols_old) for _, r in old_df.iterrows()) if not old_df.empty else set()

    added_keys = sorted(list(new_keys - old_keys))
    removed_keys = sorted(list(old_keys - new_keys))

    # key -> row 매핑(표시용)
    def build_map(df_map: pd.DataFrame, key_cols: List[str]) -> Dict[str, List[str]]:
        out = {}
        for _, r in df_map.iterrows():
            k = _row_key_from_series(r, key_cols)
            if k not in out:
                out[k] = [str(r.get(c, "")) for c in disp_cols]
        return out

    new_map = build_map(snap, key_cols_new)
    old_map = build_map(old_df, key_cols_old) if not old_df.empty else {}

    # 작성할 전체 값 구성(스냅샷은 매번 전체 재작성)
    values: List[List[str]] = []
    header = disp_cols
    values.append(header)

    # 스냅샷 rows
    for _, r in snap.iterrows():
        values.append([str(r.get(c, "")) for c in header])

    # 변동사항(누적하지 않음: 매번 덮어쓰기)
    change_start_row = len(values) + 2  # 빈행 1개 후, 헤더가 시작
    values.append([""] * len(header))  # 빈행

    change_header = ["구분"] + header
    values.append(change_header)

    change_rows: List[List[str]] = []
    for k in added_keys:
        change_rows.append(["신규"] + new_map.get(k, [""]*len(header)))
    for k in removed_keys:
        change_rows.append(["삭제"] + old_map.get(k, [""]*len(header)))

    # 없으면 헤더만 남기고 종료(그래도 탭은 깨끗하게 유지)
    values.extend(change_rows)

    # 시트 재작성(전체 clear 후 A1부터)
    try:
        ws_clear(ws)
    except Exception:
        pass
    ws_update(ws, values, "A1")
    log(f"[apgu] wrote snapshot rows={len(snap)} added={len(added_keys)} removed={len(removed_keys)}")

    # 포맷: 헤더 볼드/고정, 변동색상
    reqs = []
    # 1행 볼드 + 고정
    reqs.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.textFormat.bold",
        }
    })
    reqs.append({"updateSheetProperties": {"properties": {"sheetId": ws.id, "gridProperties": {"frozenRowCount": 1}}, "fields": "gridProperties.frozenRowCount"}})
    batch_format(ws, reqs)

    # 변동 색상: 신규(파랑), 삭제(빨강)
    # change_header는 values에서 index = len(snap)+3 (1-based)
    change_header_row = 1 + len(snap) + 2  # header(1) + snap + blank(1) + 1
    first_change_row = change_header_row + 1
    n_change = len(change_rows)
    n_cols = len(change_header)

    # 신규는 위쪽부터 len(added_keys), 삭제는 그 다음
    if n_change > 0:
        if added_keys:
            _format_change_rows(ws, first_change_row, len(added_keys), n_cols, (0.0, 0.0, 1.0))
        if removed_keys:
            _format_change_rows(ws, first_change_row + len(added_keys), len(removed_keys), n_cols, (1.0, 0.0, 0.0))

# ===================== 월 시트(전국/서울) 생성/확보 =====================
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
    # A2:.. 기존 데이터만 비움 (헤더 유지)
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
    """Get or create month worksheet.
    If missing, duplicate the nearest previous month sheet (same level) as template,
    then clear data rows below header. Fallback: create from scratch.
    """
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

        ws = fuzzy_ws(sh, title) or _retry(sh.worksheet, title)
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
    log("[MAIN] start")

    sheet_id = os.environ.get("SHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("SHEET_ID 환경변수가 필요합니다.")

    creds = load_creds()
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, sheet_id)

    today_iso = datetime.now().date().isoformat()

    files = collect_input_files(ARTIFACTS_DIR)
    xlsx = [p for p in files if p.suffix.lower() == ".xlsx"]
    log(f"[input] artifacts_root={ARTIFACTS_DIR} xlsx_files={len(xlsx)}")
    if not xlsx:
        log("[input] no xlsx files found. stop.")
        return

    # 거래요약/압구정동 집계에 사용할 원천(전국 data 시트) 프레임 모음
    df_all_frames: List[pd.DataFrame] = []

    # 월별(ym)로 최신 파일만 선택(같은 달에 여러 파일이 있을 수 있음: *_260204 같은 suffix)
    best_by_ym: Dict[str, Path] = {}
    for p in xlsx:
        nat_title, seoul_title, ym = ym_from_filename(p.name)
        if not ym:
            continue
        prev = best_by_ym.get(ym)
        if prev is None or p.stat().st_mtime > prev.stat().st_mtime:
            best_by_ym[ym] = p

    # 최신 12개월만 사용(현재 달 포함)
    # ym은 'YY/MM' -> 정렬키 (2000+YY, MM)
    def ym_key(ym: str):
        yy, mm = ym.split("/")
        return (2000 + int(yy), int(mm))

    ym_sorted = sorted(best_by_ym.keys(), key=ym_key, reverse=True)[:12]
    ym_sorted = sorted(ym_sorted, key=ym_key)  # 처리/기록은 과거→현재 순으로
    log(f"[input] months_to_process={ym_sorted}")

    # summary 누적(월별 거래건수/중앙값/평균)
    summary_rows = []  # (ym, counts, med, mean)

    for ym in ym_sorted:
        p = best_by_ym[ym]
        nat_title, seoul_title, _ = ym_from_filename(p.name)
        if not nat_title:
            continue

        log(f"[file] {p.name}")
        df = read_month_df(p)
        df_all_frames.append(df)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        counts, med, mean = agg_all_stats(df)
        summary_rows.append((ym, counts, med, mean))

        # ---- 전국 월탭 ----
        ws_nat = ensure_month_ws(sh, nat_title, '전국', ym)
        header_nat = ["날짜"] + NATION_REGIONS
        values_nat = {k: int(counts.get(k, 0)) for k in NATION_REGIONS if k != "총합계"}
        values_nat["총합계"] = int(counts.get("전국", 0))
        write_month_sheet(ws_nat, today_iso, header_nat, values_nat, sheet_id)

        # ---- 서울 월탭 ----
        ws_seoul = ensure_month_ws(sh, seoul_title, '서울', ym)
        header_seoul = ["날짜"] + SEOUL_REGIONS
        values_seoul = {k: int(counts.get(k, 0)) for k in SEOUL_REGIONS if k != "총합계"}
        values_seoul["총합계"] = int(counts.get("서울", 0))
        write_month_sheet(ws_seoul, today_iso, header_seoul, values_seoul, sheet_id)

    # ---- 거래요약 탭 업데이트(최소: 거래건수/중앙값/평균만 월별로 기록) ----
    ws_sum = get_or_create_ws(sh, SUMMARY_SHEET_NAME, rows=400, cols=60)

    # 헤더(1행): '구분' + 월들
    months = ym_sorted
    header = ["구분"] + months
    ws_update(ws_sum, [header], "A1")

    # 2~4행: 거래건수/중앙값/평균가(전국)
    # 원하는 구조가 더 복잡하면(지역별) 이 부분을 확장하면 됩니다.
    # 여기서는 '전국'만 예시로 안전 업데이트.
    lookup = {ym: (c, m1, m2) for ym, c, m1, m2 in summary_rows}
    row_map = {
        "전국 거래건수": [],
        "전국 중앙값(억)": [],
        "전국 평균가(억)": [],
        "서울 거래건수": [],
        "서울 중앙값(억)": [],
        "서울 평균가(억)": [],
        "압구정동 거래건수": [],
        "압구정동 중앙값(억)": [],
        "압구정동 평균가(억)": [],
    }

    for ym in months:
        c, md, mn = lookup[ym]
        row_map["전국 거래건수"].append(int(c.get("전국", 0)))
        row_map["전국 중앙값(억)"].append(md.get("전국", ""))
        row_map["전국 평균가(억)"].append(mn.get("전국", ""))
        row_map["서울 거래건수"].append(int(c.get("서울", 0)))
        row_map["서울 중앙값(억)"].append(md.get("서울", ""))
        row_map["서울 평균가(억)"].append(mn.get("서울", ""))
        row_map["압구정동 거래건수"].append(int(c.get("압구정동", 0)))
        row_map["압구정동 중앙값(억)"].append(md.get("압구정동", ""))
        row_map["압구정동 평균가(억)"].append(mn.get("압구정동", ""))

    out_rows = []
    for k, arr in row_map.items():
        out_rows.append([k] + arr)

    ws_update(ws_sum, out_rows, f"A2:{a1_col(len(header))}{len(out_rows)+1}")
    log(f"[summary] wrote rows={len(out_rows)} months={len(months)}")

    
    # ---- 압구정동 탭 업데이트(최근 1년 스냅샷 + 변동사항) ----
    try:
        if df_all_frames:
            df_all = pd.concat(df_all_frames, ignore_index=True)
            update_apgu_sheet(sh, df_all, today)
    except Exception as e:
        log(f"[apgu] ERROR: {e}")
log("[MAIN] done")

if __name__ == "__main__":
    main()
