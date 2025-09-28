# -*- coding: utf-8 -*-
"""
scripts/analyze_and_update.py

- 아티팩트(전국 *.xlsx) 읽기
- 각 월의 '전국/서울' 탭에 당일 거래건수 기록 (해당 날짜행 있으면 업데이트, 없으면 날짜 추가 후 기록)
- '거래요약' 탭에 거래건수(볼드), 중앙값/평균가(소수점 2자리), 전월대비(+파랑/-빨강), 예상건수(빈칸) 기록
- '압구정동' 탭: 원본 행을 중복 없이 누적(기록일 추가), 파일에 없어진 케이스는 (삭제), 새로 생긴 건 (신규)로 하단에 붉은 글자로 로그
- gspread 5.x/6.x 호환 values_batch_update
- Google Sheets API 429 방지: 쓰로틀 + 지수 백오프 + 배치 업데이트
"""

import os, sys, re, json, time, math, random
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Tuple

import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

# ===================== 상수/설정 =====================
LOG_DIR = Path("analyze_report")
ART_DIR_DEFAULT = "artifacts"
SHEET_NAME_DATA = "data"

SUMMARY_SHEET_NAME = "거래요약"

# 요약 탭에 존재해야 하는 열(좌→우 순서 보존)
SUMMARY_COLS = [
    "전국","서울","강남구","압구정동","경기도","인천광역시","세종시","서초구","송파구",
    "용산구","강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구",
    "강서구","강북구","관악구","구로구","금천구","도봉구","동대문구","서대문구","성북구",
    "은평구","중구","중랑구","부산","대구","광주","대전","강원도","경남","경북","전남",
    "전북","충남","충북","제주"
]

# 광역명 표준화 → 요약탭 열명
PROV_MAP = {
    "서울특별시": "서울",
    "세종특별자치시": "세종시",
    "강원특별자치도": "강원도",
    "경상남도": "경남",
    "경상북도": "경북",
    "광주광역시": "광주",
    "대구광역시": "대구",
    "대전광역시": "대전",
    "부산광역시": "부산",
    "울산광역시": "울산",
    "전라남도": "전남",
    "전북특별자치도": "전북",
    "제주특별자치도": "제주",
    "충청남도": "충남",
    "충청북도": "충북",
    "경기도": "경기도",
    "인천광역시": "인천광역시",
}

NEEDED_COLS = [
    "광역","구","법정동","도로명","번지","본번","부번","단지명","전용면적(㎡)",
    "계약년","계약월","계약일","거래금액(만원)","동","층"
]

APGU_SHEET = "압구정동"
APGU_CHANGE_HEADER = ["변경구분", "기록일", "계약년", "계약월", "계약일",
                      "단지명", "전용면적(㎡)", "동", "층", "거래금액(만원)"]

# ===================== 유틸 & 로깅 =====================
def _ensure_log_dir():
    try:
        if LOG_DIR.exists() and LOG_DIR.is_file():
            LOG_DIR.unlink()
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        # 마지막 보루
        try:
            LOG_DIR.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

def log(msg: str):
    _ensure_log_dir()
    ts = datetime.now().strftime("[%H:%M:%S]")
    line = f"{ts} {msg}"
    print(line)
    with open(LOG_DIR/"latest.log","a",encoding="utf-8") as f:
        f.write(line+"\n")

def log_block(title: str):
    log(f"[{title.upper()}]")

def fmt_date_kor(d: datetime) -> str:
    # 구글시트 표시와 일치 (예: 2025. 9. 28)
    return f"{d.year}. {d.month}. {d.day}"

def norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).strip()

def a1_col(idx: int) -> str:
    s = ""
    n = int(idx)
    while n>0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def ym_from_filename(fn: str):
    # '전국 2410_250928.xlsx' → ('전국 24년 10월','서울 24년 10월','24/10')
    m = re.search(r"(\d{2})(\d{2})_", fn)
    if not m:
        return None, None, None
    yy, mm = m.group(1), int(m.group(2))
    nat = f"전국 20{yy}년 {mm}월"
    se = f"서울 20{yy}년 {mm}월"
    ym = f"{yy}/{mm}"
    return nat, se, ym

def prev_ym(ym: str) -> str:
    yy, mm = ym.split("/")
    y = 2000 + int(yy)
    m = int(mm)
    if m == 1:
        return f"{str((y-1)%100).zfill(2)}/12"
    return f"{yy}/{m-1}"

# ===================== Google Sheets I/O (쓰로틀/재시도/배치) =====================
_LAST_TS = 0.0
def _throttle(min_gap=0.45):
    global _LAST_TS
    now = time.time()
    if now - _LAST_TS < min_gap:
        time.sleep(min_gap - (now - _LAST_TS))
    _LAST_TS = time.time()

def _retry(fn, *a, **kw):
    base = 0.8
    for i in range(7):
        try:
            _throttle()
            return fn(*a, **kw)
        except APIError as e:
            s = str(e)
            # 429/5xx 재시도
            if any(x in s for x in ["429", "500", "502", "503"]):
                time.sleep(base * (2**i) + random.uniform(0, 0.3))
                continue
            raise

def _values_batch_update_compat(ws: gspread.Worksheet, body: dict):
    # gspread 6.x
    try:
        return _retry(ws.spreadsheet.values_batch_update, body=body)
    except TypeError:
        # gspread 5.x
        return _retry(ws.client.values_batch_update, ws.spreadsheet.id, body=body)

def batch_values_update(ws: gspread.Worksheet, payload: List[Dict]):
    if not payload:
        return
    body = {"valueInputOption": "USER_ENTERED", "data": payload}
    return _values_batch_update_compat(ws, body)

def batch_format(ws: gspread.Worksheet, requests: List[dict]):
    if not requests:
        return
    return _retry(ws.spreadsheet.batch_update, {"requests": requests})

def ensure_rows(ws: gspread.Worksheet, need_last_row: int):
    # 필요 행이 현재 row_count보다 크면 add_rows
    rc = ws.row_count
    if need_last_row > rc:
        _retry(ws.add_rows, need_last_row - rc)

# ===================== 시트 탐색/캐시 =====================
def fuzzy_ws(sh: gspread.Spreadsheet, wanted: str) -> Optional[gspread.Worksheet]:
    tgt = norm(wanted)
    for ws in sh.worksheets():
        if norm(ws.title) == tgt:
            return ws
    # 완전 일치 못 찾으면 부분 일치 한 번 더
    for ws in sh.worksheets():
        if tgt in norm(ws.title):
            return ws
    return None

class WorksheetCache:
    def __init__(self, ws: gspread.Worksheet):
        self.ws = ws
        self._vals = None
        self._header = None
        self._index = None  # (년월,구분) -> row

    def values(self):
        if self._vals is None:
            self._vals = _retry(self.ws.get_all_values) or []
        return self._vals

    def header(self):
        if self._header is None:
            vals = self.values()
            self._header = (vals[0] if vals else [])
        return self._header

    def row_count(self):
        return len(self.values())

    def build_index(self):
        if self._index is None:
            vals = self.values()
            idx = {}
            for i, r in enumerate(vals[1:], start=2):
                ym = (r[0] if len(r)>0 else "").strip()
                lb = (r[1] if len(r)>1 else "").strip()
                if ym and lb:
                    idx[(ym, lb)] = i
            self._index = idx
        return self._index

    def invalidate(self):
        self._vals = None
        self._header = None
        self._index = None

# ===================== 엑셀 읽기/집계 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=SHEET_NAME_DATA)
    # 중복 컬럼 제거
    df = df.loc[:, ~df.columns.duplicated()].copy()
    # 누락 컬럼 보정
    for c in NEEDED_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    return df.copy()

def eok_series(ser) -> pd.Series:
    try:
        s = pd.to_numeric(ser, errors="coerce")
    except Exception:
        return pd.Series([], dtype=float)
    s = s.dropna()
    if s.empty:
        return pd.Series([], dtype=float)
    return s / 10000.0

def round2(v) -> str:
    try:
        return f"{float(v):.2f}"
    except Exception:
        return ""

def agg_all_stats(df: pd.DataFrame):
    counts = {col:0 for col in SUMMARY_COLS}
    med = {col:"" for col in SUMMARY_COLS}
    mean = {col:"" for col in SUMMARY_COLS}

    # 전국
    counts["전국"] = int(len(df))
    s_all = eok_series(df["거래금액(만원)"])
    if not s_all.empty:
        med["전국"] = round2(s_all.median())
        mean["전국"] = round2(s_all.mean())

    # 광역별
    if "광역" in df.columns:
        for prov, sub in df.groupby("광역"):
            prov_std = PROV_MAP.get(str(prov), str(prov))
            if prov_std in counts:
                counts[prov_std] += int(len(sub))
                ss = eok_series(sub["거래금액(만원)"])
                if not ss.empty:
                    med[prov_std] = round2(ss.median())
                    mean[prov_std] = round2(ss.mean())

    # 서울/자치구/압구정동
    seoul = df[df.get("광역","")=="서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul)>0:
        ss = eok_series(seoul["거래금액(만원)"])
        if not ss.empty:
            med["서울"] = round2(ss.median())
            mean["서울"] = round2(ss.mean())

    if "구" in seoul.columns:
        for gu, sub in seoul.groupby("구"):
            gu = str(gu)
            if gu in counts:
                counts[gu] += int(len(sub))
                ss = eok_series(sub["거래금액(만원)"])
                if not ss.empty:
                    med[gu] = round2(ss.median())
                    mean[gu] = round2(ss.mean())

    ap = seoul[seoul.get("법정동","")=="압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap)>0:
        ss = eok_series(ap["거래금액(만원)"])
        med["압구정동"] = round2(ss.median())
        mean["압구정동"] = round2(ss.mean())

    return counts, med, mean

# ===================== 월별(전국/서울) 탭 기록 =====================
def detect_date_col(header: List[str]) -> int:
    for i, v in enumerate(header, start=1):
        if str(v).strip() == "날짜":
            return i
    return 1

def normalize_sheet_date_cell(v: str):
    s = str(v).strip()
    if not s:
        return None
    m = re.match(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})", s)
    if m:
        y, mo, d = map(int, m.groups())
        return f"{y:04d}-{mo:02d}-{d:02d}"
    m2 = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m2:
        return s
    return None

def find_row_by_date(ws: gspread.Worksheet, date_col: int, dt: datetime) -> Optional[int]:
    vals = _retry(ws.get_all_values) or []
    tgt = f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}"
    for i, row in enumerate(vals[1:], start=2):
        cell = row[date_col-1] if date_col-1 < len(row) else ""
        key = normalize_sheet_date_cell(cell)
        if key == tgt:
            return i
    return None

def write_date_cell(ws: gspread.Worksheet, row_idx: int, dt: datetime):
    label = fmt_date_kor(dt)
    ensure_rows(ws, row_idx)
    _retry(ws.update, [[label]], f"A{row_idx}:A{row_idx}", value_input_option="USER_ENTERED")

def write_counts_to_month_sheet(ws: gspread.Worksheet, dt: datetime,
                                header: List[str], counts_map: dict, sum_col_name="총합계"):
    if not header:
        return
    date_col = detect_date_col(header)
    row = find_row_by_date(ws, date_col, dt)
    if row is None:
        row = (len(_retry(ws.get_all_values) or [])) + 1
        write_date_cell(ws, row, dt)

    hmap = {h: i+1 for i, h in enumerate(header)}
    payload = []
    total = 0
    for h in header:
        if not h or h == "날짜":
            continue
        if h == sum_col_name:
            continue
        v = int(counts_map.get(h, 0) or 0)
        payload.append({"range": f"{a1_col(hmap[h])}{row}", "values": [[v]]})
        if h != "전국":  # 전국 탭에서 총합계는 전국=총합
            total += v
    # 총합계
    if sum_col_name in hmap:
        val = int(counts_map.get("전국", total) or 0)
        payload.append({"range": f"{a1_col(hmap[sum_col_name])}{row}", "values": [[val]]})

    if payload:
        ensure_rows(ws, row)
        batch_values_update(ws, payload)

# ===================== 거래요약 탭 기록 =====================
def build_summary_index(cache: WorksheetCache) -> Dict[Tuple[str,str], int]:
    vals = cache.values()
    idx = {}
    for i, r in enumerate(vals[1:], start=2):
        ym = (r[0] if len(r)>0 else "").strip()
        lb = (r[1] if len(r)>1 else "").strip()
        if ym and lb:
            idx[(ym, lb)] = i
    return idx

def ensure_summary_header(ws: gspread.Worksheet, cache: WorksheetCache):
    if not cache.values():
        _retry(ws.update, [["년월","구분"] + SUMMARY_COLS], "A1")
        cache.invalidate()

def ensure_summary_row(ws: gspread.Worksheet, cache: WorksheetCache,
                       ym: str, label: str) -> int:
    ensure_summary_header(ws, cache)
    idx = cache.build_index()
    if (ym, label) in idx:
        return idx[(ym, label)]
    # 없으면 맨 아래 추가 (A,B만)
    row = cache.row_count() + 1
    ensure_rows(ws, row)
    _retry(ws.update, [[ym, label]], f"A{row}:B{row}", value_input_option="USER_ENTERED")
    cache.invalidate()
    return row

def format_row_bold(ws: gspread.Worksheet, row_idx: int, first_col: int, last_col: int):
    req = {
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": row_idx-1,
                "endRowIndex": row_idx,
                "startColumnIndex": first_col-1,
                "endColumnIndex": last_col
            },
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.textFormat.bold"
        }
    }
    batch_format(ws, [req])

def color_cells_batch(ws: gspread.Worksheet, row: int, col_rgb_list: List[Tuple[int, Tuple[float,float,float]]]):
    if not col_rgb_list:
        return
    reqs = []
    for col, (r,g,b) in col_rgb_list:
        reqs.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": row-1,
                    "endRowIndex": row,
                    "startColumnIndex": col-1,
                    "endColumnIndex": col
                },
                "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red":r,"green":g,"blue":b}}}},
                "fields": "userEnteredFormat.textFormat.foregroundColor"
            }
        })
    batch_format(ws, reqs)

def put_summary_line(ws: gspread.Worksheet, cache: WorksheetCache,
                     row_idx: int, data_map: dict):
    header = cache.header()
    hmap = {h: i+1 for i, h in enumerate(header)}
    payload = []
    for col in SUMMARY_COLS:
        if col in hmap:
            payload.append({"range": f"{a1_col(hmap[col])}{row_idx}",
                            "values": [[data_map.get(col, "")]]})
    if payload:
        ensure_rows(ws, row_idx)
        batch_values_update(ws, payload)
        cache.invalidate()

def write_month_summary(ws: gspread.Worksheet, cache: WorksheetCache,
                        y: int, m: int,
                        counts: dict, med: dict, mean: dict,
                        prev_counts: Optional[dict]):
    ym_label = f"{str(y%100).zfill(2)}/{m}"

    # 거래건수 (볼드)
    r1 = ensure_summary_row(ws, cache, ym_label, "거래건수")
    put_summary_line(ws, cache, r1, counts)
    header = cache.header()
    if header:
        format_row_bold(ws, r1, 3, len(header))
    log(f"[summary] {ym_label} 거래건수 -> row={r1}")

    # 중앙값(단위:억)
    r2 = ensure_summary_row(ws, cache, ym_label, "중앙값(단위:억)")
    put_summary_line(ws, cache, r2, med)
    log(f"[summary] {ym_label} 중앙값 -> row={r2}")

    # 평균가(단위:억)
    r3 = ensure_summary_row(ws, cache, ym_label, "평균가(단위:억)")
    put_summary_line(ws, cache, r3, mean)
    log(f"[summary] {ym_label} 평균가 -> row={r3}")

    # 전월대비 건수증감 (+파랑 / -빨강)
    r4 = ensure_summary_row(ws, cache, ym_label, "전월대비 건수증감")
    diffs = {}
    if prev_counts:
        for k in SUMMARY_COLS:
            cur = int(counts.get(k, 0) or 0)
            prv = int(prev_counts.get(k, 0) or 0)
            d = cur - prv
            diffs[k] = f"+{d}" if d>0 else (str(d) if d<0 else "0")
    else:
        diffs = {k:"" for k in SUMMARY_COLS}
    put_summary_line(ws, cache, r4, diffs)

    # 색상 일괄
    header_map = {h: i+1 for i, h in enumerate(cache.header())}
    jobs = []
    for k, v in diffs.items():
        if k in header_map and v not in ("", "0"):
            c = header_map[k]
            if v.startswith("+"):
                jobs.append((c, (0.0, 0.35, 1.0)))  # 파랑
            elif v.startswith("-"):
                jobs.append((c, (1.0, 0.0, 0.0)))   # 빨강
    color_cells_batch(ws, r4, jobs)
    log(f"[summary] {ym_label} 전월대비 -> row={r4}")

    # 예상건수 (빈칸)
    r5 = ensure_summary_row(ws, cache, ym_label, "예상건수")
    blanks = {k:"" for k in SUMMARY_COLS}
    put_summary_line(ws, cache, r5, blanks)
    log(f"[summary] {ym_label} 예상건수 -> row={r5}")

# ===================== 압구정동 탭 (원본 누적 + 변경 로그) =====================
def make_row_key(d: dict) -> str:
    parts = [
        d.get("계약년",""), d.get("계약월",""), d.get("계약일",""),
        d.get("광역",""), d.get("구",""), d.get("법정동",""),
        d.get("단지명",""), d.get("전용면적(㎡)",""), d.get("층",""),
        d.get("거래금액(만원)","")
    ]
    return "|".join(str(x).strip() for x in parts)

def number_or_blank(v):
    if v is None: return ""
    if isinstance(v, float) and (pd.isna(v) or math.isnan(v)): return ""
    return v

def ensure_apgu_sheet(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, APGU_SHEET)
    if ws:
        return ws
    return _retry(sh.add_worksheet, title=APGU_SHEET, rows=4000, cols=80)

def append_change_log(ws: gspread.Worksheet, added_rows: list[list], removed_rows: list[list], header: list[str]):
    # 맨 아래 붉은 글자 로그
    all_vals = _retry(ws.get_all_values) or []
    start = len(all_vals) + 1
    now_label = fmt_date_kor(datetime.now())

    idx = {h:i for i, h in enumerate(header)}

    def pick(row, col):
        return row[idx[col]] if col in idx and idx[col] < len(row) else ""

    def to_log_row(kind, row):
        return [
            kind, now_label,
            pick(row, "계약년"), pick(row, "계약월"), pick(row, "계약일"),
            pick(row, "단지명"), pick(row, "전용면적(㎡)"), pick(row, "동"),
            pick(row, "층"), pick(row, "거래금액(만원)")
        ]

    rows = []
    for r in added_rows:  rows.append(to_log_row("(신규)", r))
    for r in removed_rows: rows.append(to_log_row("(삭제)", r))
    if not rows:
        return

    end = start + len(rows)
    ensure_rows(ws, end)

    _retry(ws.update, [APGU_CHANGE_HEADER], f"A{start}:J{start}")
    _retry(ws.update, rows, f"A{start+1}:J{end}")

    # 붉은 글자 서식
    req = {
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": start-1,
                "endRowIndex": end,
                "startColumnIndex": 0,
                "endColumnIndex": 10
            },
            "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red":1.0,"green":0.0,"blue":0.0}}}},
            "fields": "userEnteredFormat.textFormat.foregroundColor"
        }
    }
    batch_format(ws, [req])

def upsert_apgu_raw(ws: gspread.Worksheet, df_all: pd.DataFrame):
    cond = (df_all.get("광역","")=="서울특별시") & (df_all.get("법정동","")=="압구정동")
    df = df_all[cond].copy()
    log(f"[압구정동] filtered {len(df)} rows")
    if df.empty:
        log("[압구정동] no rows in files")
        return

    for c in ["계약년","계약월","계약일"]:
        if c not in df.columns:
            df[c] = pd.NA
    df = df.sort_values(["계약년","계약월","계약일"], ascending=[True,True,True], kind="mergesort")

    vals = _retry(ws.get_all_values) or []
    # 헤더 준비(기록일 포함), 시트 헤더 ∪ 파일 컬럼
    if not vals:
        header = list(df.columns) + ["기록일"]
        _retry(ws.update, [header], "A1")
        vals = [header]
    else:
        header = vals[0]
        if "기록일" not in header:
            header = header + ["기록일"]
            _retry(ws.update, [header], "A1")
            vals = _retry(ws.get_all_values) or [header]
        union = list(dict.fromkeys(header + [c for c in df.columns if c not in header]))
        if union != header:
            header = union
            _retry(ws.update, [header], "A1")
            vals = _retry(ws.get_all_values) or [header]

    idx_map = {h:i for i, h in enumerate(header)}
    def row_to_dict(row):
        return {k: (row[i] if i<len(row) else "") for k, i in idx_map.items()}

    # 기존 키/행
    existing_rows = vals[1:]
    existing_keys = set(make_row_key(row_to_dict(r)) for r in existing_rows)

    # 신규 추가(중복 제외) + 기록일 추가
    today_label = fmt_date_kor(datetime.now())
    new_records = []
    for _, r in df.iterrows():
        d = {k: r.get(k, "") for k in header if k in df.columns}
        k = make_row_key(d)
        if k in existing_keys:
            continue
        rec = [number_or_blank(r.get(col, "")) for col in header if col != "기록일"]
        rec.append(today_label)
        new_records.append(rec)
        existing_keys.add(k)

    # 삭제 감지(파일엔 없고 시트엔 있던 것)
    file_keys = set()
    for _, r in df.iterrows():
        d = {k: r.get(k, "") for k in header if k in df.columns}
        file_keys.add(make_row_key(d))

    removed_rows = []
    for r in existing_rows:
        d = row_to_dict(r)
        if make_row_key(d) not in file_keys:
            removed_rows.append([d.get(h,"") for h in header])

    # 신규 행 추가
    if new_records:
        start_row = len(vals) + 1
        end_row = start_row + len(new_records) - 1
        ensure_rows(ws, end_row)
        rng = f"A{start_row}:{a1_col(len(header))}{end_row}"
        batch_values_update(ws, [{"range": rng, "values": new_records}])
        log(f"[압구정동] appended {len(new_records)} rows")
    else:
        log("[압구정동] no new rows to append")

    # 변동 로그
    append_change_log(ws, new_records, removed_rows, header)

# ===================== 메인 =====================
def open_sheet(sheet_id: str, sa_path: str|None):
    log("[gspread] auth")
    if sa_path and Path(sa_path).exists():
        creds = Credentials.from_service_account_file(
            sa_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        )
    else:
        raw = os.environ.get("SA_JSON","").strip()
        if not raw:
            raise RuntimeError("service account not provided")
        creds = Credentials.from_service_account_info(
            json.loads(raw),
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        )
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, sheet_id)
    log("[gspread] spreadsheet opened")
    return sh

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts-dir", default=os.environ.get("ARTIFACTS_DIR", ART_DIR_DEFAULT))
    parser.add_argument("--sheet-id", default=os.environ.get("SHEET_ID",""))
    parser.add_argument("--sa", default=os.environ.get("SA_PATH","sa.json"))
    args = parser.parse_args()

    # 로그 초기화
    _ensure_log_dir()
    (LOG_DIR/"latest.log").write_text("", encoding="utf-8")
    log_block("main")
    log(f"artifacts_dir={args.artifacts_dir}")

    sh = open_sheet(args.sheet_id, args.sa)

    # 파일 수집
    files = sorted(Path(args.artifacts_dir).rglob("전국 *.xlsx"))
    log(f"[collect] found {len(files)} xlsx files")

    month_cache: Dict[Tuple[int,int], Dict] = {}
    apgu_all: List[pd.DataFrame] = []
    write_date = datetime.now()

    # 월별 탭 쓰기 + 요약 집계 수집
    for f in files:
        nat_title, se_title, ym = ym_from_filename(f.name)
        if not ym:
            continue
        log(f"[read] {f.name}")
        df = read_month_df(f)

        counts, med, mean = agg_all_stats(df)
        # 캐시(요약용)
        yy = 2000 + int(ym.split("/")[0])
        mm = int(ym.split("/")[1])
        month_cache[(yy, mm)] = {"counts": counts, "med": med, "mean": mean}

        # 전국/서울 탭 기록 (있을 때만)
        ws_nat = fuzzy_ws(sh, nat_title)
        if ws_nat:
            header_nat = _retry(ws_nat.row_values, 1)
            # 헤더에 존재하는 컬럼만 기록
            values_nat = {}
            for h in header_nat:
                if not h or h == "날짜":
                    continue
                if h == "총합계":
                    values_nat[h] = int(counts.get("전국", 0))
                else:
                    values_nat[h] = int(counts.get(h, 0))
            write_counts_to_month_sheet(ws_nat, write_date, header_nat, values_nat, sum_col_name="총합계")
            log(f"[전국] {ws_nat.title} -> {fmt_date_kor(write_date)}")

        ws_se = fuzzy_ws(sh, se_title)
        if ws_se:
            header_se = _retry(ws_se.row_values, 1)
            values_se = {}
            for h in header_se:
                if not h or h == "날짜":
                    continue
                if h == "총합계":
                    values_se[h] = int(counts.get("서울", 0))
                else:
                    values_se[h] = int(counts.get(h, 0))
            write_counts_to_month_sheet(ws_se, write_date, header_se, values_se, sum_col_name="총합계")
            log(f"[서울] {ws_se.title} -> {fmt_date_kor(write_date)}")

        # 압구정동 원본 누적
        ap = df[(df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")].copy()
        if not ap.empty:
            apgu_all.append(ap)

    # 거래요약 탭 쓰기 (전월대비는 같은 캐시로 계산)
    ws_sum = fuzzy_ws(sh, SUMMARY_SHEET_NAME)
    if ws_sum and month_cache:
        cache_sum = WorksheetCache(ws_sum)
        ensure_summary_header(ws_sum, cache_sum)

        def ym_key(t):
            y, m = t
            return (y, m)

        for (y, m) in sorted(month_cache.keys(), key=ym_key):
            cur = month_cache[(y, m)]
            prev = month_cache.get((y, m-1)) if m>1 else month_cache.get((y-1, 12))
            prev_counts = prev["counts"] if prev else None
            write_month_summary(ws_sum, cache_sum, y, m, cur["counts"], cur["med"], cur["mean"], prev_counts)
            time.sleep(0.2)  # API 부하 분산

    # 압구정동 탭 업서트 + 변동 로그
    if apgu_all:
        ws_ap = ensure_apgu_sheet(sh)
        all_df = pd.concat(apgu_all, ignore_index=True)
        upsert_apgu_raw(ws_ap, all_df)

    log("[main] done")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        try:
            _ensure_log_dir()
            with open(LOG_DIR/"latest.log","a",encoding="utf-8") as f:
                f.write(f"[ERROR] {repr(e)}\n")
        finally:
            print(f"[ERROR] {repr(e)}", file=sys.stderr)
        raise
