# scripts/analyze_and_update.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, re, sys, json, time, math, random
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

# ========================= 기본 설정 =========================
LOG_DIR = Path("analyze_report")
ART_DIR_DEFAULT = "artifacts"
SHEET_NAME_DATA = "data"

SUMMARY_SHEET_NAME = "거래요약"
APGU_SHEET_NAME = "압구정동"

# 거래요약 고정 열(좌→우)
SUMMARY_COLS = [
    "전국","서울","강남구","압구정동","경기도","인천광역시","세종시","서초구","송파구","용산구",
    "강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구","강서구",
    "강북구","관악구","구로구","금천구","도봉구","동대문구","서대문구","성북구","은평구",
    "중구","중랑구","부산","대구","광주","대전","강원도","경남","경북","전남","전북","충남","충북","제주"
]

# 파일에 반드시 있어야 하는 열
NEEDED_COLS = ["광역","구","법정동","계약년","계약월","계약일","거래금액(만원)"]

# 광역 표준화(파일 → 요약키)
PROV_TO_SUMMARY = {
    "서울특별시": "서울",
    "세종특별자치시": "세종시",
    "강원특별자치도": "강원도",
    "경기도": "경기도",
    "인천광역시": "인천광역시",
    "부산광역시": "부산",
    "대구광역시": "대구",
    "광주광역시": "광주",
    "대전광역시": "대전",
    "울산광역시": "울산",
    "전라남도": "전남",
    "전북특별자치도": "전북",
    "경상남도": "경남",
    "경상북도": "경북",
    "충청남도": "충남",
    "충청북도": "충북",
    "제주특별자치도": "제주",
}

# 시트 헤더(광역) → 요약키 (헤더명이 광역 전체명일 때도 매칭되도록)
HEADER_TO_SUMMARY_KEY = {
    "서울특별시": "서울",
    "부산광역시": "부산",
    "대구광역시": "대구",
    "광주광역시": "광주",
    "대전광역시": "대전",
    "울산광역시": "울산",
    "세종특별자치시": "세종시",
    "울산광역시": "울산",
    "경기도": "경기도",
    "인천광역시": "인천광역시",
    "강원특별자치도": "강원도",
    "전라북도": "전북",
    "전북특별자치도": "전북",
    "전라남도": "전남",
    "경상남도": "경남",
    "경상북도": "경북",
    "충청남도": "충남",
    "충청북도": "충북",
    "제주특별자치도": "제주",
}

# 압구정동 변동 로그 열
APGU_CHANGE_HEADER = ["변경구분", "기록일", "계약년", "계약월", "계약일", "단지명", "전용면적(㎡)", "동", "층", "거래금액(만원)"]

# ========================= 로깅 =========================
def _ensure_logdir():
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        if LOG_DIR.exists() and LOG_DIR.is_file():
            LOG_DIR.unlink()
            LOG_DIR.mkdir(parents=True, exist_ok=True)

def _t():
    return datetime.now(ZoneInfo("Asia/Seoul")).strftime("[%H:%M:%S]")

def log(msg: str):
    _ensure_logdir()
    line = f"{_t()} {msg}"
    print(line)
    p = LOG_DIR/"latest.log"
    p.write_text((p.read_text(encoding="utf-8") if p.exists() else "") + line + "\n", encoding="utf-8")

def note_written(s: str):
    p = LOG_DIR/"where_written.txt"
    p.write_text((p.read_text(encoding="utf-8") if p.exists() else "") + s.rstrip()+"\n", encoding="utf-8")

def fmt_kdate(d: date) -> str:
    return f"{d.year}. {d.month}. {d.day}"

# ========================= gspread 공통 =========================
_LAST_TS = 0.0
def _throttle(sec=0.45):
    global _LAST_TS
    now = time.time()
    if now - _LAST_TS < sec:
        time.sleep(sec - (now - _LAST_TS))
    _LAST_TS = time.time()

def _retry(fn, *a, **kw):
    base = 0.8
    for i in range(6):
        try:
            _throttle()
            return fn(*a, **kw)
        except APIError as e:
            s = str(e)
            if any(x in s for x in ["429","500","502","503"]):
                time.sleep(base * (2**i) + random.uniform(0,0.3))
                continue
            raise

def a1_col(n: int) -> str:
    s = ""
    while n>0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def open_sheet(sheet_id: str, sa_path: Optional[str]) -> gspread.Spreadsheet:
    log("[gspread] auth")
    sa_raw = os.environ.get("SA_JSON","").strip()
    if sa_path and Path(sa_path).exists():
        creds = Credentials.from_service_account_file(sa_path,
                  scopes=["https://www.googleapis.com/auth/spreadsheets",
                          "https://www.googleapis.com/auth/drive"])
    elif sa_raw:
        creds = Credentials.from_service_account_info(json.loads(sa_raw),
                  scopes=["https://www.googleapis.com/auth/spreadsheets",
                          "https://www.googleapis.com/auth/drive"])
    else:
        raise RuntimeError("service account not provided")
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, sheet_id)
    log("[gspread] spreadsheet opened")
    return sh

def fuzzy_ws(sh: gspread.Spreadsheet, title: str) -> Optional[gspread.Worksheet]:
    want = re.sub(r"\s+","", title)
    for ws in sh.worksheets():
        if re.sub(r"\s+","", ws.title) == want:
            log(f"[ws] matched: '{ws.title}'")
            return ws
    return None

def values_batch_update(ws: gspread.Worksheet, payload: List[Dict]):
    body = {"valueInputOption":"USER_ENTERED","data": payload}
    return _retry(ws.spreadsheet.values_batch_update, body=body)

def batch_format(ws: gspread.Worksheet, requests: List[dict]):
    if not requests: return
    _retry(ws.spreadsheet.batch_update, {"requests": requests})

# ========================= 파일/탭 파싱 =========================
def parse_file_ym(path_name: str) -> Optional[Tuple[int,int]]:
    m = re.search(r"\s(\d{2})(\d{2})_", path_name)
    if not m: return None
    return int(m.group(1)), int(m.group(2))

def month_titles(yy: int, mm: int) -> Tuple[str,str,str]:
    nat_title = f"전국 {yy}년 {mm}월"
    se_title  = f"서울 {yy}년 {mm}월"
    ym = f"{yy}/{mm}"
    return nat_title, se_title, ym

def prev_ym(ym: str) -> str:
    y, m = ym.split("/")
    yy, mm = int(y), int(m)
    if mm == 1: return f"{(yy-1)}/12"
    return f"{yy}/{mm-1}"

# ========================= 데이터 읽기/집계 =========================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=SHEET_NAME_DATA, dtype=str)
    df = df.fillna("")
    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in NEEDED_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    return df[NEEDED_COLS].copy()

def eok_series(ser: pd.Series) -> pd.Series:
    s = pd.to_numeric(ser, errors="coerce").dropna()
    if s.empty: return pd.Series([], dtype=float)
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
    if df.empty: return counts, med, mean

    all_eok = eok_series(df["거래금액(만원)"])
    counts["전국"] = int(len(df))
    if not all_eok.empty:
        med["전국"] = round2(all_eok.median())
        mean["전국"] = round2(all_eok.mean())

    if "광역" in df.columns:
        for prov, sub in df.groupby("광역"):
            key = PROV_TO_SUMMARY.get(str(prov), str(prov))
            if key in counts:
                counts[key] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[key] = round2(s.median())
                    mean[key] = round2(s.mean())

    seoul = df[df.get("광역","")=="서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul)>0:
        s = eok_series(seoul["거래금액(만원)"])
        if not s.empty:
            med["서울"] = round2(s.median())
            mean["서울"] = round2(s.mean())

    if "구" in seoul.columns:
        for gu, sub in seoul.groupby("구"):
            g = str(gu)
            if g in counts:
                counts[g] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[g] = round2(s.median())
                    mean[g] = round2(s.mean())

    ap = seoul[seoul.get("법정동","")=="압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap)>0:
        s = eok_series(ap["거래금액(만원)"])
        med["압구정동"] = round2(s.median())
        mean["압구정동"] = round2(s.mean())

    return counts, med, mean

# ========================= 월별 탭 기록(유지) =========================
def find_date_col_idx(ws) -> int:
    header = _retry(ws.row_values, 1) or []
    for i, v in enumerate(header, start=1):
        if str(v).strip() == "날짜":
            return i
    return 1

def parse_sheet_date(s: str) -> Optional[date]:
    s = str(s).strip()
    if not s: return None
    for fmt in ("%Y-%m-%d","%Y.%m.%d","%Y.%m. %d","%Y. %m. %d"):
        try: return datetime.strptime(s, fmt).date()
        except Exception: pass
    m = re.match(r"^\s*(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\s*$", s)
    if m:
        y,mo,dd = map(int, m.groups())
        return date(y,mo,dd)
    return None

def find_or_append_date_row(ws, day: date, date_col: int) -> int:
    col_vals = _retry(ws.col_values, date_col)
    target = fmt_kdate(day)
    for i in range(1, len(col_vals)):
        v = str(col_vals[i]).strip()
        if not v: continue
        d = parse_sheet_date(v)
        if d and d == day:
            return i+1
    used = 1
    for i in range(len(col_vals), 1, -1):
        if str(col_vals[i-1]).strip():
            used = i; break
    return used + 1

def build_row_by_header(header: List[str], day: date, series: Dict[str,int]) -> List:
    row=[]; total=0
    for i, h in enumerate(header):
        h = str(h).strip()
        if i==0:
            row.append(fmt_kdate(day)); continue
        if not h:
            row.append(""); continue
        if h in ("총합계","합계","전체 개수"):
            row.append(total); continue
        v = series.get(h)
        if v is None:
            key = HEADER_TO_SUMMARY_KEY.get(h, h)
            v = series.get(key, 0)
        v = int(v or 0)
        row.append(v)
        if h != "전국":
            total += v
    return row

def upsert_row(ws, day: date, values_by_colname: Dict[str,int]) -> Tuple[str,int]:
    header = _retry(ws.row_values, 1) or []
    if not header:
        raise RuntimeError(f"empty header in sheet '{ws.title}'")
    date_col = find_date_col_idx(ws)
    row_idx = find_or_append_date_row(ws, day, date_col)
    mode = "update"
    line = build_row_by_header(header, day, values_by_colname)
    last_col = a1_col(len(header))
    rng = f"A{row_idx}:{last_col}{row_idx}"
    _retry(ws.update, [line], rng)
    return mode, row_idx

# ========================= 거래요약 =========================
def ensure_summary_header(ws):
    vals = _retry(ws.get_all_values) or []
    if not vals:
        _retry(ws.update, [["년월","구분"] + SUMMARY_COLS], "A1")

def find_summary_row(ws, ym: str, label: str) -> int:
    vals = _retry(ws.get_all_values) or []
    if not vals:
        ensure_summary_header(ws)
        vals = [["년월","구분"] + SUMMARY_COLS]
    for i, r in enumerate(vals[1:], start=2):
        a = (r[0].strip() if len(r)>0 else "")
        b = (r[1].strip() if len(r)>1 else "")
        if a==ym and b==label:
            return i
    return len(vals)+1

def put_summary_line(ws, row_idx: int, ym: str, label: str, line_map: dict):
    header = _retry(ws.row_values, 1) or []
    hmap = {h:i+1 for i,h in enumerate(header)}
    payload = [
        {"range": f"A{row_idx}", "values": [[ym]]},
        {"range": f"B{row_idx}", "values": [[label]]},
    ]
    for col in SUMMARY_COLS:
        if col in hmap:
            payload.append({"range": f"{a1_col(hmap[col])}{row_idx}",
                            "values": [[line_map.get(col,"")]]})
    values_batch_update(ws, payload)

def color_diff_row(ws, row_idx: int, diff_line: dict):
    header = _retry(ws.row_values, 1) or []
    hmap = {h:i+1 for i,h in enumerate(header)}
    reqs=[]
    for col, val in diff_line.items():
        if col not in hmap: continue
        v = str(val)
        if not v or v=="0": continue
        color = {"red":0.0,"green":0.35,"blue":1.0} if v.startswith("+") else {"red":1.0,"green":0.0,"blue":0.0}
        c = hmap[col]-1
        reqs.append({
            "repeatCell":{
                "range":{"sheetId":ws.id,
                         "startRowIndex":row_idx-1,"endRowIndex":row_idx,
                         "startColumnIndex":c,"endColumnIndex":c+1},
                "cell":{"userEnteredFormat":{"textFormat":{"foregroundColor":color}}},
                "fields":"userEnteredFormat.textFormat.foregroundColor"
            }
        })
    batch_format(ws, reqs)

def write_month_summary(ws, y: int, m: int,
                        counts: dict, med: dict, mean: dict,
                        prev_counts: Optional[dict]):
    ensure_summary_header(ws)
    ym = f"{y%100}/{m}"

    r1 = find_summary_row(ws, ym, "거래건수")
    put_summary_line(ws, r1, ym, "거래건수", counts)
    hdr = _retry(ws.row_values, 1) or []
    if hdr:
        req = {
            "repeatCell":{
                "range":{"sheetId":ws.id,"startRowIndex":r1-1,"endRowIndex":r1,
                         "startColumnIndex":2,"endColumnIndex":len(hdr)},
                "cell":{"userEnteredFormat":{"textFormat":{"bold":True}}},
                "fields":"userEnteredFormat.textFormat.bold"
            }
        }
        batch_format(ws, [req])
    log(f"[summary] {ym} 거래건수 -> row={r1}")

    r2 = find_summary_row(ws, ym, "중앙값(단위:억)")
    put_summary_line(ws, r2, ym, "중앙값(단위:억)", med)
    log(f"[summary] {ym} 중앙값 -> row={r2}")

    r3 = find_summary_row(ws, ym, "평균가(단위:억)")
    put_summary_line(ws, r3, ym, "평균가(단위:억)", mean)
    log(f"[summary] {ym} 평균가 -> row={r3}")

    r4 = find_summary_row(ws, ym, "전월대비 건수증감")
    diff = {col:"" for col in SUMMARY_COLS}
    if prev_counts:
        for col in SUMMARY_COLS:
            cur = int(counts.get(col,0) or 0)
            prv = int(prev_counts.get(col,0) or 0)
            d = cur - prv
            diff[col] = f"+{d}" if d>0 else (str(d) if d<0 else "0")
    put_summary_line(ws, r4, ym, "전월대비 건수증감", diff)
    color_diff_row(ws, r4, diff)
    log(f"[summary] {ym} 전월대비 -> row={r4}")

    r5 = find_summary_row(ws, ym, "예상건수")
    blanks = {col:"" for col in SUMMARY_COLS}
    put_summary_line(ws, r5, ym, "예상건수", blanks)
    log(f"[summary] {ym} 예상건수 -> row={r5}")

# ========================= 압구정동(원본 전체 + 1년 변동요약) =========================
def make_key(d: dict) -> str:
    parts = [
        d.get("계약년",""), d.get("계약월",""), d.get("계약일",""),
        d.get("광역",""), d.get("구",""), d.get("법정동",""),
        d.get("단지명",""), d.get("전용면적(㎡)",""), d.get("층",""),
        d.get("거래금액(만원)","")
    ]
    return "|".join(str(x).strip() for x in parts)

def number_or_blank(v):
    if v is None: return ""
    if isinstance(v,float) and (math.isnan(v) or pd.isna(v)): return ""
    return v

def ensure_apgu_sheet(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, APGU_SHEET_NAME)
    if ws: return ws
    return _retry(sh.add_worksheet, title=APGU_SHEET_NAME, rows=8000, cols=80)

def _dict_from_row(row: List[str], idx_map: Dict[str,int]) -> dict:
    return {k:(row[i] if i<len(row) else "") for k,i in idx_map.items()}

def _row_date_from_dict(d: dict) -> Optional[date]:
    try:
        y = int(float(d.get("계약년","") or 0))
        m = int(float(d.get("계약월","") or 0))
        dd = int(float(d.get("계약일","") or 0))
        if y and m and dd:
            return date(y, m, dd)
    except Exception:
        pass
    return None

def upsert_apgu_all(ws: gspread.Worksheet, df_all: pd.DataFrame, run_day: date):
    # 1) 압구정동 원본 전부 선별
    cond = (df_all.get("광역","")=="서울특별시") & (df_all.get("법정동","")=="압구정동")
    df = df_all[cond].copy()
    if df.empty:
        log("[압구정동] no rows")
        return

    # 오래된 → 최신
    for c in ["계약년","계약월","계약일"]:
        if c not in df.columns: df[c] = pd.NA
    df = df.sort_values(["계약년","계약월","계약일"], ascending=[True,True,True], kind="mergesort")

    # 2) 헤더 준비(원본 열 + '기록일','변경구분')
    vals = _retry(ws.get_all_values) or []
    if not vals:
        header = list(df.columns) + ["기록일","변경구분"]
        _retry(ws.update, [header], "A1")
        vals = [header]
    else:
        header = vals[0]
        union = list(dict.fromkeys(header + [c for c in df.columns if c not in header]))
        if "기록일" not in union: union.append("기록일")
        if "변경구분" not in union: union.append("변경구분")
        if union != header:
            _retry(ws.update, [union], "A1")
            header = union
            vals = _retry(ws.get_all_values) or [header]

    idx_map = {h:i for i,h in enumerate(header)}
    existing_rows = vals[1:]

    # 3) 시트에 없는 원본행만 "그대로" 추가 (기록일/변경구분은 빈칸)
    existing_keys = set()
    for r in existing_rows:
        d = _dict_from_row(r, idx_map)
        existing_keys.add(make_key(d))

    to_append = []
    for _, r in df.iterrows():
        d = {k:r.get(k,"") for k in header if k in df.columns}
        k = make_key(d)
        if k in existing_keys:
            continue
        row = [number_or_blank(r.get(col,"")) for col in header if col not in ("기록일","변경구분")]
        row += ["", ""]  # 기록일, 변경구분 비움
        to_append.append(row)
        existing_keys.add(k)

    if to_append:
        vals2 = _retry(ws.get_all_values) or [header]
        start = len(vals2) + 1
        end = start + len(to_append) - 1
        rng = f"A{start}:{a1_col(len(header))}{end}"
        _retry(ws.update, to_append, rng)
        log(f"[압구정동] appended={len(to_append)}")

    # 4) 최근 1년 변동 요약(신규/삭제) – 맨 마지막에 한 번만
    # 기간: 오늘-365일 ~ 오늘
    period_start = run_day - timedelta(days=365)
    today_label = fmt_kdate(run_day)

    # 최신 시트 값 다시 읽기
    all_vals = _retry(ws.get_all_values) or []
    if not all_vals:
        return
    header = all_vals[0]
    idx_map = {h:i for i,h in enumerate(header)}
    rows_now = all_vals[1:]

    # 파일 측 키(최근1년)
    file_keys_1y = set()
    file_rows_1y = {}
    for _, r in df.iterrows():
        d = {k:r.get(k,"") for k in header if k in df.columns}
        dt = _row_date_from_dict(d)
        if dt and (period_start <= dt <= run_day):
            k = make_key(d)
            file_keys_1y.add(k)
            file_rows_1y[k] = d

    # 시트 측 키(최근1년)
    sheet_keys_1y = set()
    sheet_rows_1y = {}
    for r in rows_now:
        d = _dict_from_row(r, idx_map)
        dt = _row_date_from_dict(d)
        if dt and (period_start <= dt <= run_day):
            k = make_key(d)
            sheet_keys_1y.add(k)
            sheet_rows_1y[k] = d

    added_keys = file_keys_1y - sheet_keys_1y
    removed_keys = sheet_keys_1y - file_keys_1y

    if not added_keys and not removed_keys:
        log("[압구정동] 1y changes: none")
        return

    # 변동 요약 블록 만들기
    def to_change_row(kind: str, d: dict) -> List[str]:
        return [
            kind, today_label,
            str(d.get("계약년","")), str(d.get("계약월","")), str(d.get("계약일","")),
            str(d.get("단지명","")), str(d.get("전용면적(㎡)","")), str(d.get("동","")), str(d.get("층","")),
            str(d.get("거래금액(만원)",""))
        ]

    rows_change = [APGU_CHANGE_HEADER]
    for k in sorted(added_keys):
        rows_change.append(to_change_row("(신규)", file_rows_1y[k]))
    for k in sorted(removed_keys):
        rows_change.append(to_change_row("(삭제)", sheet_rows_1y[k]))

    start = len(all_vals) + 1
    end = start + len(rows_change) - 1
    rng = f"A{start}:J{end}"
    _retry(ws.update, rows_change, rng)

    # 빨간 글씨
    req = {
        "repeatCell":{
            "range":{"sheetId":ws.id,
                     "startRowIndex":start-1,"endRowIndex":end,
                     "startColumnIndex":0,"endColumnIndex":10},
            "cell":{"userEnteredFormat":{"textFormat":{"foregroundColor":{"red":1.0,"green":0.0,"blue":0.0}}}},
            "fields":"userEnteredFormat.textFormat.foregroundColor"
        }
    }
    batch_format(ws, [req])
    log(f"[압구정동] 1y changes: 신규={len(added_keys)} 삭제={len(removed_keys)}")

# ========================= 메인 =========================
def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts-dir", default=ART_DIR_DEFAULT)
    ap.add_argument("--sheet-id", default=os.environ.get("SHEET_ID",""))
    ap.add_argument("--sa", default="sa.json")
    args = ap.parse_args()

    _ensure_logdir()
    (LOG_DIR/"latest.log").write_text("", encoding="utf-8")
    log("[MAIN]")

    work = Path(args.artifacts_dir)
    files = sorted([p for p in work.rglob("전국 *.xlsx") if p.is_file()])
    log(f"[collect] found {len(files)} xlsx files")

    sh = open_sheet(args.sheet_id, args.sa)
    today = datetime.now(ZoneInfo("Asia/Seoul")).date()

    month_cache: Dict[str, Dict] = {}
    apgu_all: List[pd.DataFrame] = []

    for path in files:
        fx = parse_file_ym(path.name)
        if not fx: 
            continue
        yy, mm = fx
        nat_title, se_title, ym = month_titles(yy, mm)
        log(f"[file] {path.name} -> nat='{nat_title}' / seoul='{se_title}' / ym={ym}")

        df = read_month_df(path)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        counts, med, mean = agg_all_stats(df)

        # 전국/서울 탭 (기존 로직 유지)
        ws_nat = fuzzy_ws(sh, nat_title)
        if ws_nat:
            header_nat = _retry(ws_nat.row_values, 1) or []
            values_nat = {}
            for h in header_nat:
                if not h or h=="날짜": continue
                if h=="총합계":
                    values_nat[h] = int(counts.get("전국",0))
                else:
                    key = HEADER_TO_SUMMARY_KEY.get(h, h)
                    values_nat[h] = int(counts.get(key, counts.get(h,0)))
            mode, row = upsert_row(ws_nat, today, values_nat)
            log(f"[전국] {ws_nat.title} -> {fmt_kdate(today)} {mode} row={row}")
            note_written(f"{ws_nat.title}\t{fmt_kdate(today)}\t{mode}\t{row}")
        else:
            log(f"[전국] sheet not found: '{nat_title}' (skip)")

        ws_se = fuzzy_ws(sh, se_title)
        if ws_se:
            header_se = _retry(ws_se.row_values, 1) or []
            values_se = {}
            for h in header_se:
                if not h or h=="날짜": continue
                if h=="총합계":
                    values_se[h] = int(counts.get("서울",0))
                else:
                    key = HEADER_TO_SUMMARY_KEY.get(h, h)
                    values_se[h] = int(counts.get(key, counts.get(h,0)))
            mode, row = upsert_row(ws_se, today, values_se)
            log(f"[서울] {ws_se.title} -> {fmt_kdate(today)} {mode} row={row}")
            note_written(f"{ws_se.title}\t{fmt_kdate(today)}\t{mode}\t{row}")
        else:
            log(f"[서울] sheet not found: '{se_title}' (skip)")

        # 거래요약 캐시
        month_cache[ym] = {
            "counts": {col:int(counts.get(col,0)) for col in SUMMARY_COLS},
            "med": {col:med.get(col,"") for col in SUMMARY_COLS},
            "mean": {col:mean.get(col,"") for col in SUMMARY_COLS},
            "yy": 2000+yy, "mm": mm
        }

        # 압구정동 전체 누적을 위해 모음
        ap = df[(df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")].copy()
        if not ap.empty:
            apgu_all.append(ap)

    # 거래요약 쓰기
    ws_sum = fuzzy_ws(sh, SUMMARY_SHEET_NAME)
    if ws_sum and month_cache:
        def ym_key(ym): 
            y,m = ym.split("/")
            return (int(y), int(m))
        for ym in sorted(month_cache.keys(), key=ym_key):
            cur = month_cache[ym]
            prv = month_cache.get(prev_ym(ym))
            write_month_summary(ws_sum, cur["yy"], cur["mm"],
                                cur["counts"], cur["med"], cur["mean"],
                                prv["counts"] if prv else None)

    # 압구정동: 원본 전체 추가 + 1년 변동요약
    if apgu_all:
        ws_ap = ensure_apgu_sheet(sh)
        all_df = pd.concat(apgu_all, ignore_index=True)
        upsert_apgu_all(ws_ap, all_df, today)

    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {repr(e)}")
        raise
