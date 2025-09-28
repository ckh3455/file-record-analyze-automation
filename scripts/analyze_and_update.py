# scripts/analyze_and_update.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, re, sys, json, time, math, random
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime, date
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
    "울산광역시": "울",
    "경기도": "경기도",
    "인천광역시": "인천광역시",
    "강원특별자치도": "강원도",
    "전라북도": "전북",  # 일부 시트 옛표기 보정
    "전북특별자치도": "전북",
    "전라남도": "전남",
    "경상남도": "경남",
    "경상북도": "경북",
    "충청남도": "충남",
    "충청북도": "충북",
    "제주특별자치도": "제주",
}

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
    (LOG_DIR/"latest.log").write_text(((LOG_DIR/"latest.log").read_text(encoding="utf-8") if (LOG_DIR/"latest.log").exists() else "") + line + "\n", encoding="utf-8")

def note_written(s: str):
    (LOG_DIR/"where_written.txt").write_text(((LOG_DIR/"where_written.txt").read_text(encoding="utf-8") if (LOG_DIR/"where_written.txt").exists() else "") + s.rstrip()+"\n", encoding="utf-8")

def fmt_kdate(d: date) -> str:
    # 구글시트 표시와 동일 (예: 2025. 9. 28)
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
# "전국 2509_250928.xlsx" → (yy=25, mm=9)
def parse_file_ym(path_name: str) -> Optional[Tuple[int,int]]:
    m = re.search(r"\s(\d{2})(\d{2})_", path_name)
    if not m: return None
    return int(m.group(1)), int(m.group(2))

def month_titles(yy: int, mm: int) -> Tuple[str,str,str]:
    # 탭명(공백 허용 매칭) + 요약용 "YY/M"
    nat_title = f"전국 {yy}년 {mm}월"
    se_title  = f"서울 {yy}년 {mm}월"
    ym = f"{yy}/{mm}"
    return nat_title, se_title, ym

def prev_ym(ym: str) -> str:
    # "25/9" → "25/8"
    y, m = ym.split("/")
    yy, mm = int(y), int(m)
    if mm == 1: return f"{(yy-1)}/12"
    return f"{yy}/{mm-1}"

# ========================= 데이터 읽기/집계 =========================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=SHEET_NAME_DATA, dtype=str)
    df = df.fillna("")
    # 숫자화
    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # 누락 보정
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

    # 전국
    all_eok = eok_series(df["거래금액(만원)"])
    counts["전국"] = int(len(df))
    if not all_eok.empty:
        med["전국"] = round2(all_eok.median())
        mean["전국"] = round2(all_eok.mean())

    # 광역
    if "광역" in df.columns:
        for prov, sub in df.groupby("광역"):
            key = PROV_TO_SUMMARY.get(str(prov), str(prov))
            if key in counts:
                counts[key] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[key] = round2(s.median())
                    mean[key] = round2(s.mean())

    # 서울 및 자치구
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

    # 압구정동
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
    # 날짜 열에서 target이 있으면 그 자리, 없으면 마지막 아래
    col_vals = _retry(ws.col_values, date_col)
    target = fmt_kdate(day)
    for i in range(1, len(col_vals)):
        v = str(col_vals[i]).strip()
        if not v: continue
        d = parse_sheet_date(v)
        if d and d == day:
            return i+1  # 1-index row
    # append 위치
    used = 1
    for i in range(len(col_vals), 1, -1):
        if str(col_vals[i-1]).strip():
            used = i
            break
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
        # 1차: 그대로
        v = series.get(h)
        if v is None:
            # 2차: 헤더→요약키
            k = HEADER_TO_SUMMARY_KEY.get(h, h)
            v = series.get(k, 0)
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
    # 준비: header 순으로 한 줄 구성
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
        if v.startswith("+"):
            color = {"red":0.0,"green":0.35,"blue":1.0}
        elif v.startswith("-"):
            color = {"red":1.0,"green":0.0,"blue":0.0}
        else:
            continue
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

    # 거래건수(볼드)
    r1 = find_summary_row(ws, ym, "거래건수")
    put_summary_line(ws, r1, ym, "거래건수", counts)
    # 볼드
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

    # 중앙값/평균(억, 소수2자리 문자열로 이미 구성됨)
    r2 = find_summary_row(ws, ym, "중앙값(단위:억)")
    put_summary_line(ws, r2, ym, "중앙값(단위:억)", med)
    log(f"[summary] {ym} 중앙값 -> row={r2}")

    r3 = find_summary_row(ws, ym, "평균가(단위:억)")
    put_summary_line(ws, r3, ym, "평균가(단위:억)", mean)
    log(f"[summary] {ym} 평균가 -> row={r3}")

    # 전월대비 건수증감(+파랑 / -빨강)
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

    # 예상건수(빈칸 유지)
    r5 = find_summary_row(ws, ym, "예상건수")
    blanks = {col:"" for col in SUMMARY_COLS}
    put_summary_line(ws, r5, ym, "예상건수", blanks)
    log(f"[summary] {ym} 예상건수 -> row={r5}")

# ========================= 압구정동(원본+증감 로그) =========================
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
    return _retry(sh.add_worksheet, title=APGU_SHEET_NAME, rows=4000, cols=80)

def upsert_apgu_month(ws: gspread.Worksheet, df_all: pd.DataFrame, yy: int, mm: int, run_day: date):
    cond = (df_all.get("광역","")=="서울특별시") & (df_all.get("법정동","")=="압구정동")
    df = df_all[cond].copy()
    if df.empty:
        log(f"[압구정동] {yy}/{mm} no rows")
        return
    # 대상 월만
    df = df[(df["계약년"]==(2000+yy)) & (df["계약월"]==mm)].copy()
    if df.empty:
        log(f"[압구정동] {yy}/{mm} empty after month filter")
        return

    for c in ["계약년","계약월","계약일"]:
        if c not in df.columns: df[c] = pd.NA
    # 오래된 → 최신
    df = df.sort_values(["계약년","계약월","계약일"], ascending=[True,True,True], kind="mergesort")

    vals = _retry(ws.get_all_values) or []
    if not vals:
        header = list(df.columns) + ["기록일","변경구분"]
        _retry(ws.update, [header], "A1")
        vals = [header]
    else:
        header = vals[0]
        # 헤더에 누락 있으면 합치기
        union = list(dict.fromkeys(header + [c for c in df.columns if c not in header]))
        if "기록일" not in union: union.append("기록일")
        if "변경구분" not in union: union.append("변경구분")
        if union != header:
            _retry(ws.update, [union], "A1")
            header = union
            vals = _retry(ws.get_all_values) or [header]

    idx_map = {h:i for i,h in enumerate(header)}
    def row_to_dict(row):
        return {k:(row[i] if i<len(row) else "") for k,i in idx_map.items()}

    # 기존 키 세트(같은 ym만 비교)
    existing_rows = vals[1:]
    existing = []
    for r in existing_rows:
        d = row_to_dict(r)
        try:
            y = int(float(d.get("계약년","") or 0))
            m = int(float(d.get("계약월","") or 0))
        except Exception:
            continue
        if y%100 == yy and m == mm:
            existing.append(r)
    existing_keys = set(make_key(row_to_dict(r)) for r in existing)

    # 신규
    to_append = []
    today_label = fmt_kdate(run_day)
    for _, r in df.iterrows():
        d = {k:r.get(k,"") for k in header if k in df.columns}
        k = make_key(d)
        if k in existing_keys: continue
        row = [number_or_blank(r.get(col,"")) for col in header if col not in ("기록일","변경구분")]
        row += [today_label, "(신규)"]
        to_append.append(row)
        existing_keys.add(k)

    # 삭제(기존엔 있는데 이번 파일엔 없는 것)
    file_keys = set(make_key({k:r.get(k,"") for k in header if k in df.columns}) for _, r in df.iterrows())
    removed = []
    for r in existing:
        d = row_to_dict(r)
        if make_key(d) not in file_keys:
            row = [d.get(col,"") for col in header]
            row[idx_map["기록일"]] = today_label
            row[idx_map["변경구분"]] = "(삭제)"
            removed.append(row)

    # 시트 하단에 추가
    if to_append or removed:
        vals2 = _retry(ws.get_all_values) or [header]
        start = len(vals2) + 1
        rows = to_append + removed  # 신규 먼저, 삭제 다음
        end = start + len(rows) - 1
        rng = f"A{start}:{a1_col(len(header))}{end}"
        _retry(ws.update, rows, rng)
        # 빨간색
        req = {
            "repeatCell":{
                "range":{"sheetId":ws.id,
                         "startRowIndex":start-1,"endRowIndex":end,
                         "startColumnIndex":0,"endColumnIndex":len(header)},
                "cell":{"userEnteredFormat":{"textFormat":{"foregroundColor":{"red":1.0,"green":0.0,"blue":0.0}}}},
                "fields":"userEnteredFormat.textFormat.foregroundColor"
            }
        }
        batch_format(ws, [req])

    log(f"[압구정동] {yy}/{mm} new={len(to_append)} removed={len(removed)}")

# ========================= 메인 =========================
def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts-dir", default=ART_DIR_DEFAULT)
    ap.add_argument("--sheet-id", default=os.environ.get("SHEET_ID",""))
    ap.add_argument("--sa", default="sa.json")
    args = ap.parse_args()

    # 로그 초기화
    _ensure_logdir()
    (LOG_DIR/"latest.log").write_text("", encoding="utf-8")
    log("[MAIN]")

    # 수집
    work = Path(args.artifacts_dir)
    files = sorted([p for p in work.rglob("전국 *.xlsx") if p.is_file()])
    log(f"[collect] found {len(files)} xlsx files")

    sh = open_sheet(args.sheet_id, args.sa)

    # 오늘(KST)
    today = datetime.now(ZoneInfo("Asia/Seoul")).date()

    # 월별 캐시(거래요약)
    month_cache: Dict[str, Dict] = {}
    apgu_all: List[pd.DataFrame] = []

    for path in files:
        fx = parse_file_ym(path.name)
        if not fx: 
            continue
        yy, mm = fx
        nat_title, se_title, ym = month_titles(yy, mm)
        log(f"[file] {path.name} -> nat='{nat_title}' / seoul='{se_title}' / ym={ym}")

        # 파일 읽기
        df = read_month_df(path)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        # 집계
        counts, med, mean = agg_all_stats(df)

        # ==== 전국/서울 탭: 기존 방식 유지 ====
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

        # 거래요약용 캐시
        month_cache[ym] = {
            "counts": {col:int(counts.get(col,0)) for col in SUMMARY_COLS},
            "med": {col:med.get(col,"") for col in SUMMARY_COLS},
            "mean": {col:mean.get(col,"") for col in SUMMARY_COLS},
            "yy": 2000+yy, "mm": mm
        }

        # 압구정동 축적(원본)
        ap = df[(df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")].copy()
        if not ap.empty:
            apgu_all.append(ap)

    # ==== 거래요약 ====
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

    # ==== 압구정동(월별 원본 + 증감) ====
    if apgu_all:
        ws_ap = ensure_apgu_sheet(sh)
        all_df = pd.concat(apgu_all, ignore_index=True)
        # 각 ym마다 비교·기록
        ym_set = set()
        for path in files:
            fx = parse_file_ym(path.name)
            if fx:
                ym_set.add(fx)
        for yy, mm in sorted(ym_set):
            upsert_apgu_month(ws_ap, all_df, yy, mm, today)

    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {repr(e)}")
        raise
