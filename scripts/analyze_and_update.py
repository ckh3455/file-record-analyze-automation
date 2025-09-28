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

# ===================== 로깅 =====================
LOG_DIR = Path("analyze_report")
def _ensure_logdir():
    try:
        if LOG_DIR.exists() and not LOG_DIR.is_dir():
            LOG_DIR.unlink()
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
_ensure_logdir()

RUN_LOG = LOG_DIR / f"run-{datetime.now(ZoneInfo('Asia/Seoul')).strftime('%Y%m%dT%H%M%S%z')}.log"
LATEST  = LOG_DIR / "latest.log"
WRITTEN = LOG_DIR / "where_written.txt"

def _t(): return datetime.now(ZoneInfo("Asia/Seoul")).strftime("[%H:%M:%S]")

def _w(line: str):
    print(line)
    try:
        with RUN_LOG.open("a", encoding="utf-8") as f: f.write(line + "\n")
        with LATEST.open("a", encoding="utf-8") as f: f.write(line + "\n")
    except Exception:
        pass

def log(msg: str): _w(f"{_t()} {msg}")
def log_error(msg: str, exc: Optional[BaseException] = None):
    _w(f"{_t()} [ERROR] {msg}")
    if exc:
        import traceback
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        print(tb, file=sys.stderr)
        try:
            with RUN_LOG.open("a", encoding="utf-8") as f: f.write(tb + "\n")
            with LATEST.open("a", encoding="utf-8") as f: f.write(tb + "\n")
        except Exception:
            pass
def note_written(s: str):
    try:
        with WRITTEN.open("a", encoding="utf-8") as f: f.write(s.rstrip() + "\n")
    except Exception:
        pass

# ===================== 공통 유틸 =====================
def norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).strip()

def kdate_str(d: date) -> str:
    return f"{d.year}. {d.month}. {d.day}"

def parse_any_date(s: str) -> Optional[date]:
    if s is None: return None
    s = str(s).strip()
    if not s: return None
    for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%Y.%m. %d", "%Y. %m. %d"):
        try: return datetime.strptime(s, fmt).date()
        except Exception: pass
    m = re.match(r"^\s*(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\s*$", s)
    if m:
        y, mn, dd = map(int, m.groups())
        return date(y, mn, dd)
    return None

def ym_key(ym: str) -> Tuple[int,int]:
    yy, mm = ym.split("/")
    return (int(yy), int(mm))

def prev_ym(ym: str) -> str:
    yy, mm = ym.split("/")
    y = int(yy); m = int(mm)
    if m == 1:
        return f"{y-1}/12"
    return f"{yy}/{m-1}"

# ===================== 파일명 파서 =====================
# 예: '전국 2507_250928.xlsx'
FN_RE = re.compile(r".*?(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})\.xlsx$")
def parse_filename(fname: str) -> Tuple[int,int,int]:
    m = FN_RE.match(fname)
    if not m: raise ValueError(f"unexpected filename: {fname}")
    y = 2000 + int(m.group(1))
    mth = int(m.group(2))
    day = int(m.group(5))
    return y, mth, day

def titles_from_y_m(y: int, m: int) -> Tuple[str, str, str]:
    # 시트 탭명은 2자리 연도 포맷 (예: '전국 25년 7월')
    nat = f"전국 {y%100}년 {m}월"
    se  = f"서울 {y%100}년 {m}월"
    ym  = f"{str(y%100).zfill(2)}/{m}"
    return nat, se, ym

# ===================== 시트 접근/호출 래퍼 =====================
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
            if any(x in s for x in ("429","500","502","503")):
                time.sleep(base * (2**i) + random.uniform(0,0.3))
                continue
            raise

def open_sheet(sheet_id: str, sa_path: Optional[str]) -> gspread.Spreadsheet:
    sa_raw = os.environ.get("SA_JSON","").strip()
    if sa_path and Path(sa_path).exists():
        creds = Credentials.from_service_account_file(sa_path,
                 scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])
    elif sa_raw:
        creds = Credentials.from_service_account_info(json.loads(sa_raw),
                 scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])
    else:
        raise RuntimeError("Service Account not provided")
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    log("[gspread] spreadsheet opened")
    return sh

def fuzzy_ws(sh: gspread.Spreadsheet, wanted: str) -> Optional[gspread.Worksheet]:
    tgt = norm(wanted)
    for ws in sh.worksheets():
        if norm(ws.title) == tgt:
            log(f"[ws] matched: '{ws.title}'")
            return ws
    return None

def ensure_rows(ws: gspread.Worksheet, need_last_row: int):
    if ws.row_count < need_last_row:
        _retry(ws.add_rows, need_last_row - ws.row_count)

def a1_col(idx: int) -> str:
    s=""; n=idx
    while n>0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def values_batch_update(ws: gspread.Worksheet, payload: List[dict]):
    """모든 range에 시트명을 강제 접두해 다른 시트로 새는 문제 방지."""
    fixed=[]
    t = ws.title
    for item in payload:
        rng = item["range"]
        if "!" not in rng:
            rng = f"'{t}'!{rng}"
        fixed.append({"range": rng, "values": item["values"]})
    body = {"valueInputOption": "USER_ENTERED", "data": fixed}
    return _retry(ws.spreadsheet.values_batch_update, body=body)

def batch_format(ws: gspread.Worksheet, requests: List[dict]):
    if not requests: return
    _retry(ws.spreadsheet.batch_update, {"requests": requests})

# ===================== 데이터 표준화/집계 =====================
NEEDED_COLS = ["광역","구","법정동","계약년","계약월","계약일","거래금액(만원)"]

PROV_MAP = {  # 요약 열명과 매핑
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

SUMMARY_COLS = [
    "전국","서울","강남구","압구정동","경기도","인천광역시","세종시","서초구","송파구","용산구",
    "강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구","강서구",
    "강북구","관악구","구로구","금천구","도봉구","동대문구","서대문구","성북구","은평구",
    "중구","중랑구","부산","대구","광주","대전","강원도","경남","경북","전남","전북","충남","충북","제주"
]

def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data", dtype=str)
    df = df.fillna("")
    # 숫자화
    for c in ("계약년","계약월","계약일","거래금액(만원)"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # 누락 보정
    for c in NEEDED_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    return df[NEEDED_COLS].copy()

def eok_series(ser) -> pd.Series:
    s = pd.to_numeric(ser, errors="coerce").dropna()
    if s.empty: return pd.Series([], dtype=float)
    return s/10000.0

def round2(v) -> str:
    try: return f"{float(v):.2f}"
    except Exception: return ""

def agg_all_stats(df: pd.DataFrame):
    counts = {col:0 for col in SUMMARY_COLS}
    med = {col:"" for col in SUMMARY_COLS}
    mean= {col:"" for col in SUMMARY_COLS}

    # 전국
    all_eok = eok_series(df["거래금액(만원)"])
    counts["전국"] = int(len(df))
    if not all_eok.empty:
        med["전국"]  = round2(all_eok.median())
        mean["전국"] = round2(all_eok.mean())

    # 광역
    for prov, sub in df.groupby("광역"):
        prov_std = PROV_MAP.get(str(prov), str(prov))
        if prov_std in counts:
            counts[prov_std] += int(len(sub))
            s = eok_series(sub["거래금액(만원)"])
            if not s.empty:
                med[prov_std]  = round2(s.median())
                mean[prov_std] = round2(s.mean())

    # 서울/구
    seoul = df[df.get("광역","") == "서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul)>0:
        s = eok_series(seoul["거래금액(만원)"])
        if not s.empty:
            med["서울"]  = round2(s.median())
            mean["서울"] = round2(s.mean())
    if "구" in seoul.columns:
        for gu, sub in seoul.groupby("구"):
            gu = str(gu)
            if gu in counts:
                counts[gu] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[gu]  = round2(s.median())
                    mean[gu] = round2(s.mean())

    # 압구정동
    ap = seoul[seoul.get("법정동","")=="압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap)>0:
        s = eok_series(ap["거래금액(만원)"])
        med["압구정동"]  = round2(s.median())
        mean["압구정동"] = round2(s.mean())

    return counts, med, mean

# ===================== 월별 탭 쓰기 (성공 버전 유지) =====================
def find_date_col_idx(ws) -> int:
    header = _retry(ws.row_values, 1) or []
    for i, v in enumerate(header, start=1):
        if str(v).strip() == "날짜":
            return i
    return 1

def find_date_row(ws, target: date, date_col_idx: int = 1, header_row: int = 1) -> Optional[int]:
    col_vals = _retry(ws.col_values, date_col_idx) or []
    for i, v in enumerate(col_vals[header_row:], start=header_row+1):
        d = parse_any_date(v)
        if d and d == target:
            return i
    return None

def build_row_by_header(header: List[str], day: date, series: Dict[str,int]) -> List:
    row=[]; total=0
    for i, h in enumerate(header):
        h = str(h).strip()
        if i==0:
            row.append(kdate_str(day)); continue
        if not h:
            row.append(""); continue
        if h in ("총합계","합계","전체 개수"):
            row.append(total); continue
        v = int(series.get(h, 0))
        row.append(v)
        if h != "전국":  # 전국 탭의 총합계는 나머지 합, 서울 탭은 서울 합, 여기선 기존 합산 유지
            total += v
    return row

def upsert_row(ws, day: date, series: Dict[str,int]) -> Tuple[str,int]:
    header = _retry(ws.row_values, 1) or []
    if not header: raise RuntimeError(f"empty header in sheet '{ws.title}'")
    date_col = find_date_col_idx(ws)
    row_idx = find_date_row(ws, day, date_col_idx=date_col, header_row=1)
    mode = "update" if row_idx else "append"
    if not row_idx:
        col_vals = _retry(ws.col_values, date_col) or []
        used = 1
        for i in range(len(col_vals), 1, -1):
            if str(col_vals[i-1]).strip():
                used = i; break
        row_idx = used + 1
    ensure_rows(ws, row_idx)
    last_col = a1_col(len(header))
    rng = f"A{row_idx}:{last_col}{row_idx}"
    values_batch_update(ws, [{"range": rng, "values": [build_row_by_header(header, day, series)]}])
    return mode, row_idx

# ===================== 거래요약 쓰기 =====================
SUMMARY_SHEET = "거래요약"

def ensure_summary_header(ws):
    vals = _retry(ws.get_all_values) or []
    if not vals:
        header = ["년월","구분"] + SUMMARY_COLS
        values_batch_update(ws, [{"range": "A1", "values": [header]}])

def find_summary_row(ws, ym: str, label: str) -> int:
    vals = _retry(ws.get_all_values) or []
    if not vals: return 2
    for i, row in enumerate(vals[1:], start=2):
        a = (row[0] if len(row)>0 else "").strip()
        b = (row[1] if len(row)>1 else "").strip()
        if a==ym and b==label:
            return i
    return len(vals) + 1  # 헤더는 보존

def color_cells(ws, row: int, col_rgb_list: List[Tuple[int,Tuple[float,float,float]]]):
    if not col_rgb_list: return
    reqs=[]
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

def write_month_summary(ws: gspread.Worksheet, y: int, m: int,
                        counts: dict, med: dict, mean: dict,
                        prev_counts: Optional[dict]):
    ensure_summary_header(ws)
    header = _retry(ws.row_values, 1) or []
    hmap = {h: i+1 for i,h in enumerate(header)}
    ym = f"{str(y%100).zfill(2)}/{m}"

    def put_line(row_idx: int, label: str, line_map: dict):
        ensure_rows(ws, row_idx)
        payload = [
            {"range": f"A{row_idx}:B{row_idx}", "values": [[ym, label]]}
        ]
        for col_name in SUMMARY_COLS:
            if col_name in hmap:
                c = hmap[col_name]
                payload.append({"range": f"{a1_col(c)}{row_idx}",
                                "values": [[line_map.get(col_name, "")]]})
        values_batch_update(ws, payload)

    # 1) 거래건수(볼드)
    r1 = find_summary_row(ws, ym, "거래건수"); put_line(r1, "거래건수", counts)
    # 볼드
    if header:
        req = {
            "repeatCell": {
                "range":{
                    "sheetId": ws.id,
                    "startRowIndex": r1-1,
                    "endRowIndex": r1,
                    "startColumnIndex": 2-1,
                    "endColumnIndex": len(header)
                },
                "cell":{"userEnteredFormat":{"textFormat":{"bold": True}}},
                "fields":"userEnteredFormat.textFormat.bold"
            }
        }
        batch_format(ws, [req])
    log(f"[summary] {ym} 거래건수 -> row={r1}")

    # 2) 중앙값(억)
    r2 = find_summary_row(ws, ym, "중앙값(단위:억)"); put_line(r2, "중앙값(단위:억)", med)
    log(f"[summary] {ym} 중앙값 -> row={r2}")

    # 3) 평균가(억)
    r3 = find_summary_row(ws, ym, "평균가(단위:억)"); put_line(r3, "평균가(단위:억)", mean)
    log(f"[summary] {ym} 평균가 -> row={r3}")

    # 4) 전월대비
    diffs = {}
    if prev_counts:
        for k in SUMMARY_COLS:
            cur = int(counts.get(k,0) or 0)
            prv = int(prev_counts.get(k,0) or 0)
            d = cur - prv
            diffs[k] = f"+{d}" if d>0 else (str(d) if d<0 else "0")
    else:
        diffs = {k:"" for k in SUMMARY_COLS}
    r4 = find_summary_row(ws, ym, "전월대비 건수증감"); put_line(r4, "전월대비 건수증감", diffs)

    # +파랑/-빨강 칠하기
    color_jobs=[]
    for k, v in diffs.items():
        if not v or k not in hmap: continue
        col = hmap[k]
        if v.startswith("+"): color_jobs.append((col, (0.0,0.35,1.0)))
        elif v.startswith("-"): color_jobs.append((col, (1.0,0.0,0.0)))
    color_cells(ws, r4, color_jobs)
    log(f"[summary] {ym} 전월대비 -> row={r4}")

    # 5) 예상건수 (빈칸)
    blanks = {k:"" for k in SUMMARY_COLS}
    r5 = find_summary_row(ws, ym, "예상건수"); put_line(r5, "예상건수", blanks)
    log(f"[summary] {ym} 예상건수 -> row={r5}")

# ===================== 압구정동 탭 (원본 행 + (신규)/(삭제) 로그) =====================
APGU_SHEET = "압구정동"
APGU_CHANGE_HEADER = ["변경구분","기록일","계약년","계약월","계약일","단지명","전용면적(㎡)","동","층","거래금액(만원)"]

def ensure_apgu_sheet(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, APGU_SHEET)
    if ws: return ws
    return _retry(sh.add_worksheet, title=APGU_SHEET, rows=4000, cols=80)

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
    if isinstance(v,float) and (pd.isna(v) or math.isnan(v)): return ""
    return v

def upsert_apgu_month(ws: gspread.Worksheet, df_all: pd.DataFrame, y: int, m: int, run_day: date):
    # 이번 파일의 해당 월(압구정동)만
    cond = (df_all.get("광역","")=="서울특별시") & (df_all.get("법정동","")=="압구정동")
    ap = df_all[cond].copy()
    ap = ap[(ap["계약년"]==y) & (ap["계약월"]==m)]
    log(f"[압구정동] {y%100}/{m} rows in artifact: {len(ap)}")
    if ap.empty: 
        log(f"[압구정동] {y%100}/{m} no data in file(s)")
        return

    for c in ("계약년","계약월","계약일"):
        if c not in ap.columns: ap[c]=pd.NA
    ap = ap.sort_values(["계약년","계약월","계약일","거래금액(만원)"], ascending=[True,True,True,True], kind="mergesort")

    vals = _retry(ws.get_all_values) or []
    if not vals:
        header = list(ap.columns) + ["기록일"]
        values_batch_update(ws, [{"range": "A1", "values": [header]}])
        vals = [header]
    else:
        header = vals[0]
        if "기록일" not in header:
            header = header + ["기록일"]
            values_batch_update(ws, [{"range": "A1", "values": [header]}])
            vals = _retry(ws.get_all_values) or [header]
        # 헤더 확장(새 컬럼 등장시)
        union = list(dict.fromkeys(header + [c for c in ap.columns if c not in header]))
        if union != header:
            header = union
            values_batch_update(ws, [{"range": "A1", "values": [header]}])
            vals = _retry(ws.get_all_values) or [header]

    idx = {h:i for i,h in enumerate(header)}
    # 기존 이 월 데이터만 추출 (계약년/월 기준)
    existing_rows = []
    for r in vals[1:]:
        try:
            yy = int(float(r[idx["계약년"]])) if "계약년" in idx and idx["계약년"]<len(r) and r[idx["계약년"]] else -1
            mm = int(float(r[idx["계약월"]])) if "계약월" in idx and idx["계약월"]<len(r) and r[idx["계약월"]] else -1
            if yy==y and mm==m:
                existing_rows.append(r)
        except Exception:
            continue

    def row_to_dict(row):
        return {k: (row[i] if i<len(row) else "") for k,i in idx.items()}

    exist_keys = set()
    for r in existing_rows:
        exist_keys.add(make_row_key(row_to_dict(r)))

    file_keys = set()
    for _, r in ap.iterrows():
        d = {k: r.get(k, "") for k in header if k in ap.columns}
        file_keys.add(make_row_key(d))

    # 신규 / 삭제 감지
    new_records = []
    today_label = kdate_str(run_day)
    for _, r in ap.iterrows():
        d = {k: r.get(k, "") for k in header if k in ap.columns}
        k = make_row_key(d)
        if k in exist_keys: continue
        rec = [number_or_blank(r.get(col,"")) for col in header if col!="기록일"]
        rec.append(today_label)
        new_records.append(rec)

    removed_records = []
    for r in existing_rows:
        d = row_to_dict(r)
        if make_row_key(d) not in file_keys:
            removed_records.append([d.get(h,"") for h in header])

    # 신규 행 추가(기존 데이터 보존, 중복만 회피)
    if new_records:
        start = len(vals) + 1
        end   = start + len(new_records) - 1
        ensure_rows(ws, end)
        rng = f"A{start}:{a1_col(len(header))}{end}"
        values_batch_update(ws, [{"range": rng, "values": new_records}])

    # 변동 로그 (맨 아래 빨간 글씨)
    logs = []
    def to_log(kind, row):
        d = {h:(row[i] if i<len(row) else "") for h,i in idx.items()}
        return [
            kind, today_label,
            d.get("계약년",""), d.get("계약월",""), d.get("계약일",""),
            d.get("단지명",""), d.get("전용면적(㎡)",""), d.get("동",""),
            d.get("층",""), d.get("거래금액(만원)","")
        ]
    for r in new_records:  # r는 방금 쓴 레코드의 값 배열이므로 로그는 file 기반으로 재구성
        # 값 배열을 헤더 dict로 복원
        d = {h: r[i] if i < len(r) else "" for i,h in enumerate([h for h in header if h!="기록일"])}
        # 기록일은 이미 r 끝에 있으나 로그에는 별도 컬럼으로 넣음
        logs.append(["(신규)", today_label,
                     d.get("계약년",""), d.get("계약월",""), d.get("계약일",""),
                     d.get("단지명",""), d.get("전용면적(㎡)",""), d.get("동",""),
                     d.get("층",""), d.get("거래금액(만원)","")])
    for r in removed_records:
        logs.append(to_log("(삭제)", r))

    if logs:
        all_vals = _retry(ws.get_all_values) or []
        start = len(all_vals) + 1
        end   = start + len(logs)
        ensure_rows(ws, end)
        values_batch_update(ws, [
            {"range": f"A{start}:J{start}", "values": [APGU_CHANGE_HEADER]},
            {"range": f"A{start+1}:J{end}", "values": logs},
        ])
        # 빨간 글씨
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
    log(f"[압구정동] {y%100}/{m} new={len(new_records)} removed={len(removed_records)}")

# ===================== 메인 =====================
def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts-dir", required=True)
    ap.add_argument("--sheet-id", required=True)
    ap.add_argument("--sa", default="sa.json")
    args = ap.parse_args()

    # latest.log 초기화
    try:
        if LATEST.exists(): LATEST.unlink()
        if WRITTEN.exists(): WRITTEN.unlink()
    except Exception: pass

    log("[MAIN]")
    work = Path(args.artifacts_dir)
    files = sorted([p for p in work.rglob("전국 *.xlsx") if p.is_file()])
    log(f"[collect] found {len(files)} xlsx files")
    sh = open_sheet(args.sheet_id, args.sa)

    today_kst = datetime.now(ZoneInfo("Asia/Seoul")).date()

    # 월별 요약 캐시(전월대비 계산용)
    month_cache: Dict[str, Dict] = {}
    apgu_all: List[pd.DataFrame] = []

    for path in files:
        try:
            y, m, file_day = parse_filename(path.name)
        except Exception as e:
            log_error(f"filename parse failed: {path.name}", e); continue

        nat_title, se_title, ym = titles_from_y_m(y, m)
        log(f"[file] {path.name} -> nat='{nat_title}' / seoul='{se_title}' / ym={ym}")

        try:
            df = read_month_df(path)
        except Exception as e:
            log_error(f"read error: {path}", e); continue

        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        # 월별 탭 기록 (기존 성공 버전 유지)
        ws_nat = fuzzy_ws(sh, nat_title)
        ws_se  = fuzzy_ws(sh, se_title)

        # 집계
        counts, med, mean = agg_all_stats(df)

        if ws_nat:
            # 전국 탭: 헤더 기준으로 값 구성 + 총합계는 전국으로 채움
            header_nat = _retry(ws_nat.row_values, 1) or []
            values_nat = {}
            for h in header_nat:
                if not h or h=="날짜": continue
                if h=="총합계":
                    values_nat[h] = int(counts.get("전국",0))
                else:
                    values_nat[h] = int(counts.get(h,0))
            mode, row = upsert_row(ws_nat, today_kst, values_nat)
            log(f"[전국] {ws_nat.title} -> {kdate_str(today_kst)} {mode} row={row}")
            note_written(f"{ws_nat.title}\t{kdate_str(today_kst)}\t{mode}\t{row}")
        else:
            log(f"[전국] sheet not found: '{nat_title}' (skip)")

        if ws_se:
            header_se = _retry(ws_se.row_values, 1) or []
            values_se = {}
            for h in header_se:
                if not h or h=="날짜": continue
                if h=="총합계":
                    values_se[h] = int(counts.get("서울",0))
                else:
                    values_se[h] = int(counts.get(h,0))
            mode, row = upsert_row(ws_se, today_kst, values_se)
            log(f"[서울] {ws_se.title} -> {kdate_str(today_kst)} {mode} row={row}")
            note_written(f"{ws_se.title}\t{kdate_str(today_kst)}\t{mode}\t{row}")
        else:
            log(f"[서울] sheet not found: '{se_title}' (skip)")

        # 요약 캐시 저장
        month_cache[ym] = {
            "counts": {k:int(counts.get(k,0)) for k in SUMMARY_COLS},
            "med":    {k:med.get(k,"") for k in SUMMARY_COLS},
            "mean":   {k:mean.get(k,"") for k in SUMMARY_COLS},
            "y": y, "m": m
        }

        # 압구정동 원본 누적(후에 월별 비교/기록)
        ap = df[(df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")].copy()
        if not ap.empty:
            apgu_all.append(ap)

    # 거래요약 탭 처리(월 순서대로)
    ws_sum = fuzzy_ws(sh, SUMMARY_SHEET)
    if ws_sum and month_cache:
        for ym in sorted(month_cache.keys(), key=ym_key):
            cur = month_cache[ym]; y=cur["y"]; m=cur["m"]
            prv = month_cache.get(prev_ym(ym))
            prev_counts = prv["counts"] if prv else None
            write_month_summary(ws_sum, y, m, cur["counts"], cur["med"], cur["mean"], prev_counts)

    # 압구정동: 월 단위로 원본 붙이기 + (신규)/(삭제) 로그
    if apgu_all:
        ws_ap = ensure_apgu_sheet(sh)
        all_df = pd.concat(apgu_all, ignore_index=True)
        yms = sorted({(int(r["계약년"]), int(r["계약월"])) for _, r in all_df.dropna(subset=["계약년","계약월"]).iterrows()})
        for y, m in yms:
            upsert_apgu_month(ws_ap, all_df, y, m, today_kst)

    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_error(str(e), e)
        sys.exit(1)
