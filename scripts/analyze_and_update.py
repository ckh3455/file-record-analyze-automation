# scripts/analyze_and_update.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, re, sys, json, math, time, random
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime, date
from zoneinfo import ZoneInfo

import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

# ===================== Logging =====================
LOG_DIR = Path("analyze_report")
def _ensure_logdir():
    if LOG_DIR.exists() and not LOG_DIR.is_dir():
        try: LOG_DIR.unlink()
        except Exception: pass
    if not LOG_DIR.exists():
        LOG_DIR.mkdir(parents=True, exist_ok=True)
_ensure_logdir()

RUN_LOG = LOG_DIR / f"run-{datetime.now(ZoneInfo('Asia/Seoul')).strftime('%Y%m%dT%H%M%S%z')}.log"
LATEST  = LOG_DIR / "latest.log"
WRITTEN = LOG_DIR / "where_written.txt"

def _t(): return datetime.now(ZoneInfo("Asia/Seoul")).strftime("[%H:%M:%S]")

def _write(line: str):
    print(line)
    try:
        with RUN_LOG.open("a", encoding="utf-8") as f: f.write(line+"\n")
        with LATEST.open("a", encoding="utf-8") as f: f.write(line+"\n")
    except Exception: pass

def log(msg: str): _write(f"{_t()} {msg}")

def log_error(msg: str, exc: Optional[BaseException]=None):
    _write(f"{_t()} [ERROR] {msg}")
    if exc:
        import traceback
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        print(tb, file=sys.stderr)
        try:
            with RUN_LOG.open("a", encoding="utf-8") as f: f.write(tb+"\n")
            with LATEST.open("a", encoding="utf-8") as f: f.write(tb+"\n")
        except Exception: pass

def note_written(s: str):
    try:
        with WRITTEN.open("a", encoding="utf-8") as f: f.write(s.rstrip()+"\n")
    except Exception: pass

# ===================== Utils / Normalization =====================
def norm(s: str) -> str:
    return re.sub(r"\s+","", str(s or "")).strip()

def kst_today() -> date:
    return datetime.now(ZoneInfo("Asia/Seoul")).date()

def kdate_str(d: date) -> str:
    return f"{d.year}. {d.month}. {d.day}"

def ym_from_filename(fname: str) -> Tuple[str,str,str]:
    # '전국 2507_250928.xlsx' -> ('전국 2025년 7월','서울 2025년 7월','25/7')
    m = re.search(r"(\d{2})(\d{2})_", fname)
    if not m: raise ValueError(f"unexpected filename: {fname}")
    yy, mm = int(m.group(1)), int(m.group(2))
    nat = f"전국 {2000+yy}년 {mm}월"
    se  = f"서울 {2000+yy}년 {mm}월"
    ym  = f"{str(yy).zfill(2)}/{mm}"
    return nat, se, ym

def prev_ym(ym: str) -> str:
    yy, mm = ym.split("/")
    y = int(yy); m = int(mm)
    if m==1: return f"{str(y-1).zfill(2)}/12"
    return f"{yy}/{m-1}"

# ===================== Sheets I/O (throttle + retry) =====================
_LAST = 0.0
def _throttle(min_gap=0.45):
    global _LAST
    now = time.time()
    if now - _LAST < min_gap:
        time.sleep(min_gap - (now - _LAST))
    _LAST = time.time()

def _retry(fn, *a, **kw):
    back = 0.8
    for i in range(6):
        try:
            _throttle()
            return fn(*a, **kw)
        except APIError as e:
            s = str(e)
            if any(x in s for x in ("429","500","502","503")):
                time.sleep(back*(2**i)+random.uniform(0,0.3))
                continue
            raise

def a1_col(idx: int) -> str:
    s = ""
    n = idx
    while n>0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def open_sheet(sheet_id: str, sa_json: Optional[str], sa_path: Optional[str]):
    log("[gspread] auth")
    if sa_json:
        creds = Credentials.from_service_account_info(
            json.loads(sa_json),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
    else:
        creds = Credentials.from_service_account_file(
            sa_path or "sa.json",
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheet_id)

def find_ws_exact(sh, title: str):
    t = norm(title)
    for ws in sh.worksheets():
        if norm(ws.title) == t:
            log(f"[ws] matched: '{ws.title}'")
            return ws
    return None

def batch_values_update(ws, payload: List[Dict]):
    body = {"valueInputOption":"USER_ENTERED","data":payload}
    return _retry(ws.spreadsheet.values_batch_update, body=body)

def batch_format(ws, requests: List[dict]):
    if not requests: return
    _retry(ws.spreadsheet.batch_update, {"requests": requests})

# ===================== Data load & aggregate =====================
NEEDED = ["광역","구","법정동","계약년","계약월","계약일","거래금액(만원)"]

def read_xlsx(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data", dtype=str, engine="openpyxl").fillna("")
    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(r"[^\d.]","",regex=True), errors="coerce")
    for c in NEEDED:
        if c not in df.columns: df[c] = pd.NA
    return df[NEEDED].copy()

PROV_MAP = {
    "서울특별시":"서울","세종특별자치시":"세종시","강원특별자치도":"강원도",
    "경기도":"경기도","인천광역시":"인천광역시","부산광역시":"부산","대구광역시":"대구",
    "광주광역시":"광주","대전광역시":"대전","울산광역시":"울산","전라남도":"전남",
    "전북특별자치도":"전북","경상남도":"경남","경상북도":"경북","충청남도":"충남",
    "충청북도":"충북","제주특별자치도":"제주",
}

SUMMARY_COLS = [
    "전국","서울","강남구","압구정동","경기도","인천광역시","세종시","서초구","송파구","용산구",
    "강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구","강서구",
    "강북구","관악구","구로구","금천구","도봉구","동대문구","서대문구","성북구","은평구",
    "중구","중랑구","부산","대구","광주","대전","강원도","경남","경북","전남","전북","충남","충북","제주"
]

def eok_series(ser: pd.Series) -> pd.Series:
    s = pd.to_numeric(ser, errors="coerce").dropna()
    if s.empty: return pd.Series([], dtype=float)
    return s/10000.0

def round2(v) -> str:
    try:
        return f"{float(v):.2f}"
    except Exception:
        return ""

def agg_all(df: pd.DataFrame):
    counts = {c:0 for c in SUMMARY_COLS}
    med    = {c:"" for c in SUMMARY_COLS}
    mean   = {c:"" for c in SUMMARY_COLS}

    # 전국
    counts["전국"] = int(len(df))
    s = eok_series(df["거래금액(만원)"])
    if not s.empty:
        med["전국"]  = round2(s.median())
        mean["전국"] = round2(s.mean())

    # 광역 → 매핑
    for prov, sub in df.groupby("광역"):
        key = PROV_MAP.get(str(prov), str(prov))
        if key in counts:
            counts[key] += int(len(sub))
            ss = eok_series(sub["거래금액(만원)"])
            if not ss.empty:
                med[key]  = round2(ss.median())
                mean[key] = round2(ss.mean())

    # 서울/구
    seoul = df[df["광역"]=="서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul)>0:
        ss = eok_series(seoul["거래금액(만원)"])
        if not ss.empty:
            med["서울"]  = round2(ss.median())
            mean["서울"] = round2(ss.mean())
    if "구" in seoul.columns:
        for gu, sub in seoul.groupby("구"):
            gu = str(gu)
            if gu in counts:
                counts[gu] += int(len(sub))
                ss = eok_series(sub["거래금액(만원)"])
                if not ss.empty:
                    med[gu]  = round2(ss.median())
                    mean[gu] = round2(ss.mean())

    # 압구정동
    ap = seoul[seoul.get("법정동","")== "압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap)>0:
        ss = eok_series(ap["거래금액(만원)"])
        med["압구정동"]  = round2(ss.median())
        mean["압구정동"] = round2(ss.mean())

    return counts, med, mean

# ===================== Daily tabs write (전국/서울) =====================
def find_date_col(ws) -> int:
    header = _retry(ws.row_values, 1) or []
    for i, v in enumerate(header, start=1):
        if str(v).strip() == "날짜":
            return i
    return 1

def parse_cell_date(s: str) -> Optional[date]:
    s = str(s or "").strip()
    for fmt in ("%Y-%m-%d","%Y.%m.%d","%Y. %m. %d"):
        try: return datetime.strptime(s, fmt).date()
        except Exception: pass
    m = re.match(r"^\s*(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\s*$", s)
    if m: return date(*map(int, m.groups()))
    return None

def find_or_append_row(ws, target: date) -> int:
    dcol = find_date_col(ws)
    vals = _retry(ws.col_values, dcol) or []
    # 찾기
    for r, v in enumerate(vals[1:], start=2):
        if parse_cell_date(v) == target:
            return r
    # 맨 아래
    used = 1
    for i in range(len(vals), 0, -1):
        if str(vals[i-1]).strip():
            used = i; break
    return used + 1

def build_daily_row(header: List[str], day: date, series: Dict[str,int]) -> List:
    out, total = [], 0
    for i, h in enumerate(header):
        if i==0:
            out.append(kdate_str(day)); continue
        if not h: out.append(""); continue
        name = str(h).strip()
        if name in ("총합계","합계","전체"):
            out.append(total); continue
        v = int(series.get(name, 0))
        out.append(v)
        if name != "전국": total += v
    return out

def write_daily(ws, day: date, series: Dict[str,int]):
    # 보호: 요약 탭에는 절대 쓰지 않음
    if norm(ws.title) == norm("거래요약"):
        return
    header = _retry(ws.row_values, 1) or []
    if not header:
        raise RuntimeError(f"empty header in {ws.title}")
    row = find_or_append_row(ws, day)
    payload = [{"range": f"A{row}:{a1_col(len(header))}{row}",
                "values":[build_daily_row(header, day, series)]}]
    batch_values_update(ws, payload)
    log(f"[ws] {ws.title} -> {kdate_str(day)} row={row}")
    note_written(f"{ws.title}\t{kdate_str(day)}\tOK\t{row}")

# ===================== Summary(거래요약) =====================
def ensure_summary_header(ws):
    vals = _retry(ws.get_all_values) or []
    if not vals:
        _retry(ws.update, [["년월","구분"]+SUMMARY_COLS], "A1")
        log("[summary] header created")

def find_summary_row(ws, ym: str, label: str) -> int:
    vals = _retry(ws.get_all_values) or []
    if not vals: return 2
    for i, r in enumerate(vals[1:], start=2):
        a = (r[0] if len(r)>0 else "").strip()
        b = (r[1] if len(r)>1 else "").strip()
        if a==ym and b==label:
            return i
    return len(vals)+1

def put_summary_line(ws, row: int, ym: str, label: str, line_map: Dict[str,object]):
    header = _retry(ws.row_values, 1) or []
    hmap = {h:i+1 for i,h in enumerate(header)}
    payload = [{"range": f"A{row}", "values": [[ym]]},
               {"range": f"B{row}", "values": [[label]]}]
    for c in SUMMARY_COLS:
        if c in hmap:
            payload.append({"range": f"{a1_col(hmap[c])}{row}",
                            "values": [[line_map.get(c,"")]]})
    batch_values_update(ws, payload)

def color_summary_diff(ws, row: int, diff_map: Dict[str,str]):
    header = _retry(ws.row_values, 1) or []
    hmap = {h:i+1 for i,h in enumerate(header)}
    reqs=[]
    for k,v in diff_map.items():
        if k not in hmap: continue
        if v.startswith("+"):
            col = (0.0,0.35,1.0)
        elif v.startswith("-"):
            col = (1.0,0.0,0.0)
        else:
            continue
        cidx = hmap[k]
        reqs.append({
            "repeatCell":{
                "range":{
                    "sheetId": ws.id,
                    "startRowIndex": row-1, "endRowIndex": row,
                    "startColumnIndex": cidx-1, "endColumnIndex": cidx
                },
                "cell":{"userEnteredFormat":{"textFormat":{"foregroundColor":{
                    "red":col[0],"green":col[1],"blue":col[2]
                }}}},
                "fields":"userEnteredFormat.textFormat.foregroundColor"
            }
        })
    batch_format(ws, reqs)

def write_summary(ws, ym: str, counts: Dict[str,int], med: Dict[str,str], mean: Dict[str,str], prev_counts: Optional[Dict[str,int]]):
    ensure_summary_header(ws)

    r1 = find_summary_row(ws, ym, "거래건수")
    put_summary_line(ws, r1, ym, "거래건수", counts)
    log(f"[summary] {ym} 거래건수 -> row={r1}")

    r2 = find_summary_row(ws, ym, "중앙값(단위:억)")
    put_summary_line(ws, r2, ym, "중앙값(단위:억)", med)
    log(f"[summary] {ym} 중앙값 -> row={r2}")

    r3 = find_summary_row(ws, ym, "평균가(단위:억)")
    put_summary_line(ws, r3, ym, "평균가(단위:억)", mean)
    log(f"[summary] {ym} 평균가 -> row={r3}")

    diffs = {c:"0" for c in SUMMARY_COLS}
    if prev_counts:
        for c in SUMMARY_COLS:
            cur = int(counts.get(c,0) or 0)
            prv = int(prev_counts.get(c,0) or 0)
            d = cur - prv
            diffs[c] = f"+{d}" if d>0 else (str(d) if d<0 else "0")

    r4 = find_summary_row(ws, ym, "전월대비 건수증감")
    put_summary_line(ws, r4, ym, "전월대비 건수증감", diffs)
    color_summary_diff(ws, r4, diffs)
    log(f"[summary] {ym} 전월대비 -> row={r4}")

    r5 = find_summary_row(ws, ym, "예상건수")
    blanks = {c:"" for c in SUMMARY_COLS}
    put_summary_line(ws, r5, ym, "예상건수", blanks)
    log(f"[summary] {ym} 예상건수 -> row={r5}")

# ===================== 압구정동 (원본 달별 + 변동로그) =====================
APGU_SHEET = "압구정동"
CHANGE_HEADER = ["변경구분","기록일","계약년","계약월","계약일","단지명","전용면적(㎡)","동","층","거래금액(만원)"]

def ensure_apgu_sheet(sh) -> gspread.Worksheet:
    ws = find_ws_exact(sh, APGU_SHEET)
    if ws: return ws
    return _retry(sh.add_worksheet, title=APGU_SHEET, rows=4000, cols=80)

def row_key(d: dict) -> str:
    parts = [d.get("계약년",""), d.get("계약월",""), d.get("계약일",""),
             d.get("단지명",""), d.get("전용면적(㎡)",""), d.get("동",""),
             d.get("층",""), d.get("거래금액(만원)","")]
    return "|".join(str(x).strip() for x in parts)

def upsert_apgu_month(ws: gspread.Worksheet, df: pd.DataFrame, ym: str):
    # 해당 월만
    yy, mm = ym.split("/")
    yy_full = 2000 + int(yy)
    dff = df[(df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")].copy()
    dff = dff[(dff["계약년"]==yy_full) & (dff["계약월"]==int(mm))]

    if dff.empty:
        log(f"[압구정동] {ym} no rows")
        return

    # 정렬(오래된 → 최신)
    for c in ["계약년","계약월","계약일"]:
        if c not in dff.columns: dff[c]=pd.NA
    dff = dff.sort_values(["계약년","계약월","계약일"], ascending=[True,True,True], kind="mergesort")

    # 시트 헤더 확보(없으면 원본 헤더로)
    vals = _retry(ws.get_all_values) or []
    if not vals:
        header = list(dff.columns) + ["기록일"]
        _retry(ws.update, [header], "A1")
        vals = [header]
    else:
        header = vals[0]
        if "기록일" not in header:
            header = header + ["기록일"]
            _retry(ws.update, [header], "A1")
            vals = _retry(ws.get_all_values) or [header]
        # 파일에 있고 시트에 없는 컬럼이 있으면 확장
        union = list(dict.fromkeys(header + [c for c in dff.columns if c not in header]))
        if union != header:
            header = union
            _retry(ws.update, [header], "A1")
            vals = _retry(ws.get_all_values) or [header]

    existing = vals[1:]
    idx = {h:i for i,h in enumerate(header)}
    def row_to_dict(row):
        return {k:(row[i] if i<len(row) else "") for k,i in idx.items()}

    # 기존 키 셋(해당 월만 필터)
    def ym_of(dct):
        try:
            y = int(float(dct.get("계약년","0"))); m = int(float(dct.get("계약월","0")))
            return f"{str(y%100).zfill(2)}/{m}"
        except Exception: return ""
    ex_rows = [r for r in existing if ym_of(row_to_dict(r))==ym]
    ex_keys = set(row_key(row_to_dict(r)) for r in ex_rows)

    # 신규 rows
    today = kdate_str(kst_today())
    new_recs = []
    for _, r in dff.iterrows():
        d = {k:r.get(k,"") for k in header if k in dff.columns}
        k = row_key(d)
        if k in ex_keys: continue
        rec = [r.get(col,"") if col!="기록일" else today for col in header]
        new_recs.append(rec)
        ex_keys.add(k)

    # 삭제 rows: 파일엔 없고 시트엔 있는 것(해당 월 범위만)
    file_keys = set()
    for _, r in dff.iterrows():
        d = {k:r.get(k,"") for k in header if k in dff.columns}
        file_keys.add(row_key(d))
    removed = [row_to_dict(r) for r in ex_rows if row_key(row_to_dict(r)) not in file_keys]

    # append 신규
    if new_recs:
        start = len(vals)+1
        need_rows = start + len(new_recs) + 1
        if need_rows > ws.row_count:
            _retry(ws.add_rows, need_rows - ws.row_count + 50)
        rng = f"A{start}:{a1_col(len(header))}{start+len(new_recs)-1}"
        batch_values_update(ws, [{"range":rng, "values":new_recs}])
    log(f"[압구정동] {ym} new={len(new_recs)} removed={len(removed)}")

    # 변동 로그(맨 하단, 빨간색)
    if new_recs or removed:
        all_vals = _retry(ws.get_all_values) or []
        start = len(all_vals)+1
        rows = []
        def take(dct, col): return dct.get(col,"")
        for rec in new_recs:
            dd = {h: (rec[idx[h]] if h in idx and idx[h]<len(rec) else "") for h in header}
            rows.append(["(신규)", today, take(dd,"계약년"), take(dd,"계약월"), take(dd,"계약일"),
                         take(dd,"단지명"), take(dd,"전용면적(㎡)"), take(dd,"동"), take(dd,"층"), take(dd,"거래금액(만원)")])
        for dd in removed:
            rows.append(["(삭제)", today, take(dd,"계약년"), take(dd,"계약월"), take(dd,"계약일"),
                         take(dd,"단지명"), take(dd,"전용면적(㎡)"), take(dd,"동"), take(dd,"층"), take(dd,"거래금액(만원)")])
        need = start + len(rows)
        if need > ws.row_count: _retry(ws.add_rows, need - ws.row_count + 20)
        _retry(ws.update, [CHANGE_HEADER], f"A{start}:J{start}")
        _retry(ws.update, rows, f"A{start+1}:J{start+len(rows)}")
        req = {
            "repeatCell":{
                "range":{"sheetId":ws.id,"startRowIndex":start-1,"endRowIndex":start+len(rows),
                         "startColumnIndex":0,"endColumnIndex":10},
                "cell":{"userEnteredFormat":{"textFormat":{"foregroundColor":{"red":1.0,"green":0.0,"blue":0.0}}}},
                "fields":"userEnteredFormat.textFormat.foregroundColor"
            }
        }
        batch_format(ws, [req])

# ===================== Main =====================
def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts-dir", required=True)
    ap.add_argument("--sheet-id", required=True)
    ap.add_argument("--sa-path", default="sa.json")
    args = ap.parse_args()

    # latest.log reset
    try:
        if LATEST.exists(): LATEST.unlink()
    except Exception: pass

    log("[MAIN]")
    work = Path(args.artifacts_dir)
    files = sorted([p for p in work.rglob("전국 *.xlsx") if p.is_file()])
    log(f"[collect] found {len(files)} xlsx files")

    sa_json = os.environ.get("SA_JSON","").strip()
    sh = open_sheet(args.sheet_id, sa_json, args.sa_path)
    log("[gspread] spreadsheet opened")

    today = kst_today()

    # 월별 캐시(요약용)
    month_cache: Dict[str, Dict] = {}

    for path in files:
        nat_title, se_title, ym = ym_from_filename(path.name)
        log(f"[file] {path.name} -> nat='{nat_title}' / seoul='{se_title}' / ym={ym}")

        df = read_xlsx(path)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        counts, med, mean = agg_all(df)

        # 일일 탭 쓰기
        ws_nat = find_ws_exact(sh, nat_title)
        ws_se  = find_ws_exact(sh, se_title)
        if ws_nat:
            # 전국 탭: 총합계는 '전국' 값으로 이미 반영. 헤더 기반으로 채움
            header = _retry(ws_nat.row_values, 1) or []
            series = {}
            for h in header:
                if not h or h=="날짜": continue
                if h=="총합계":
                    series[h] = int(counts.get("전국",0))
                else:
                    series[h] = int(counts.get(h,0))
            write_daily(ws_nat, today, series)
        else:
            log(f"[전국] sheet not found: '{nat_title}' (skip)")

        if ws_se:
            header = _retry(ws_se.row_values, 1) or []
            series = {}
            for h in header:
                if not h or h=="날짜": continue
                if h=="총합계":
                    series[h] = int(counts.get("서울",0))
                else:
                    series[h] = int(counts.get(h,0))
            write_daily(ws_se, today, series)
        else:
            log(f"[서울] sheet not found: '{se_title}' (skip)")

        # 요약 캐시 저장
        month_cache[ym] = {
            "counts": {c:int(counts.get(c,0)) for c in SUMMARY_COLS},
            "med":    {c:med.get(c,"") for c in SUMMARY_COLS},
            "mean":   {c:mean.get(c,"") for c in SUMMARY_COLS},
            "df": df,  # 압구정동 월별 추출용
        }

    # 요약 탭 업데이트(정렬 순서대로)
    ws_sum = find_ws_exact(sh, "거래요약")
    if ws_sum and month_cache:
        def key_ym(ym: str):
            yy, mm = ym.split("/")
            return (int(yy), int(mm))
        for ym in sorted(month_cache.keys(), key=key_ym):
            cur = month_cache[ym]
            prv = month_cache.get(prev_ym(ym))
            write_summary(ws_sum, ym, cur["counts"], cur["med"], cur["mean"], prv["counts"] if prv else None)

    # 압구정동: 월별 원본 붙이기 + 변동로그
    ws_ap = ensure_apgu_sheet(sh)
    if ws_ap:
        for ym, bundle in month_cache.items():
            upsert_apgu_month(ws_ap, bundle["df"], ym)

    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_error(str(e), e)
        sys.exit(1)
