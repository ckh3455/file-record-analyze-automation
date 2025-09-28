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


# ===================== Logging =====================
LOG_DIR = Path("analyze_report")
def _ensure_logdir():
    if LOG_DIR.exists() and not LOG_DIR.is_dir():
        try: LOG_DIR.unlink()
        except Exception: pass
    LOG_DIR.mkdir(parents=True, exist_ok=True)
_ensure_logdir()

RUN_LOG = LOG_DIR / f"run-{datetime.now(ZoneInfo('Asia/Seoul')).strftime('%Y%m%dT%H%M%S%z')}.log"
LATEST  = LOG_DIR / "latest.log"
WRITTEN = LOG_DIR / "where_written.txt"

def _t(): return datetime.now(ZoneInfo("Asia/Seoul")).strftime("[%H:%M:%S]")
def _w(s: str):
    print(s)
    try:
        with RUN_LOG.open("a", encoding="utf-8") as f: f.write(s+"\n")
        with LATEST.open("a", encoding="utf-8") as f: f.write(s+"\n")
    except Exception: pass
def log(msg: str): _w(f"{_t()} {msg}")
def log_error(msg: str, e: Optional[BaseException]=None):
    _w(f"{_t()} [ERROR] {msg}")
    if e:
        import traceback
        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        print(tb, file=sys.stderr)
        try:
            with RUN_LOG.open("a", encoding="utf-8") as f: f.write(tb+"\n")
            with LATEST.open("a", encoding="utf-8") as f: f.write(tb+"\n")
        except Exception: pass
def note_written(s: str):
    try:
        with WRITTEN.open("a", encoding="utf-8") as f: f.write(s.rstrip()+"\n")
    except Exception: pass


# ===================== Utils =====================
def kdate_str(d: date) -> str: return f"{d.year}. {d.month}. {d.day}"
def fmt2(v) -> str:
    try: return f"{float(v):.2f}"
    except Exception: return ""

def a1_col(n: int) -> str:
    s = ""
    while n>0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def norm_raw(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).strip()

def ym_from_filename(fname: str) -> Tuple[int,int,int]:
    # '전국 2509_250928.xlsx' → (2025, 9, 28)
    m = re.search(r"(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})", fname)
    if not m: raise ValueError(f"unexpected filename: {fname}")
    return 2000+int(m.group(1)), int(m.group(2)), int(m.group(5))

def ym_label(y: int, m: int) -> str: return f"{str(y%100).zfill(2)}/{m}"
def prev_ym_label(y: int, m: int) -> str: return ym_label(y if m>1 else y-1, m-1 if m>1 else 12)


# ===================== Sheets: throttle/retry =====================
_LAST = 0.0
def _throttle(sec=0.40):
    global _LAST
    now = time.time()
    if now - _LAST < sec:
        time.sleep(sec - (now - _LAST))
    _LAST = time.time()

def _retry(fn, *a, **kw):
    base = 0.8
    for i in range(6):
        try:
            _throttle()
            return fn(*a, **kw)
        except APIError as e:
            s = str(e)
            if any(x in s for x in ("429","500","502","503")):
                time.sleep(base*(2**i) + random.uniform(0,0.3))
                continue
            raise

def values_batch_update(ws: gspread.Worksheet, payload: List[Dict]):
    if not payload: return
    body = {"valueInputOption":"USER_ENTERED","data":payload}
    return _retry(ws.spreadsheet.values_batch_update, body=body)

def batch_format(ws: gspread.Worksheet, requests: List[dict]):
    if requests: _retry(ws.spreadsheet.batch_update, {"requests": requests})


# ===================== Columns/Mapping =====================
NEEDED_COLS = ["광역","구","법정동","계약년","계약월","계약일","거래금액(만원)"]

PROV_MAP = {
    "서울특별시":"서울","세종특별자치시":"세종시","강원특별자치도":"강원도","경기도":"경기도",
    "인천광역시":"인천광역시","부산광역시":"부산","대구광역시":"대구","광주광역시":"광주",
    "대전광역시":"대전","울산광역시":"울산","전라남도":"전남","전북특별자치도":"전북",
    "경상남도":"경남","경상북도":"경북","충청남도":"충남","충청북도":"충북","제주특별자치도":"제주",
}

SUMMARY_COLS = [
    "전국","서울","강남구","압구정동","경기도","인천광역시","세종시","서초구","송파구","용산구",
    "강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구","강서구",
    "강북구","관악구","구로구","금천구","도봉구","동대문구","서대문구","성북구","은평구",
    "중구","중랑구","부산","대구","광주","대전","강원도","경남","경북","전남","전북","충남","충북","제주"
]


# ===================== Read/aggregate =====================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data", dtype=str, engine="openpyxl").fillna("")
    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in NEEDED_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    return df

def eok_series(ser) -> pd.Series:
    s = pd.to_numeric(ser, errors="coerce").dropna()
    return s/10000.0 if not s.empty else pd.Series([], dtype=float)

def agg_all_stats(df: pd.DataFrame):
    counts = {c:0 for c in SUMMARY_COLS}
    med   = {c:"" for c in SUMMARY_COLS}
    mean  = {c:"" for c in SUMMARY_COLS}

    s_all = eok_series(df["거래금액(만원)"])
    counts["전국"] = int(len(df))
    if not s_all.empty:
        med["전국"]  = fmt2(s_all.median())
        mean["전국"] = fmt2(s_all.mean())

    for prov, sub in df.groupby("광역"):
        key = PROV_MAP.get(str(prov), str(prov))
        if key in counts:
            counts[key] += int(len(sub))
            s = eok_series(sub["거래금액(만원)"])
            if not s.empty:
                med[key]  = fmt2(s.median())
                mean[key] = fmt2(s.mean())

    seoul = df[df.get("광역","")=="서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul)>0:
        s = eok_series(seoul["거래금액(만원)"])
        if not s.empty:
            med["서울"]  = fmt2(s.median())
            mean["서울"] = fmt2(s.mean())
        if "구" in seoul.columns:
            for gu, sub in seoul.groupby("구"):
                gu = str(gu)
                if gu in counts:
                    counts[gu] += int(len(sub))
                    s = eok_series(sub["거래금액(만원)"])
                    if not s.empty:
                        med[gu]  = fmt2(s.median())
                        mean[gu] = fmt2(s.mean())

    ap = seoul[seoul.get("법정동","")=="압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap)>0:
        s = eok_series(ap["거래금액(만원)"])
        med["압구정동"]  = fmt2(s.median())
        mean["압구정동"] = fmt2(s.mean())

    return counts, med, mean


# ===================== Tab matching (2자리/4자리 연도) =====================
def tab_key(title: str) -> Optional[str]:
    s = title.replace(" ", "")
    m = re.match(r"^(전국|서울)(\d{2,4})년(\d{1,2})월$", s)
    if not m: return None
    city = m.group(1); y = int(m.group(2))%100; mth = int(m.group(3))
    return f"{city}-{y:02d}-{mth}"

def find_month_sheets(sh: gspread.Spreadsheet, y: int, m: int) -> Tuple[Optional[gspread.Worksheet], Optional[gspread.Worksheet]]:
    want_nat = f"전국-{y%100:02d}-{m}"
    want_se  = f"서울-{y%100:02d}-{m}"
    nat = se = None
    for ws in sh.worksheets():
        k = tab_key(ws.title or "")
        if k == want_nat: nat = ws
        if k == want_se:  se  = ws
    return nat, se


# ===================== 월 탭 쓰기(전국/서울) =====================
def find_or_append_date_row(ws, target: date) -> int:
    vals = _retry(ws.get_all_values) or []
    if not vals: return 2
    for i, r in enumerate(vals[1:], start=2):
        s = (r[0] if r else "").strip()
        m = re.match(r"^\s*(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\s*$", s)
        if m and date(*map(int, m.groups())) == target:
            return i
    return len(vals)+1

def write_month_sheet(ws, when: date, series: Dict[str,int], sum_to: str="총합계"):
    title_ok = ws.title.strip().startswith(("전국","서울"))
    header = _retry(ws.row_values, 1) or []
    if not (title_ok and header and header[0].strip()=="날짜"):
        log(f"[skip] not a month sheet: {ws.title}")
        return

    hmap = {h: i+1 for i,h in enumerate(header)}
    row  = find_or_append_date_row(ws, when)

    payload = [{"range": f"A{row}", "values": [[kdate_str(when)]]}]
    total = 0
    for h in header[1:]:
        if h==sum_to: continue
        v = int(series.get(h, 0) or 0)
        payload.append({"range": f"{a1_col(hmap[h])}{row}", "values": [[v]]})
        if h!="전국": total += v
    if sum_to in hmap:
        payload.append({"range": f"{a1_col(hmap[sum_to])}{row}", "values": [[int(series.get('전국', total))]]})
    values_batch_update(ws, payload)
    log(f"[ws] {ws.title} -> {kdate_str(when)} row={row}")


# ===================== 거래요약 =====================
def header_row(ws) -> List[str]:
    hdr = _retry(ws.row_values, 1)
    if hdr: return hdr
    vals = _retry(ws.get_all_values) or []
    return vals[0] if vals else []

def ensure_summary_header(ws):
    hdr = header_row(ws)
    if not hdr:
        _retry(ws.update, [["년월","구분"] + SUMMARY_COLS], "A1")

def find_summary_row(ws, ym: str, label: str) -> int:
    vals = _retry(ws.get_all_values) or []
    if not vals: return 2
    for i, r in enumerate(vals[1:], start=2):
        a = (r[0] if len(r)>0 else "").strip()
        b = (r[1] if len(r)>1 else "").strip()
        if a==ym and b==label:
            return i
    return len(vals)+1

def put_summary_line(ws, row_idx: int, ym: str, label: str, line_map: dict):
    hdr = header_row(ws)
    if not hdr:
        ensure_summary_header(ws)
        hdr = header_row(ws)
        if not hdr:
            log("[skip] summary header missing; abort write")
            return
    hmap = {h: i+1 for i,h in enumerate(hdr)}
    payload = [
        {"range": f"A{row_idx}", "values": [[ym]]},
        {"range": f"B{row_idx}", "values": [[label]]},
    ]
    for col in SUMMARY_COLS:
        if col in hmap:
            payload.append({"range": f"{a1_col(hmap[col])}{row_idx}", "values": [[line_map.get(col,"")]]})
    values_batch_update(ws, payload)

def color_diff_cells(ws, row_idx: int, diff_line: dict):
    hdr = header_row(ws)
    hmap = {h: i+1 for i,h in enumerate(hdr)}
    reqs = []
    for k,v in diff_line.items():
        if k not in hmap: continue
        if v.startswith("+"): col = {"red":0.0,"green":0.35,"blue":1.0}
        elif v.startswith("-"): col = {"red":1.0,"green":0.0,"blue":0.0}
        else: continue
        reqs.append({
            "repeatCell":{
                "range":{"sheetId":ws.id,"startRowIndex":row_idx-1,"endRowIndex":row_idx,
                         "startColumnIndex":hmap[k]-1,"endColumnIndex":hmap[k]},
                "cell":{"userEnteredFormat":{"textFormat":{"foregroundColor":col}}},
                "fields":"userEnteredFormat.textFormat.foregroundColor"
            }
        })
    batch_format(ws, reqs)

def write_month_summary(ws, y: int, m: int, counts: dict, med: dict, mean: dict, prev_counts: Optional[dict]):
    ensure_summary_header(ws)
    ym = ym_label(y, m)

    r1 = find_summary_row(ws, ym, "거래건수")
    put_summary_line(ws, r1, ym, "거래건수", counts)
    hdr = header_row(ws)
    if hdr:
        batch_format(ws, [{
            "repeatCell":{
                "range":{"sheetId":ws.id,"startRowIndex":r1-1,"endRowIndex":r1,"startColumnIndex":2,"endColumnIndex":len(hdr)},
                "cell":{"userEnteredFormat":{"textFormat":{"bold":True}}},
                "fields":"userEnteredFormat.textFormat.bold"
            }
        }])
    log(f"[summary] {ym} 거래건수 -> row={r1}")

    r2 = find_summary_row(ws, ym, "중앙값(단위:억)")
    put_summary_line(ws, r2, ym, "중앙값(단위:억)", med)
    log(f"[summary] {ym} 중앙값 -> row={r2}")

    r3 = find_summary_row(ws, ym, "평균가(단위:억)")
    put_summary_line(ws, r3, ym, "평균가(단위:억)", mean)
    log(f"[summary] {ym} 평균가 -> row={r3}")

    r4 = find_summary_row(ws, ym, "전월대비 건수증감")
    diff = {}
    if prev_counts:
        for k in SUMMARY_COLS:
            cur = int(counts.get(k,0) or 0); prv = int(prev_counts.get(k,0) or 0)
            d = cur - prv
            diff[k] = f"+{d}" if d>0 else (str(d) if d<0 else "0")
    else:
        diff = {k:"" for k in SUMMARY_COLS}
    put_summary_line(ws, r4, ym, "전월대비 건수증감", diff)
    color_diff_cells(ws, r4, diff)
    log(f"[summary] {ym} 전월대비 -> row={r4}")

    r5 = find_summary_row(ws, ym, "예상건수")
    put_summary_line(ws, r5, ym, "예상건수", {k:"" for k in SUMMARY_COLS})
    log(f"[summary] {ym} 예상건수 -> row={r5}")


# ===================== 압구정동 (원본 + 증감 로그) =====================
def ensure_apgu_sheet(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    for ws in sh.worksheets():
        if ws.title.strip()=="압구정동": return ws
    return _retry(sh.add_worksheet, title="압구정동", rows=4000, cols=120)

def number_or_blank(v):
    if v is None: return ""
    if isinstance(v,float) and math.isnan(v): return ""
    return v

def upsert_apgu_month(ws: gspread.Worksheet, df_full: pd.DataFrame, y: int, m: int):
    cond = (df_full.get("광역","")=="서울특별시") & (df_full.get("법정동","")=="압구정동")
    dd = df_full[cond].copy()
    if dd.empty:
        log(f"[압구정동] {y%100}/{m} no rows"); return
    for c in ["계약년","계약월","계약일"]:
        if c not in dd.columns: dd[c] = pd.NA
    dd = dd.sort_values(["계약년","계약월","계약일"], ascending=[True,True,True], kind="mergesort")
    dd = dd[(dd["계약년"]==y) & (dd["계약월"]==m)]

    vals = _retry(ws.get_all_values) or []
    if not vals:
        header = list(dd.columns) + ["기록일","변경구분","변동일"]
        _retry(ws.update, [header], "A1"); vals=[header]
    else:
        header = vals[0]
        union = list(dict.fromkeys(header + [c for c in dd.columns if c not in header] + ["기록일","변경구분","변동일"]))
        if union != header:
            _retry(ws.update, [union], "A1")
            header = union
            vals = _retry(ws.get_all_values) or [header]
    idx = {h:i for i,h in enumerate(header)}
    def row_to_dict(row): return {k:(row[i] if i<len(row) else "") for k,i in idx.items()}
    def make_key(d: dict) -> str:
        parts = [d.get("계약년",""), d.get("계약월",""), d.get("계약일",""),
                 d.get("단지명",""), d.get("전용면적(㎡)",""), d.get("동",""), d.get("층",""), d.get("거래금액(만원)","")]
        return "|".join(str(x).strip() for x in parts)

    existing_rows = vals[1:]
    exist_month_keys = set()
    for r in existing_rows:
        d = row_to_dict(r)
        try:
            if int(float(d.get("계약년","0")))==y and int(float(d.get("계약월","0")))==m:
                exist_month_keys.add(make_key(d))
        except Exception: pass

    file_month_keys = set()
    for _, r in dd.iterrows():
        d = {k: r.get(k,"") for k in header if k in dd.columns}
        file_month_keys.add(make_key(d))

    today = kdate_str(datetime.now(ZoneInfo("Asia/Seoul")).date())

    to_append = []
    for _, r in dd.iterrows():
        d = {k:r.get(k,"") for k in header if k in dd.columns}
        k = make_key(d)
        if k in exist_month_keys: continue
        rec = [number_or_blank(d.get(h,"")) for h in header]
        if "기록일" in idx: rec[idx["기록일"]] = today
        to_append.append(rec)

    if to_append:
        start = len(vals)+1; end = start+len(to_append)-1
        if end > ws.row_count: _retry(ws.add_rows, end-ws.row_count)
        rng = f"A{start}:{a1_col(len(header))}{end}"
        _retry(ws.update, to_append, rng)
        vals = _retry(ws.get_all_values) or [header]
        existing_rows = vals[1:]

    added_logs, removed_logs = [], []
    for _, r in dd.iterrows():
        d = {k:r.get(k,"") for k in header if k in dd.columns}
        k = make_key(d)
        if k not in exist_month_keys:
            cp = [d.get(h,"") for h in header]
            if "변경구분" in idx: cp[idx["변경구분"]] = "(신규)"
            if "변동일"  in idx: cp[idx["변동일"]]  = today
            added_logs.append(cp)

    for r in existing_rows:
        d = row_to_dict(r)
        try:
            if int(float(d.get("계약년","0")))==y and int(float(d.get("계약월","0")))==m:
                if make_key(d) not in file_month_keys:
                    cp = [d.get(h,"") for h in header]
                    if "변경구분" in idx: cp[idx["변경구분"]] = "(삭제)"
                    if "변동일"  in idx: cp[idx["변동일"]]  = today
                    removed_logs.append(cp)
        except Exception: pass

    changes = added_logs + removed_logs
    if changes:
        cur_vals = _retry(ws.get_all_values) or []
        start = len(cur_vals) + 1
        end = start + len(changes) - 1
        if end > ws.row_count: _retry(ws.add_rows, end-ws.row_count)
        rng = f"A{start}:{a1_col(len(header))}{end}"
        _retry(ws.update, changes, rng)
        batch_format(ws, [{
            "repeatCell":{
                "range":{"sheetId":ws.id,"startRowIndex":start-1,"endRowIndex":end,"startColumnIndex":0,"endColumnIndex":len(header)},
                "cell":{"userEnteredFormat":{"textFormat":{"foregroundColor":{"red":1.0,"green":0.0,"blue":0.0}}}},
                "fields":"userEnteredFormat.textFormat.foregroundColor"
            }
        }])

    log(f"[압구정동] {y%100}/{m} new={len(added_logs)} removed={len(removed_logs)} appended_main={len(to_append)}")


# ===================== Main =====================
def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts-dir", default="artifacts")
    ap.add_argument("--sheet-id", default=os.environ.get("SHEET_ID",""))
    ap.add_argument("--sa", default="sa.json")
    args = ap.parse_args()

    try:
        if LATEST.exists(): LATEST.unlink()
        if WRITTEN.exists(): WRITTEN.unlink()
    except Exception: pass

    log("[MAIN]")
    art = Path(args.artifacts_dir)
    files = sorted([p for p in art.rglob("전국 *.xlsx") if p.is_file()])
    log(f"[collect] found {len(files)} xlsx files")

    sa_raw = os.environ.get("SA_JSON","").strip()
    if sa_raw:
        creds = Credentials.from_service_account_info(json.loads(sa_raw),
                  scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])
    else:
        creds = Credentials.from_service_account_file(args.sa,
                  scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, args.sheet_id)
    log("[gspread] spreadsheet opened")

    today = datetime.now(ZoneInfo("Asia/Seoul")).date()
    log(f"[date] {kdate_str(today)}")

    ws_summary = next((ws for ws in sh.worksheets() if ws.title.strip()=="거래요약"), None)

    month_cache: Dict[Tuple[int,int], Dict] = {}
    apgu_all: List[pd.DataFrame] = []

    for p in files:
        try:
            y, m, _ = ym_from_filename(p.name)
        except Exception as e:
            log_error(f"filename parse failed: {p.name}", e); continue

        nat_ws, se_ws = find_month_sheets(sh, y, m)
        log(f"[file] {p.name} -> nat='{nat_ws.title if nat_ws else 'N/A'}' / seoul='{se_ws.title if se_ws else 'N/A'}' / ym={y%100}/{m}")

        df = read_month_df(p)
        counts, med, mean = agg_all_stats(df)
        month_cache[(y,m)] = {"counts":counts, "med":med, "mean":mean}

        if nat_ws:
            header = _retry(nat_ws.row_values, 1) or []
            series = {}
            for h in header:
                if not h or h=="날짜": continue
                series[h] = int(counts.get("전국",0)) if h=="총합계" else int(counts.get(h,0))
            write_month_sheet(nat_ws, today, series, sum_to="총합계")
            note_written(f"{nat_ws.title}\t{kdate_str(today)}\tOK")
        if se_ws:
            header = _retry(se_ws.row_values, 1) or []
            series = {}
            for h in header:
                if not h or h=="날짜": continue
                series[h] = int(counts.get("서울",0)) if h=="총합계" else int(counts.get(h,0))
            write_month_sheet(se_ws, today, series, sum_to="총합계")
            note_written(f"{se_ws.title}\t{kdate_str(today)}\tOK")

        ap = df[(df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")].copy()
        if not ap.empty: apgu_all.append(df)  # 전체 df 전달(함수에서 필터)

    if ws_summary and month_cache:
        for (y,m) in sorted(month_cache.keys(), key=lambda t: (t[0], t[1])):
            cur = month_cache[(y,m)]
            prv = month_cache.get((y, m-1) if m>1 else (y-1,12))
            prev_counts = prv["counts"] if prv else None
            write_month_summary(ws_summary, y, m, cur["counts"], cur["med"], cur["mean"], prev_counts)

    if apgu_all:
        ws_ap = ensure_apgu_sheet(sh)
        all_df = pd.concat(apgu_all, ignore_index=True)
        handled = set()
        for (y,m) in sorted(month_cache.keys(), key=lambda t:(t[0],t[1])):
            if (y,m) in handled: continue
            handled.add((y,m))
            upsert_apgu_month(ws_ap, all_df, y, m)

    log("[main] done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_error(str(e), e)
        sys.exit(1)
