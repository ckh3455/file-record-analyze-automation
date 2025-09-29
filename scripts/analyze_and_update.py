# scripts/analyze_and_update.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, re, sys, json, time, random
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ===================== 공통 설정 =====================
LOG_DIR = Path("analyze_report")
WORK_DIR = os.environ.get("ARTIFACTS_DIR", "artifacts")
DATA_SHEET_NAME = "data"

SUMMARY_SHEET_TITLE = "거래요약"   # 요약 시트 이름
SUMMARY_HEADER_FALLBACK = ["년월","구분"]  # 요약 시트에 헤더 없을 때 최소 컬럼

# 압구정동 본표에 유지할 컬럼(원문 그대로)
APGU_KEEP_COLS = [
    "광역","구","법정동","리","번지","본번","부번","단지명","전용면적(㎡)",
    "계약년","계약월","계약일","거래금액(만원)","동","층",
    "매수자","매도자","건축년도","도로명","해제사유발생일","거래유형",
    "중개사소재지","등기일자","주택유형"
]

# ===================== 로깅/리트라이 =====================
def _ensure_logdir():
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

_ensure_logdir()
RUNLOG = LOG_DIR / "latest.log"

def log(msg: str):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line)
    try:
        with RUNLOG.open("a", encoding="utf-8") as f: f.write(line+"\n")
    except Exception:
        pass

_LAST = 0.0
def _throttle(sec=0.45):
    global _LAST
    now = time.time()
    if now - _LAST < sec:
        time.sleep(sec - (now - _LAST))
    _LAST = time.time()

def _retry(fn, *a, **kw):
    base = 0.8
    for i in range(7):
        try:
            _throttle()
            return fn(*a, **kw)
        except APIError as e:
            s = str(e)
            if any(x in s for x in ("429","500","502","503")):
                time.sleep(base*(2**i) + random.uniform(0,0.25))
                continue
            raise

# ===================== 유틸 =====================
def ns(s: str) -> str:
    return "" if s is None else str(s).replace("\u00A0","").replace(" ", "").strip()

def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def kdate(d: date) -> str:
    return f"{d.year}. {d.month}. {d.day}"

def parse_filename(fname: str) -> Tuple[int,int]:
    # '전국 2507_250929.xlsx' → (yy=25, mm=7)
    m = re.search(r"(\d{2})(\d{2})_", fname)
    if not m: raise ValueError(f"bad filename: {fname}")
    return int(m.group(1)), int(m.group(2))

def ym_key(yy: int, mm: int) -> str:
    return f"{str(yy).zfill(2)}/{mm}"

def prev_ym(ym: str) -> str:
    yy, mm = ym.split("/")
    y = int(yy); m = int(mm)
    if m==1: return f"{(y-1):02d}/12"
    return f"{yy}/{m-1}"

# ===================== 시트 접근 =====================
def open_sheet() -> gspread.Spreadsheet:
    sa_raw = os.environ.get("SA_JSON","").strip()
    sa_path = os.environ.get("SA_PATH","sa.json")
    if sa_raw:
        creds = Credentials.from_service_account_info(
            json.loads(sa_raw),
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"],
        )
    else:
        creds = Credentials.from_service_account_file(
            sa_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"],
        )
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, os.environ.get("SHEET_ID","").strip())
    log("[gspread] spreadsheet opened")
    return sh

def find_ws_candidates(sh, titles: List[str]) -> Optional[gspread.Worksheet]:
    # 1) 정확히, 2) 공백 제거 후
    for t in titles:
        for ws in sh.worksheets():
            if ws.title == t:
                log(f"[ws] matched (exact): '{ws.title}'"); return ws
    want = [ns(t) for t in titles]
    for ws in sh.worksheets():
        if ns(ws.title) in want:
            log(f"[ws] matched (nospace): '{ws.title}'"); return ws
    return None

def find_or_append_date_row(ws: gspread.Worksheet, date_label: str) -> int:
    vals = _retry(ws.get_all_values)
    if not vals: return 2
    for i, row in enumerate(vals[1:], start=2):
        if (len(row)>0) and str(row[0]).strip()==date_label:
            return i
    return len(vals)+1

def ensure_rows(ws: gspread.Worksheet, need_end_row: int):
    if need_end_row > ws.row_count:
        _retry(ws.add_rows, need_end_row - ws.row_count)

def set_color_row(ws: gspread.Worksheet, row_idx: int, start_col: int, end_col: int, rgb: Tuple[float,float,float]):
    r,g,b = rgb
    req = {
        "repeatCell":{
            "range":{
                "sheetId": ws.id,
                "startRowIndex": row_idx-1, "endRowIndex": row_idx,
                "startColumnIndex": start_col-1, "endColumnIndex": end_col
            },
            "cell":{"userEnteredFormat":{"textFormat":{"foregroundColor":{"red":r,"green":g,"blue":b}}}},
            "fields":"userEnteredFormat.textFormat.foregroundColor"
        }
    }
    _retry(ws.spreadsheet.batch_update, {"requests":[req]})

# ===================== 파일/집계 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=DATA_SHEET_NAME, dtype=str)
    df = df.fillna("")
    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def eok_series(ser) -> pd.Series:
    s = pd.to_numeric(ser, errors="coerce").dropna()
    if s.empty: return pd.Series([], dtype=float)
    return s/10000.0

def agg_for_headers(df: pd.DataFrame, header: List[str]) -> Dict[str,int]:
    """
    header 에 적힌 지역명(전국 탭: 광역 공식명칭, 서울 탭: 구 이름)을 그대로 쓰기 위해
    - 헤더를 정규화(ns)해서 keys로 사용
    - 데이터 쪽도 같은 방식(ns)로 비교
    """
    out: Dict[str,int] = {}
    if "광역" in df.columns:
        # 광역 단위 (전국 탭)
        # 헤더에서 날짜/합계/총합계 제외
        want = [h for h in header if h and h not in ("날짜","합계","총합계")]
        # 데이터의 '광역'을 ns로 그룹
        key = df["광역"].map(ns)
        grp = key.groupby(key).size()
        for h in want:
            out[h] = int(grp.get(ns(h), 0))
    return out

def agg_seoul_for_headers(df: pd.DataFrame, header: List[str]) -> Dict[str,int]:
    out: Dict[str,int] = {}
    if "광역" in df.columns and "구" in df.columns:
        se = df[ns(df["광역"]) == ns("서울특별시")]
        want = [h for h in header if h and h not in ("날짜","합계","총합계")]
        key = se["구"].map(ns)
        grp = key.groupby(key).size()
        for h in want:
            out[h] = int(grp.get(ns(h), 0))
    return out

def med_mean_for_headers(df: pd.DataFrame, header: List[str]) -> Tuple[Dict[str,str], Dict[str,str]]:
    med: Dict[str,str] = {}
    mean: Dict[str,str] = {}
    if "거래금액(만원)" not in df.columns: 
        return med, mean

    def r2(x): 
        try: return f"{float(x):.2f}"
        except: return ""

    # 전국/광역
    if "광역" in df.columns:
        for h in header:
            if not h or h in ("날짜","합계","총합계"): continue
            sub = df[ns(df["광역"]) == ns(h)]
            if len(sub)>0:
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[h] = r2(s.median()); mean[h] = r2(s.mean())

    # 서울/구
    if "광역" in df.columns and "구" in df.columns:
        se = df[ns(df["광역"]) == ns("서울특별시")]
        for h in header:
            if not h or h in ("날짜","합계","총합계"): continue
            sub = se[ns(se["구"]) == ns(h)]
            if len(sub)>0:
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[h] = r2(s.median()); mean[h] = r2(s.mean())

    return med, mean

# ===================== 월별 탭 쓰기 =====================
def build_full_row(header: List[str], date_label: str, value_by_name: Dict[str,int]) -> List:
    row = []
    total = 0
    for i,h in enumerate(header):
        if i==0: row.append(date_label); continue
        if not h: row.append(""); continue
        if h in ("합계","총합계"):
            row.append(total); continue
        v = int(value_by_name.get(h, 0))
        row.append(v); total += v
    return row

def write_month_sheet(ws: gspread.Worksheet, date_label: str, header: List[str], value_by_name: Dict[str,int]):
    row_idx = find_or_append_date_row(ws, date_label)
    full = build_full_row(header, date_label, value_by_name)
    rng = f"A{row_idx}:{a1_col(len(header))}{row_idx}"
    _retry(ws.update, [full], rng)
    log(f"[ws] {ws.title} -> {date_label} row={row_idx}")

# ===================== 거래요약 쓰기 (YY/MM 행) =====================
def get_summary_header(ws: gspread.Worksheet) -> List[str]:
    hdr = _retry(ws.row_values, 1) or []
    if not hdr:
        _retry(ws.update, [SUMMARY_HEADER_FALLBACK], "A1")
        hdr = SUMMARY_HEADER_FALLBACK[:]
    return hdr

def find_summary_row(ws: gspread.Worksheet, ym: str, label: str) -> int:
    vals = _retry(ws.get_all_values) or []
    for i, row in enumerate(vals[1:], start=2):
        a = str(row[0]).strip() if len(row)>0 else ""
        b = str(row[1]).strip() if len(row)>1 else ""
        if a==ym and b==label:
            return i
    return len(vals)+1

def put_summary_line(ws: gspread.Worksheet, row_idx: int, ym: str, label: str, line_map: Dict[str, str|int]):
    header = get_summary_header(ws)
    if header[:2] != ["년월","구분"]:
        header = ["년월","구분"] + header[2:]
        _retry(ws.update, [header], "A1")

    payload = [
        {"range": f"A{row_idx}", "values": [[ym]]},
        {"range": f"B{row_idx}", "values": [[label]]},
    ]
    hmap = {h:i+1 for i,h in enumerate(header)}
    for h in header[2:]:
        v = line_map.get(h, "")
        payload.append({"range": f"{a1_col(hmap[h])}{row_idx}", "values": [[v]]})
    _retry(ws.spreadsheet.values_batch_update, body={"valueInputOption":"USER_ENTERED","data":payload})

def color_diffs(ws: gspread.Worksheet, row_idx: int, diff_map: Dict[str,str]):
    header = get_summary_header(ws)
    hmap = {h:i+1 for i,h in enumerate(header)}
    reqs = []
    for h,v in diff_map.items():
        if h not in hmap: continue
        if str(v) in ("","0"): continue
        rgb = (0.0,0.35,1.0) if str(v).startswith("+") else (1.0,0.0,0.0)
        reqs.append({
            "repeatCell":{
                "range":{
                    "sheetId": ws.id,
                    "startRowIndex": row_idx-1, "endRowIndex": row_idx,
                    "startColumnIndex": hmap[h]-1, "endColumnIndex": hmap[h]
                },
                "cell":{"userEnteredFormat":{"textFormat":{"foregroundColor":{"red":rgb[0],"green":rgb[1],"blue":rgb[2]}}}},
                "fields":"userEnteredFormat.textFormat.foregroundColor"
            }
        })
    if reqs:
        _retry(ws.spreadsheet.batch_update, {"requests": reqs})

# ===================== 압구정동 =====================
def upsert_apgu(ws: gspread.Worksheet, df_all: pd.DataFrame, run_day: date):
    base = df_all[df_all.get("법정동","")=="압구정동"].copy()
    if base.empty:
        log("[압구정동] no rows"); return

    # 누락 컬럼 채우기
    for c in APGU_KEEP_COLS:
        if c not in base.columns:
            base[c] = ""

    # 날짜 오름차순
    for c in ["계약년","계약월","계약일"]:
        if c not in base.columns: base[c] = 0
        base[c] = pd.to_numeric(base[c], errors="coerce").fillna(0).astype(int)
    base = base.sort_values(["계약년","계약월","계약일","단지명","전용면적(㎡)"], kind="mergesort")

    # 헤더 보장
    vals = _retry(ws.get_all_values) or []
    if not vals:
        _retry(ws.update, [APGU_KEEP_COLS], "A1")
        vals = [APGU_KEEP_COLS]
    header = vals[0]
    if header != APGU_KEEP_COLS:
        _retry(ws.update, [APGU_KEEP_COLS], "A1")
        header = APGU_KEEP_COLS

    # 기존 본표(변경 요약 제외) 읽기
    body = (_retry(ws.get_all_values) or [header])[1:]
    old_rows = []
    for r in body:
        if r and r[0] in ("변경구분","(신규)","(삭제)"):
            break
        old_rows.append((r + [""]*len(header))[:len(header)])

    new_rows = []
    for _, row in base.iterrows():
        new_rows.append([str(row.get(c,"")).strip() for c in APGU_KEEP_COLS])

    # 본표 덮어쓰기
    start = 2
    end = start + max(len(new_rows),1) - 1
    ensure_rows(ws, end)
    _retry(ws.update, new_rows, f"A{start}:{a1_col(len(header))}{end}")
    log(f"[압구정동] base rows written: {len(new_rows)}")

    # 변경 비교 (키: 모든 보존 컬럼 연결)
    def key_of(r: List[str]) -> str:
        return "|".join((r + [""]*len(header))[:len(header)])

    old_keys = {key_of(r): r for r in old_rows}
    new_keys = {key_of(r): r for r in new_rows}

    added = [new_keys[k] for k in new_keys.keys() - old_keys.keys()]
    removed = [old_keys[k] for k in old_keys.keys() - new_keys.keys()]

    if not added and not removed:
        log("[압구정동] changes: none")
        return

    today = kdate(run_day)
    change_header = ["변경구분","변경일"] + APGU_KEEP_COLS
    change_rows = [change_header]
    for r in added:
        change_rows.append(["(신규)", today] + r)
    for r in removed:
        change_rows.append(["(삭제)", today] + r)

    start2 = end + 1
    end2 = start2 + len(change_rows) - 1
    ensure_rows(ws, end2)
    _retry(ws.update, change_rows, f"A{start2}:{a1_col(len(change_header))}{end2}")

    # 빨간색
    req = {
        "repeatCell":{
            "range":{
                "sheetId": ws.id,
                "startRowIndex": start2-1, "endRowIndex": end2,
                "startColumnIndex": 0, "endColumnIndex": len(change_header)
            },
            "cell":{"userEnteredFormat":{"textFormat":{"foregroundColor":{"red":1.0,"green":0.0,"blue":0.0}}}},
            "fields":"userEnteredFormat.textFormat.foregroundColor"
        }
    }
    _retry(ws.spreadsheet.batch_update, {"requests":[req]})
    log(f"[압구정동] changes: 신규={len(added)} 삭제={len(removed)}")

# ===================== 메인 =====================
def main():
    # 로그 초기화
    try: RUNLOG.write_text("", encoding="utf-8")
    except Exception: pass

    log("[MAIN]")
    log(f"artifacts_dir={WORK_DIR}")

    sh = open_sheet()
    today = datetime.now().date()
    date_label = kdate(today)

    # 파일 수집
    files = sorted(Path(WORK_DIR).rglob("전국 *.xlsx"))
    log(f"[collect] found {len(files)} xlsx files")

    # 거래요약 캐시: ym -> {counts, med, mean}
    summary_cache: Dict[str, Dict[str, Dict[str,str|int]]] = {}
    apgu_concat: List[pd.DataFrame] = []

    for p in files:
        try:
            yy, mm = parse_filename(p.name)
        except:
            log(f"[file] skip {p.name}"); continue
        ym = ym_key(yy, mm)
        nat_cands = [f"전국 {yy}년 {mm}월", f"전국 {2000+yy}년 {mm}월"]
        se_cands  = [f"서울 {yy}년 {mm}월", f"서울 {2000+yy}년 {mm}월"]
        log(f"[file] {p.name} -> nat={nat_cands} / seoul={se_cands}")

        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        # ----- 월별 탭 기록 (탭 헤더를 기준으로 동적 매핑) -----
        ws_nat = find_ws_candidates(sh, nat_cands)
        if ws_nat:
            h_nat = _retry(ws_nat.row_values, 1)
            nat_counts = agg_for_headers(df, h_nat)
            write_month_sheet(ws_nat, date_label, h_nat, nat_counts)
        else:
            log(f"[전국] sheet not found: {nat_cands}")

        ws_se = find_ws_candidates(sh, se_cands)
        if ws_se:
            h_se = _retry(ws_se.row_values, 1)
            se_counts = agg_seoul_for_headers(df, h_se)
            write_month_sheet(ws_se, date_label, h_se, se_counts)
        else:
            log(f"[서울] sheet not found: {se_cands}")

        # ----- 거래요약 집계 (헤더 이름별로 그대로 쓰기) -----
        # counts
        counts_nat = {}
        if "광역" in df.columns:
            g = df["광역"].map(ns)
            s = g.groupby(g).size()
            for k,v in s.items():
                counts_nat[k] = int(v)

        counts_se = {}
        if "광역" in df.columns and "구" in df.columns:
            se = df[ns(df["광역"])==ns("서울특별시")]
            g2 = se["구"].map(ns)
            s2 = g2.groupby(g2).size()
            for k,v in s2.items():
                counts_se[k] = int(v)

        # 중앙값/평균(억) – 헤더 기준
        med_map_nat, mean_map_nat = med_mean_for_headers(df, list(counts_nat.keys()))
        med_map_se,  mean_map_se  = med_mean_for_headers(df, list(counts_se.keys()))

        # 합치기(요약 시트는 ‘전국/서울/각 광역/각 구’가 한 시트에 있으므로 키 병합)
        counts_all: Dict[str,int] = {}
        counts_all["전국"] = int(len(df))
        # 서울 전체
        if "광역" in df.columns:
            se_df = df[ns(df["광역"])==ns("서울특별시")]
            counts_all["서울"] = int(len(se_df))
        # 광역/구 키 그대로 추가
        for k,v in counts_nat.items(): counts_all[k] = v
        for k,v in counts_se.items():  counts_all[k] = v

        # 중앙값/평균
        med_all: Dict[str,str] = {}
        mean_all: Dict[str,str] = {}
        # 전국/서울
        s_all = eok_series(df.get("거래금액(만원)", []))
        if not s_all.empty:
            med_all["전국"] = f"{float(s_all.median()):.2f}"
            mean_all["전국"] = f"{float(s_all.mean()):.2f}"
        if "광역" in df.columns:
            if len(se_df)>0:
                s = eok_series(se_df["거래금액(만원)"])
                if not s.empty:
                    med_all["서울"] = f"{float(s.median()):.2f}"
                    mean_all["서울"] = f"{float(s.mean()):.2f}"
        # 광역/구
        med_all.update(med_map_nat);  mean_all.update(mean_map_nat)
        med_all.update(med_map_se);   mean_all.update(mean_map_se)

        summary_cache[ym] = {"counts": counts_all, "med": med_all, "mean": mean_all}

        # ----- 압구정동 누적 -----
        ap = df[(ns(df.get("광역",""))==ns("서울특별시")) & (ns(df.get("법정동",""))==ns("압구정동"))]
        if not ap.empty:
            apgu_concat.append(ap)

    # ===== 거래요약 시트 쓰기 (YY/MM 행) =====
    ws_sum = None
    for ws in sh.worksheets():
        if ns(ws.title) == ns(SUMMARY_SHEET_TITLE):
            ws_sum = ws; break
    if ws_sum and summary_cache:
        # 요약 시트의 헤더를 기준으로 ‘어떤 열명이든’ 그대로 사용
        header = get_summary_header(ws_sum)

        # YM 순서대로
        for ym in sorted(summary_cache.keys(), key=lambda s: (int(s.split("/")[0]), int(s.split("/")[1]))):
            cur = summary_cache[ym]
            prv = summary_cache.get(prev_ym(ym))
            # 1) 거래건수
            line_counts: Dict[str,int|str] = {}
            for h in header[2:]:
                line_counts[h] = int(cur["counts"].get(h, 0))
            r1 = find_summary_row(ws_sum, ym, "거래건수")
            put_summary_line(ws_sum, r1, ym, "거래건수", line_counts)
            log(f"[summary] {ym} 거래건수 -> row={r1}")

            # 2) 중앙값(단위:억)
            line_med: Dict[str,str] = {}
            for h in header[2:]:
                line_med[h] = cur["med"].get(h, "")
            r2 = find_summary_row(ws_sum, ym, "중앙값(단위:억)")
            put_summary_line(ws_sum, r2, ym, "중앙값(단위:억)", line_med)
            log(f"[summary] {ym} 중앙값 -> row={r2}")

            # 3) 평균가(단위:억)
            line_mean: Dict[str,str] = {}
            for h in header[2:]:
                line_mean[h] = cur["mean"].get(h, "")
            r3 = find_summary_row(ws_sum, ym, "평균가(단위:억)")
            put_summary_line(ws_sum, r3, ym, "평균가(단위:억)", line_mean)
            log(f"[summary] {ym} 평균가 -> row={r3}")

            # 4) 전월대비 건수증감
            diffs: Dict[str,str] = {}
            if prv:
                for h in header[2:]:
                    d = int(cur["counts"].get(h,0)) - int(prv["counts"].get(h,0))
                    diffs[h] = f"+{d}" if d>0 else (str(d) if d<0 else "0")
            else:
                for h in header[2:]: diffs[h] = ""
            r4 = find_summary_row(ws_sum, ym, "전월대비 건수증감")
            put_summary_line(ws_sum, r4, ym, "전월대비 건수증감", diffs)
            color_diffs(ws_sum, r4, diffs)
            log(f"[summary] {ym} 전월대비 -> row={r4}")

            # 5) 예상건수(빈칸 유지)
            blanks = {h:"" for h in header[2:]}
            r5 = find_summary_row(ws_sum, ym, "예상건수")
            put_summary_line(ws_sum, r5, ym, "예상건수", blanks)
            log(f"[summary] {ym} 예상건수 -> row={r5}")

    # ===== 압구정동 (원문 그대로 + 변경요약) =====
    if apgu_concat:
        ws_ap = None
        for ws in sh.worksheets():
            if ns(ws.title) == ns("압구정동"):
                ws_ap = ws; break
        if ws_ap:
            upsert_apgu(ws_ap, pd.concat(apgu_concat, ignore_index=True), today)
        else:
            log("[압구정동] sheet not found (skip)")

    log("[main] done")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {repr(e)}")
        raise
