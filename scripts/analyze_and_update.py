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


# ===================== 로그 =====================
LOG_DIR = Path("analyze_report")
def _ensure_logdir():
    if LOG_DIR.exists() and not LOG_DIR.is_dir():
        try: LOG_DIR.unlink()
        except Exception: pass
    LOG_DIR.mkdir(parents=True, exist_ok=True)
_ensure_logdir()

RUN_LOG = LOG_DIR / f"run-{datetime.now(ZoneInfo('Asia/Seoul')).strftime('%Y%m%dT%H%M%S%z')}.log"
LATEST = LOG_DIR / "latest.log"
WRITTEN = LOG_DIR / "where_written.txt"

def _t(): return datetime.now(ZoneInfo("Asia/Seoul")).strftime("[%H:%M:%S]")

def _w(line: str):
    print(line)
    try:
        with RUN_LOG.open("a", encoding="utf-8") as f: f.write(line+"\n")
        with LATEST.open("a", encoding="utf-8") as f: f.write(line+"\n")
    except Exception:
        pass

def log(msg: str): _w(f"{_t()} {msg}")
def note_written(s: str):
    try:
        with WRITTEN.open("a", encoding="utf-8") as f: f.write(s.rstrip()+"\n")
    except Exception:
        pass
def log_error(msg: str, exc: Optional[BaseException]=None):
    _w(f"{_t()} [ERROR] {msg}")
    if exc:
        import traceback
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        print(tb, file=sys.stderr)
        try:
            with RUN_LOG.open("a", encoding="utf-8") as f: f.write(tb+"\n")
            with LATEST.open("a", encoding="utf-8") as f: f.write(tb+"\n")
        except Exception:
            pass


# ===================== 공통 유틸 =====================
def kdate_str(d: date) -> str:
    # 구글시트 표시와 동일: 2025. 9. 28
    return f"{d.year}. {d.month}. {d.day}"

def fmt2(v) -> str:
    try:
        return f"{float(v):.2f}"
    except Exception:
        return ""

def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def norm_raw(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).strip()

def ym_from_filename(fname: str) -> Tuple[int,int,int]:
    """
    '전국 2509_250928.xlsx' → (2025,9,28)
    """
    m = re.search(r"(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})", fname)
    if not m:
        raise ValueError(f"unexpected filename: {fname}")
    y = 2000 + int(m.group(1))
    mth = int(m.group(2))
    day = int(m.group(5))
    return y, mth, day

def ym_label(y: int, m: int) -> str:
    return f"{str(y%100).zfill(2)}/{m}"

def prev_ym_label(y: int, m: int) -> str:
    if m>1: return ym_label(y, m-1)
    return ym_label(y-1, 12)


# ===================== 탭 정규화(2자리/4자리 연도 둘 다 허용) =====================
def tab_title_key(title: str) -> Optional[str]:
    """
    '전국 25년 9월' → '전국-25-9'
    '전국 2025년 9월' → '전국-25-9'
    '서울 25년 07월' → '서울-25-7'
    """
    s = title.replace(" ", "")
    m = re.match(r"^(전국|서울)(\d{2,4})년(\d{1,2})월$", s)
    if not m: return None
    city = m.group(1)
    y = int(m.group(2)) % 100
    mm = int(m.group(3))
    return f"{city}-{y:02d}-{mm}"

def find_month_sheets(sh: gspread.Spreadsheet, y: int, m: int) -> Tuple[Optional[gspread.Worksheet], Optional[gspread.Worksheet]]:
    want_nat = f"전국-{y%100:02d}-{m}"
    want_se  = f"서울-{y%100:02d}-{m}"
    ws_nat = ws_se = None
    for ws in sh.worksheets():
        key = tab_title_key(ws.title or "")
        if key == want_nat: ws_nat = ws
        if key == want_se:  ws_se  = ws
    return ws_nat, ws_se


# ===================== gspread 래퍼 (429 완화) =====================
_LAST_TS = 0.0
def _throttle(sec=0.40):
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

def values_batch_update(ws: gspread.Worksheet, payload: List[Dict]):
    body = {"valueInputOption": "USER_ENTERED", "data": payload}
    return _retry(ws.spreadsheet.values_batch_update, body=body)

def batch_format(ws: gspread.Worksheet, requests: List[dict]):
    if not requests: return
    _retry(ws.spreadsheet.batch_update, {"requests": requests})


# ===================== 파일 읽기/집계 =====================
NEEDED_COLS = ["광역","구","법정동","계약년","계약월","계약일","거래금액(만원)"]

PROV_MAP = {
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
    df = pd.read_excel(path, sheet_name="data", dtype=str, engine="openpyxl")
    df = df.fillna("")
    # 숫자 변환
    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in NEEDED_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    return df[NEEDED_COLS].copy()

def eok_series(ser) -> pd.Series:
    s = pd.to_numeric(ser, errors="coerce").dropna()
    if s.empty: return pd.Series([], dtype=float)
    return s / 10000.0

def agg_all_stats(df: pd.DataFrame):
    counts = {col:0 for col in SUMMARY_COLS}
    med = {col:"" for col in SUMMARY_COLS}
    mean = {col:"" for col in SUMMARY_COLS}

    # 전국
    all_eok = eok_series(df["거래금액(만원)"])
    counts["전국"] = int(len(df))
    if not all_eok.empty:
        med["전국"] = fmt2(all_eok.median())
        mean["전국"] = fmt2(all_eok.mean())

    # 광역
    for prov, sub in df.groupby("광역"):
        prov_std = PROV_MAP.get(str(prov), str(prov))
        if prov_std in counts:
            counts[prov_std] += int(len(sub))
            s = eok_series(sub["거래금액(만원)"])
            if not s.empty:
                med[prov_std] = fmt2(s.median())
                mean[prov_std] = fmt2(s.mean())

    # 서울 및 구
    seoul = df[df.get("광역","")=="서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul)>0:
        s = eok_series(seoul["거래금액(만원)"])
        if not s.empty:
            med["서울"] = fmt2(s.median())
            mean["서울"] = fmt2(s.mean())
    if "구" in seoul.columns:
        for gu, sub in seoul.groupby("구"):
            gu = str(gu)
            if gu in counts:
                counts[gu] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[gu] = fmt2(s.median())
                    mean[gu] = fmt2(s.mean())

    # 압구정동
    ap = seoul[seoul.get("법정동","")=="압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap)>0:
        s = eok_series(ap["거래금액(만원)"])
        med["압구정동"] = fmt2(s.median())
        mean["압구정동"] = fmt2(s.mean())

    return counts, med, mean


# ===================== 월별 탭(전국/서울) 기록 =====================
def find_date_col_idx(ws) -> int:
    header = _retry(ws.row_values, 1)
    for i, v in enumerate(header, start=1):
        if str(v).strip() == "날짜":
            return i
    return 1

def find_or_append_date_row(ws, target: date, date_col_idx: int = 1) -> int:
    vals = _retry(ws.get_all_values)
    if not vals:  # 헤더만 없으면 최소 1행
        return 2
    # 날짜 찾기
    for i, row in enumerate(vals[1:], start=2):
        cell = row[0] if row else ""
        s = str(cell).strip()
        if not s: continue
        m = re.match(r"^\s*(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\s*$", s)
        if m:
            y, mm, dd = map(int, m.groups())
            if date(y,mm,dd) == target:
                return i
    # 없으면 추가 위치
    return len(vals)+1

def write_month_sheet(ws, when: date, series: Dict[str,int], sum_to: str = "총합계"):
    header = _retry(ws.row_values, 1)
    hmap = {h: i+1 for i,h in enumerate(header)}
    row = find_or_append_date_row(ws, when, find_date_col_idx(ws))
    out_pay = [{"range": f"A{row}", "values": [[kdate_str(when)]]}]
    total = 0
    for h in header[1:]:
        if h == sum_to:
            continue
        v = int(series.get(h, 0) or 0)
        out_pay.append({"range": f"{a1_col(hmap[h])}{row}", "values": [[v]]})
        if h != "전국":
            total += v
    if sum_to in hmap:
        out_pay.append({"range": f"{a1_col(hmap[sum_to])}{row}", "values": [[int(series.get('전국', total))]]})
    values_batch_update(ws, out_pay)
    log(f"[ws] {ws.title} -> {kdate_str(when)} row={row}")


# ===================== 거래요약 =====================
def ensure_summary_header(ws):
    vals = _retry(ws.get_all_values) or []
    if not vals:
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
    header = _retry(ws.row_values, 1)
    hmap = {h: i+1 for i,h in enumerate(header)}
    payload = [
        {"range": f"A{row_idx}", "values": [[ym]]},
        {"range": f"B{row_idx}", "values": [[label]]},
    ]
    for col in SUMMARY_COLS:
        if col in hmap:
            payload.append({"range": f"{a1_col(hmap[col])}{row_idx}", "values": [[line_map.get(col, "")]]})
    values_batch_update(ws, payload)

def color_diff_cells(ws, row_idx: int, diff_line: dict):
    header = _retry(ws.row_values, 1)
    hmap = {h: i+1 for i,h in enumerate(header)}
    reqs = []
    for k, v in diff_line.items():
        if k not in hmap: continue
        if v.startswith("+"):
            color = {"red":0.0,"green":0.35,"blue":1.0}
        elif v.startswith("-"):
            color = {"red":1.0,"green":0.0,"blue":0.0}
        else:
            continue
        reqs.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": row_idx-1,
                    "endRowIndex": row_idx,
                    "startColumnIndex": hmap[k]-1,
                    "endColumnIndex": hmap[k]
                },
                "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": color}}},
                "fields": "userEnteredFormat.textFormat.foregroundColor"
            }
        })
    batch_format(ws, reqs)

def write_month_summary(ws, y: int, m: int, counts: dict, med: dict, mean: dict, prev_counts: Optional[dict]):
    ensure_summary_header(ws)
    ym = ym_label(y, m)

    r1 = find_summary_row(ws, ym, "거래건수")
    # 거래건수는 볼드
    put_summary_line(ws, r1, ym, "거래건수", counts)
    header = _retry(ws.row_values, 1)
    if header:
        req = {
            "repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": r1-1, "endRowIndex": r1, "startColumnIndex": 2, "endColumnIndex": len(header)},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold"
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
    diff_line = {}
    if prev_counts:
        for k in SUMMARY_COLS:
            cur = int(counts.get(k, 0) or 0)
            prv = int(prev_counts.get(k, 0) or 0)
            d = cur - prv
            diff_line[k] = f"+{d}" if d>0 else (str(d) if d<0 else "0")
    else:
        for k in SUMMARY_COLS: diff_line[k] = ""
    put_summary_line(ws, r4, ym, "전월대비 건수증감", diff_line)
    color_diff_cells(ws, r4, diff_line)
    log(f"[summary] {ym} 전월대비 -> row={r4}")

    r5 = find_summary_row(ws, ym, "예상건수")
    blanks = {k:"" for k in SUMMARY_COLS}
    put_summary_line(ws, r5, ym, "예상건수", blanks)
    log(f"[summary] {ym} 예상건수 -> row={r5}")


# ===================== 압구정동: 월별 원본 + 증감 로그 =====================
def ensure_apgu_sheet(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    for ws in sh.worksheets():
        if ws.title.strip() == "압구정동":
            return ws
    return _retry(sh.add_worksheet, title="압구정동", rows=4000, cols=80)

def number_or_blank(v):
    if v is None: return ""
    if isinstance(v,float) and (math.isnan(v)): return ""
    return v

def upsert_apgu_month(ws: gspread.Worksheet, df: pd.DataFrame, y: int, m: int):
    # 해당 월의 압구정동 원본
    cond = (df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")
    dd = df[cond].copy()
    if "계약년" in dd and "계약월" in dd and "계약일" in dd:
        dd = dd[(dd["계약년"]==y) & (dd["계약월"]==m)]
        dd = dd.sort_values(["계약년","계약월","계약일"], ascending=[True,True,True], kind="mergesort")
    if dd.empty:
        log(f"[압구정동] {y%100}/{m} no rows")
        return

    vals = _retry(ws.get_all_values) or []
    if not vals:
        header = list(NEEDED_COLS) + ["기록일","변경구분","변동일"]
        _retry(ws.update, [header], "A1")
        vals = [header]
    else:
        header = vals[0]
        for need in ["기록일","변경구분","변동일"]:
            if need not in header:
                header.append(need)
        if header != vals[0]:
            _retry(ws.update, [header], "A1")
            vals = _retry(ws.get_all_values) or [header]
    idx = {h:i for i,h in enumerate(header)}

    def make_key(d: dict) -> str:
        parts = [
            d.get("계약년",""), d.get("계약월",""), d.get("계약일",""),
            d.get("단지명",""), d.get("전용면적(㎡)",""), d.get("동",""),
            d.get("층",""), d.get("거래금액(만원)","")
        ]
        return "|".join(str(x).strip() for x in parts)

    def row_to_dict(row: List[str]) -> dict:
        return {k: (row[i] if i<len(row) else "") for k,i in idx.items()}

    existing_rows = vals[1:]
    # 해당 월의 기존키
    exist_month_keys = set()
    for r in existing_rows:
        d = row_to_dict(r)
        try:
            if int(float(d.get("계약년","0"))) == y and int(float(d.get("계약월","0"))) == m:
                exist_month_keys.add(make_key(d))
        except Exception:
            pass

    # 파일의 월키
    file_month_keys = set()
    for _, r in dd.iterrows():
        d = {k: r.get(k, "") for k in header if k in dd.columns}
        file_month_keys.add(make_key(d))

    # 본문 추가(기존 월에 없는 것만)
    today = kdate_str(datetime.now(ZoneInfo("Asia/Seoul")).date())
    to_append_main = []
    for _, r in dd.iterrows():
        d = {k: r.get(k, "") for k in header if k in dd.columns}
        k = make_key(d)
        if k in exist_month_keys:
            continue
        rec = [number_or_blank(d.get(h,"")) for h in header]
        if "기록일" in idx:
            rec[idx["기록일"]] = today
        to_append_main.append(rec)

    if to_append_main:
        start = len(vals)+1
        end = start + len(to_append_main) - 1
        # row capacity check
        if end > ws.row_count:
            _retry(ws.add_rows, end - ws.row_count)
        rng = f"A{start}:{a1_col(len(header))}{end}"
        _retry(ws.update, to_append_main, rng)
        vals = _retry(ws.get_all_values) or [header]
        existing_rows = vals[1:]

    # 증감 로그 산출
    # 전체 키(본문 추가 이후)
    exist_all_keys = set()
    for r in existing_rows:
        exist_all_keys.add(make_key(row_to_dict(r)))

    added_logs, removed_logs = [], []
    # 신규: 파일엔 있으나 시트에 아직 없던 월 키
    for _, r in dd.iterrows():
        d = {k: r.get(k, "") for k in header if k in dd.columns}
        k = make_key(d)
        if k not in exist_month_keys and k not in exist_all_keys:
            cp = [d.get(h,"") for h in header]
            if "변경구분" in idx: cp[idx["변경구분"]] = "(신규)"
            if "변동일"  in idx: cp[idx["변동일"]]  = today
            added_logs.append(cp)

    # 삭제: 시트 월키 중 파일에 없는 것
    for r in existing_rows:
        d = row_to_dict(r)
        try:
            if int(float(d.get("계약년","0"))) == y and int(float(d.get("계약월","0"))) == m:
                if make_key(d) not in file_month_keys:
                    cp = [d.get(h,"") for h in header]
                    if "변경구분" in idx: cp[idx["변경구분"]] = "(삭제)"
                    if "변동일"  in idx: cp[idx["변동일"]]  = today
                    removed_logs.append(cp)
        except Exception:
            pass

    changes = added_logs + removed_logs
    if changes:
        start = (len(_retry(ws.get_all_values)) or 1) + 1
        end = start + len(changes) - 1
        if end > ws.row_count:
            _retry(ws.add_rows, end - ws.row_count)
        rng = f"A{start}:{a1_col(len(header))}{end}"
        _retry(ws.update, changes, rng)
        # 빨간 글자
        batch_format(ws, [{
            "repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": start-1, "endRowIndex": end, "startColumnIndex": 0, "endColumnIndex": len(header)},
                "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red":1.0,"green":0.0,"blue":0.0}}}},
                "fields":"userEnteredFormat.textFormat.foregroundColor"
            }
        }])

    log(f"[압구정동] {y%100}/{m} new={len(added_logs)} removed={len(removed_logs)} main_appended={len(to_append_main)}")


# ===================== 메인 =====================
def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts-dir", default="artifacts")
    ap.add_argument("--sheet-id", default=os.environ.get("SHEET_ID",""))
    ap.add_argument("--sa", default="sa.json")
    args = ap.parse_args()

    # latest 초기화
    try:
        if LATEST.exists(): LATEST.unlink()
        if WRITTEN.exists(): WRITTEN.unlink()
    except Exception:
        pass

    log("[MAIN]")
    art = Path(args.artifacts_dir)
    files = sorted([p for p in art.rglob("전국 *.xlsx") if p.is_file()])
    log(f"[collect] found {len(files)} xlsx files")

    # 인증
    sa_raw = os.environ.get("SA_JSON","").strip()
    if sa_raw:
        creds = Credentials.from_service_account_info(
            json.loads(sa_raw),
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        )
    else:
        creds = Credentials.from_service_account_file(
            args.sa,
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        )
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, args.sheet_id)
    log("[gspread] spreadsheet opened")

    # 오늘 날짜(KST)
    today = datetime.now(ZoneInfo("Asia/Seoul")).date()
    log(f"[date] {kdate_str(today)}")

    # 거래요약 시트
    ws_summary = None
    for ws in sh.worksheets():
        if ws.title.strip() == "거래요약":
            ws_summary = ws
            break

    # 월별 요약 캐시
    month_cache: Dict[Tuple[int,int], Dict] = {}

    # 압구정동 누적
    apgu_all: List[pd.DataFrame] = []

    for p in files:
        try:
            y, m, file_day = ym_from_filename(p.name)
        except Exception as e:
            log_error(f"filename parse failed: {p.name}", e); continue

        nat_ws, se_ws = find_month_sheets(sh, y, m)
        log(f"[file] {p.name} -> nat='{nat_ws.title if nat_ws else 'N/A'}' / seoul='{se_ws.title if se_ws else 'N/A'}' / ym={y%100}/{m}")

        df = read_month_df(p)
        counts, med, mean = agg_all_stats(df)
        month_cache[(y,m)] = {"counts": counts, "med": med, "mean": mean}

        # 월 시트 기록(오늘 날짜)
        if nat_ws:
            # 월 탭의 헤더 기준으로 필요한 열만 쓰기
            header = _retry(nat_ws.row_values, 1)
            series = {}
            for h in header:
                if not h or h=="날짜": continue
                if h=="총합계":
                    series[h] = int(counts.get("전국", 0))
                else:
                    series[h] = int(counts.get(h, 0))
            write_month_sheet(nat_ws, today, series, sum_to="총합계")
            note_written(f"{nat_ws.title}\t{kdate_str(today)}\tOK")
        else:
            log(f"[전국] sheet not found for {y%100}/{m} (both 2-digit/4-digit matched)")

        if se_ws:
            header = _retry(se_ws.row_values, 1)
            series = {}
            for h in header:
                if not h or h=="날짜": continue
                if h=="총합계":
                    series[h] = int(counts.get("서울", 0))
                else:
                    series[h] = int(counts.get(h, 0))
            write_month_sheet(se_ws, today, series, sum_to="총합계")
            note_written(f"{se_ws.title}\t{kdate_str(today)}\tOK")
        else:
            log(f"[서울] sheet not found for {y%100}/{m} (both 2-digit/4-digit matched)")

        # 압구정동 누적(원본)
        ap = df[(df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")].copy()
        if not ap.empty:
            apgu_all.append(ap)

        # 거래요약(첫 파일에서만 써도 되지만, 여기선 모든 월에 대해 한 번에 아래서 기록)

    # 거래요약: 월별로 기록(+전월대비 색상)
    if ws_summary and month_cache:
        yms = sorted(month_cache.keys(), key=lambda t: (t[0], t[1]))
        for (y,m) in yms:
            cur = month_cache[(y,m)]
            prev = month_cache.get((y, m-1) if m>1 else (y-1, 12))
            prev_counts = prev["counts"] if prev else None
            write_month_summary(ws_summary, y, m, cur["counts"], cur["med"], cur["mean"], prev_counts)

    # 압구정동: 월별로 원본 반영 + 증감 로그
    if apgu_all:
        ws_ap = ensure_apgu_sheet(sh)
        all_df = pd.concat(apgu_all, ignore_index=True)
        # 파일에 포함된 월만 처리
        handled = set()
        for (y,m) in month_cache.keys():
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
