#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyze_and_update.py  (전체코드)

변경점:
- analyze_report 경로가 '파일'로 존재해도 안전하게 디렉터리 보장
- 나머지 동작(집계/총합계/거래요약/압구정동 원본+변동로그)은 이전 버전 그대로
"""

from __future__ import annotations
import os, sys, re, json, time, shutil
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional

import pandas as pd
import numpy as np
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

# ==================== 로그 디렉터리 보장 ====================
LOG_DIR = Path("analyze_report")
if LOG_DIR.exists():
    if LOG_DIR.is_file():            # 파일로 남아있던 케이스 방지
        LOG_DIR.unlink()
    # 심볼릭 링크 등 특수 케이스면 제거 후 생성
    elif not LOG_DIR.is_dir():
        try:
            LOG_DIR.unlink()
        except Exception:
            try:
                shutil.rmtree(LOG_DIR, ignore_errors=True)
            except Exception:
                pass
LOG_DIR.mkdir(parents=True, exist_ok=True)

def _now(): return datetime.now().strftime("%H:%M:%S")
def log(msg: str):
    line = f"[{_now()}] {msg}"
    print(line)
    try:
        (LOG_DIR / "latest.log").write_text(line + "\n", encoding="utf-8")
    except Exception:
        pass
    try:
        with open(LOG_DIR / f"run-{datetime.now().strftime('%Y%m%dT%H%M%S%z')}.log", "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass
def log_block(title: str): log(f"[{title.upper()}]")

# ==================== 상수/매핑 ====================
SUMMARY_SHEET_TITLE = "거래요약"
SUMMARY_COLS = [
    "전국","서울","강남구","압구정동","경기도","인천광역시","세종시","서초구","송파구","용산구",
    "강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구","강서구","강북구",
    "관악구","구로구","금천구","도봉구","동대문구","서대문구","성북구","은평구","중구","중랑구",
    "부산","대구","광주","대전","강원도","경남","경북","전남","전북","충남","충북","제주"
]
PROV_TO_SUMMARY_COL = {
    "서울특별시":"서울","세종특별자치시":"세종시","강원특별자치도":"강원도","경기도":"경기도",
    "인천광역시":"인천광역시","부산광역시":"부산","대구광역시":"대구","광주광역시":"광주",
    "대전광역시":"대전","울산광역시":"울산","경상남도":"경남","경상북도":"경북",
    "전라남도":"전남","전북특별자치도":"전북","충청남도":"충남","충청북도":"충북","제주특별자치도":"제주"
}
APGU_SHEET_TITLE = "압구정동"
APGU_KEY_FIELDS = ["계약년","계약월","계약일","광역","구","법정동","단지명","전용면적(㎡)","층","거래금액(만원)"]
NEEDED_COLS = ["광역","구","법정동","거래금액(만원)","계약년","계약월","계약일"]

# ==================== 유틸 ====================
def a1_col(n: int) -> str:
    s = ""
    while n>0:
        n, r = divmod(n-1, 26)
        s = chr(65+r)+s
    return s
def fmt_date_kor(dt: datetime) -> str: return f"{dt.year}. {dt.month}. {dt.day}"
def ym_label(y: int, m: int) -> str: return f"{str(y%100).zfill(2)}/{m}"
def norm_name(s: str) -> str: return re.sub(r"\s+","", str(s or ""))

def _retry(fn, *a, _tries=5, _delay=1.2, **kw):
    for i in range(_tries):
        try:
            return fn(*a, **kw)
        except APIError:
            if i==_tries-1: raise
            time.sleep(_delay)
        except Exception:
            if i==_tries-1: raise
            time.sleep(_delay)

def fuzzy_ws(sh, title: str):
    want = norm_name(title)
    for ws in sh.worksheets():
        if norm_name(ws.title)==want: return ws
    for ws in sh.worksheets():
        if norm_name(ws.title)==norm_name(title.replace(" ", "")): return ws
    return None

# ==================== 인증/엑셀 읽기 ====================
def open_spreadsheet(sheet_id: str):
    sa = os.environ.get("SA_JSON","").strip()
    if sa:
        creds = Credentials.from_service_account_info(
            json.loads(sa),
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        )
        gc = gspread.authorize(creds)
    else:
        gc = gspread.oauth()
    sh = gc.open_by_key(sheet_id)
    log("[gspread] spreadsheet opened")
    return sh

def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data", dtype=str).fillna("")
    for c in NEEDED_COLS:
        if c not in df.columns: df[c] = ""
    return df.copy()

# ==================== 집계 ====================
def eok_series(ser: pd.Series) -> pd.Series:
    if ser is None or ser.empty: return pd.Series([], dtype=float)
    s = pd.to_numeric(ser, errors="coerce").dropna()
    if s.empty: return pd.Series([], dtype=float)
    return s/10000.0  # 만원→억원
def round2(v) -> str:
    try:
        if v in ("", None): return ""
        return f"{float(v):.2f}"
    except Exception:
        return ""

def agg_all_stats(df: pd.DataFrame):
    counts = {c:0 for c in SUMMARY_COLS}
    med = {c:"" for c in SUMMARY_COLS}
    mean= {c:"" for c in SUMMARY_COLS}

    all_eok = eok_series(df["거래금액(만원)"])
    counts["전국"] = int(len(df))
    if not all_eok.empty:
        med["전국"]  = round2(all_eok.median())
        mean["전국"] = round2(all_eok.mean())

    if "광역" in df.columns:
        for prov, sub in df.groupby("광역"):
            label = PROV_TO_SUMMARY_COL.get(prov, prov)
            if label in counts:
                counts[label] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[label]  = round2(s.median())
                    mean[label] = round2(s.mean())

    seoul = df[df.get("광역","")=="서울특별시"].copy()
    if len(seoul):
        counts["서울"] = int(len(seoul))
        s = eok_series(seoul["거래금액(만원)"])
        if not s.empty:
            med["서울"]  = round2(s.median())
            mean["서울"] = round2(s.mean())
        if "구" in seoul.columns:
            for gu, sub in seoul.groupby("구"):
                if gu in counts:
                    counts[gu] += int(len(sub))
                    s = eok_series(sub["거래금액(만원)"])
                    if not s.empty:
                        med[gu]  = round2(s.median())
                        mean[gu] = round2(s.mean())

    ap = seoul[seoul.get("법정동","")=="압구정동"]
    counts["압구정동"] = int(len(ap))
    s = eok_series(ap["거래금액(만원)"])
    if not s.empty:
        med["압구정동"]  = round2(s.median())
        mean["압구정동"] = round2(s.mean())

    return counts, med, mean

# ==================== 월 탭 기록(총합계 포함) ====================
def ensure_row_for_date(ws, date_label: str) -> Tuple[int,bool]:
    vals = _retry(ws.get_all_values) or []
    if not vals:
        _retry(ws.update, [["날짜"]], "A1")
        vals = [["날짜"]]
    for i in range(1, len(vals)):
        v = (vals[i][0] if len(vals[i])>0 else "").strip()
        if v == date_label:
            return i+1, True
    new_idx = len(vals)+1
    _retry(ws.update, [[date_label]], f"A{new_idx}")
    return new_idx, False

def write_counts_row(ws, row_idx: int, header: List[str], counts_map: Dict[str,int]):
    col_count = max(ws.col_count, len(header))
    if ws.col_count < col_count:
        _retry(ws.add_cols, col_count - ws.col_count)

    hmap = {h:i for i,h in enumerate(header)}
    row_vals = [""]*col_count
    total = 0
    for k, v in counts_map.items():
        if k in hmap:
            j = hmap[k]
            row_vals[j] = int(v)
            if k != "총합계":
                total += int(v)
    if "총합계" in hmap:
        row_vals[hmap["총합계"]] = total

    rng = f"A{row_idx}:{a1_col(col_count)}{row_idx}"
    _retry(ws.update, [row_vals], rng, value_input_option="USER_ENTERED")

    req = { "requests": [{
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": row_idx-1, "endRowIndex": row_idx,
                      "startColumnIndex": 0, "endColumnIndex": col_count},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.textFormat.bold"
        }
    }]}
    _retry(ws.spreadsheet.batch_update, req)

# ==================== 거래요약 ====================
def ensure_summary_header(ws):
    vals = _retry(ws.get_all_values) or []
    if not vals:
        header = ["년월","구분"] + SUMMARY_COLS
        _retry(ws.update, [header], "A1")
    else:
        header = vals[0]
        for c in ["년월","구분"]+SUMMARY_COLS:
            if c not in header: header.append(c)
        if header != vals[0]:
            _retry(ws.update, [header], "A1")
    return header, {h:i for i,h in enumerate(header)}

def find_summary_row(ws, ym: str, label: str) -> int:
    vals = _retry(ws.get_all_values) or []
    if not vals:
        _retry(ws.update, [["년월","구분"]], "A1")
        vals = [["년월","구분"]]
    for i in range(1, len(vals)):
        r = vals[i]
        if (r[0] if len(r)>0 else "")==ym and (r[1] if len(r)>1 else "")==label:
            return i+1
    new_idx = len(vals)+1
    _retry(ws.update, [[ym,label]], f"A{new_idx}:B{new_idx}")
    return new_idx

def batch_values_update(ws, payload):
    body = {"valueInputOption":"USER_ENTERED",
            "data":[{"range":p["range"],"values":p["values"]} for p in payload]}
    _retry(ws.spreadsheet.values_batch_update, ws.spreadsheet.id, body)

def colorize_cell(ws, row_idx: int, col0: int, rgb):
    req = {"requests":[{
        "repeatCell":{
            "range":{"sheetId":ws.id,"startRowIndex":row_idx-1,"endRowIndex":row_idx,
                     "startColumnIndex":col0,"endColumnIndex":col0+1},
            "cell":{"userEnteredFormat":{"textFormat":{"foregroundColor":{
                  "red":rgb[0],"green":rgb[1],"blue":rgb[2]}}}},
            "fields":"userEnteredFormat.textFormat.foregroundColor"
        }
    }]}
    _retry(ws.spreadsheet.batch_update, req)

def write_month_summary(ws, year, month, counts, med, mean, prev_counts):
    ym = ym_label(year, month)
    header, hmap = ensure_summary_header(ws)

    # 거래건수(볼드)
    r1 = find_summary_row(ws, ym, "거래건수")
    payload = []
    for col in SUMMARY_COLS:
        c = hmap[col]
        payload.append({"range": f"{a1_col(c+1)}{r1}", "values": [[counts.get(col,0)]]})
    batch_values_update(ws, payload)
    req = {"requests":[{
        "repeatCell":{
            "range":{"sheetId":ws.id,"startRowIndex":r1-1,"endRowIndex":r1,
                     "startColumnIndex":hmap[SUMMARY_COLS[0]],"endColumnIndex":hmap[SUMMARY_COLS[-1]]+1},
            "cell":{"userEnteredFormat":{"textFormat":{"bold":True}}},
            "fields":"userEnteredFormat.textFormat.bold"
        }
    }]}
    _retry(ws.spreadsheet.batch_update, req)
    log(f"[summary] {ym} 거래건수 -> row={r1}")

    # 중앙값(억, 소수점2자리 문자열)
    r2 = find_summary_row(ws, ym, "중앙값(단위:억)")
    payload = []
    for col in SUMMARY_COLS:
        payload.append({"range": f"{a1_col(hmap[col]+1)}{r2}", "values": [[med.get(col,"")]]})
    batch_values_update(ws, payload)
    log(f"[summary] {ym} 중앙값 -> row={r2}")

    # 평균가(억, 소수점2자리 문자열)
    r3 = find_summary_row(ws, ym, "평균가(단위:억)")
    payload = []
    for col in SUMMARY_COLS:
        payload.append({"range": f"{a1_col(hmap[col]+1)}{r3}", "values": [[mean.get(col,"")]]})
    batch_values_update(ws, payload)
    log(f"[summary] {ym} 평균가 -> row={r3}")

    # 전월대비 건수증감(+파랑 / -빨강)
    r4 = find_summary_row(ws, ym, "전월대비 건수증감")
    payload, colors = [], []
    if prev_counts:
        for col in SUMMARY_COLS:
            diff = counts.get(col,0) - prev_counts.get(col,0)
            if diff>0:
                s = f"+{diff}"; rgb=(0.0,0.3,1.0)
            elif diff<0:
                s = f"-{abs(diff)}"; rgb=(1.0,0.0,0.0)
            else:
                s = "0"; rgb=None
            c = hmap[col]
            payload.append({"range": f"{a1_col(c+1)}{r4}", "values": [[s]]})
            if rgb: colors.append((r4, c, rgb))
    else:
        for col in SUMMARY_COLS:
            c = hmap[col]
            payload.append({"range": f"{a1_col(c+1)}{r4}", "values": [[""]]})
    batch_values_update(ws, payload)
    for r,c,rgb in colors: colorize_cell(ws, r, c, rgb)
    log(f"[summary] {ym} 전월대비 -> row={r4}")

    # 예상건수(공란)
    r5 = find_summary_row(ws, ym, "예상건수")
    payload = [{"range": f"{a1_col(hmap[col]+1)}{r5}", "values":[[""]]} for col in SUMMARY_COLS]
    batch_values_update(ws, payload)
    log(f"[summary] {ym} 예상건수 -> row={r5}")

# ==================== 압구정동: 원본 누적 + 변동로그 ====================
def ensure_apgu_sheet(sh):
    ws = fuzzy_ws(sh, APGU_SHEET_TITLE)
    return ws if ws else _retry(sh.add_worksheet, title=APGU_SHEET_TITLE, rows=2000, cols=40)

def ensure_apgu_header(ws) -> List[str]:
    vals = _retry(ws.get_all_values) or []
    header = vals[0] if vals else []
    must = set(APGU_KEY_FIELDS + ["도로명","번지","본번","부번","동"])
    union = list(dict.fromkeys(header + list(must)))
    if "기록일" not in union: union.append("기록일")
    if union != header: _retry(ws.update, [union], "A1")
    return union

def make_apgu_key_from_dict(d: dict) -> tuple:
    return tuple(str(d.get(k,"")).strip() for k in APGU_KEY_FIELDS)
def make_apgu_key_from_row(row: List[str], header: List[str]) -> tuple:
    idx = {h:i for i,h in enumerate(header)}
    def get(h):
        i = idx.get(h,-1)
        return row[i].strip() if (i>=0 and i<len(row)) else ""
    return tuple(get(k) for k in APGU_KEY_FIELDS)

def append_apgu_change_log(ws, added: set[tuple], removed: set[tuple]):
    if not added and not removed: return
    used = (_retry(ws.get_all_values) or [])
    base = len(used) if used else 0
    now = datetime.now().strftime("%Y.%m.%d %H:%M")
    _retry(ws.append_rows, [[f"{now} 변동 로그"]], value_input_option="USER_ENTERED")
    lines = []
    def line(t, tag):
        y,mo,d,_,_,_,apt,area,fl,price = t
        return [(f"({tag}) {y}/{mo}/{d} {apt} {area}㎡ {fl}층 {price}만원")]
    for t in sorted(added):   lines.append(line(t,"신규"))
    for t in sorted(removed): lines.append(line(t,"삭제"))
    if lines:
        _retry(ws.append_rows, [x[0] for x in lines], value_input_option="USER_ENTERED")
        start = base+2
        end = base+1+len(lines)+1
        req = {"requests":[{
            "repeatCell":{
                "range":{"sheetId":ws.id,"startRowIndex":start-1,"endRowIndex":end,
                         "startColumnIndex":0,"endColumnIndex":1},
                "cell":{"userEnteredFormat":{"textFormat":{"foregroundColor":{"red":1.0,"green":0.0,"blue":0.0}}}},
                "fields":"userEnteredFormat.textFormat.foregroundColor"
            }
        }]}
        _retry(ws.spreadsheet.batch_update, req)

def upsert_apgu_raw(ws, df_all: pd.DataFrame, record_date: str):
    cond = (df_all.get("광역","")=="서울특별시") & (df_all.get("법정동","")=="압구정동")
    df = df_all[cond].copy()
    log(f"[압구정동] filtered rows in file(s): {len(df)}")
    if df.empty: return

    for c in ["계약년","계약월","계약일"]:
        if c not in df.columns: df[c]=""
    df = df.sort_values(
        ["계약년","계약월","계약일","단지명","전용면적(㎡)","층","거래금액(만원)"],
        ascending=[True,True,True,True,True,True,True],
        kind="mergesort"
    )

    header = ensure_apgu_header(ws)
    vals = _retry(ws.get_all_values) or []
    exist_rows = vals[1:] if vals else []
    old_keys = set(make_apgu_key_from_row(r, header) for r in exist_rows)

    def nb(v):
        if v is None: return ""
        if isinstance(v,float) and pd.isna(v): return ""
        return v

    new_rows = []
    new_keys = set()
    for _, sr in df.iterrows():
        d = {col: sr.get(col,"") for col in header if col in df.columns}
        d["기록일"] = record_date
        k = make_apgu_key_from_dict(d)
        if k in new_keys: continue
        row = [nb(d.get(col,"")) for col in header]
        new_rows.append(row); new_keys.add(k)

    added   = new_keys - old_keys
    removed = old_keys - new_keys

    # 본문 교체(헤더 제외 전부 지우고 재작성)
    need = 1 + max(1, len(new_rows))
    if ws.row_count < need: _retry(ws.add_rows, need - ws.row_count)
    last = ws.row_count
    if last >= 2: _retry(ws.batch_clear, [f"2:{last}"])
    if new_rows: _retry(ws.update, new_rows, "A2", value_input_option="USER_ENTERED")

    append_apgu_change_log(ws, added, removed)
    log(f"[압구정동] updated rows={len(new_rows)}, +{len(added)} / -{len(removed)}")

# ==================== 메인 ====================
def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--artifacts-dir", default=os.environ.get("ARTIFACTS_DIR","artifacts"))
    p.add_argument("--sheet-id", default=os.environ.get("SHEET_ID","").strip(), required=False)
    args = p.parse_args()

    log_block("MAIN")
    if not args.sheet_id:
        print("SHEET_ID 가 비어있습니다.", file=sys.stderr); sys.exit(1)

    sh = open_spreadsheet(args.sheet_id)
    root = Path(args.artifacts_dir)
    if not root.exists():
        print(f"artifacts dir not found: {root}", file=sys.stderr); sys.exit(1)

    files = sorted(p for p in root.rglob("*.xlsx") if "서울시" not in p.name)
    log(f"[collect] found {len(files)} xlsx files")

    today_label = fmt_date_kor(datetime.now())
    apgu_df_list = []
    month_cache: Dict[Tuple[int,int], Dict[str,Dict]] = {}

    # 월 탭 반영
    for path in files:
        m = re.search(r"전국\s*(\d{2})(\d{2})_", path.name)
        if not m: continue
        yy, mm = int(m.group(1)), int(m.group(2))
        year, month = 2000+yy, mm
        nat_title  = f"전국 {yy}년 {month}월"
        se_title   = f"서울 {yy}년 {month}월"

        log(f"[file] {path.name} -> nat='{nat_title}' seoul='{se_title}'")
        df = read_month_df(path)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        counts, med, mean = agg_all_stats(df)

        # 전국 탭
        ws = fuzzy_ws(sh, nat_title)
        if ws:
            vals = _retry(ws.get_all_values) or []
            header = vals[0] if vals else ["날짜"]+SUMMARY_COLS+["총합계"]
            if "총합계" not in header:
                header = header+["총합계"]; _retry(ws.update, [header], "A1")
            row, existed = ensure_row_for_date(ws, today_label)
            write_counts_row(ws, row, header, counts)
            log(f"[전국] {nat_title} -> {today_label} {'update' if existed else 'append'} row={row}")
        else:
            log(f"[전국] {nat_title} -> sheet not found (skip)")

        # 서울 탭
        ws = fuzzy_ws(sh, se_title)
        if ws:
            vals = _retry(ws.get_all_values) or []
            header = vals[0] if vals else ["날짜"]+SUMMARY_COLS+["총합계"]
            if "총합계" not in header:
                header = header+["총합계"]; _retry(ws.update, [header], "A1")
            row, existed = ensure_row_for_date(ws, today_label)
            write_counts_row(ws, row, header, counts)
            log(f"[서울] {se_title} -> {today_label} {'update' if existed else 'append'} row={row}")
        else:
            log(f"[서울] {se_title} -> sheet not found (skip)")

        month_cache[(year, month)] = {"counts":counts,"med":med,"mean":mean}

        ap = df[(df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")]
        if len(ap): apgu_df_list.append(ap)

    # 거래요약
    if month_cache:
        ws_sum = fuzzy_ws(sh, SUMMARY_SHEET_TITLE) or _retry(sh.add_worksheet, title=SUMMARY_SHEET_TITLE, rows=2000, cols=100)
        for (y,m) in sorted(month_cache.keys()):
            cur = month_cache[(y,m)]
            # 전월 검색
            py, pm = (y, m-1) if m>1 else (y-1, 12)
            prev_counts = month_cache.get((py,pm), {}).get("counts")
            write_month_summary(ws_sum, y, m, cur["counts"], cur["med"], cur["mean"], prev_counts)

    # 압구정동
    if apgu_df_list:
        ws_ap = ensure_apgu_sheet(sh)
        all_ap = pd.concat(apgu_df_list, ignore_index=True)
        upsert_apgu_raw(ws_ap, all_ap, record_date=today_label)

    (LOG_DIR/"where_written.txt").write_text("완료\n", encoding="utf-8")
    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    main()
