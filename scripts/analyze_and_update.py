# -*- coding: utf-8 -*-
from __future__ import annotations
import os, re, sys, json, time, random
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

# ===================== 기본 설정 =====================
LOG_DIR = Path("analyze_report")
WORK_DIR_DEFAULT = "artifacts"
SHEET_NAME_DATA = "data"

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

SEOUL_GU = [
    "강남구","강동구","강북구","강서구","관악구","광진구","구로구","금천구","노원구","도봉구",
    "동대문구","동작구","마포구","서대문구","서초구","성동구","성북구","송파구","양천구",
    "영등포구","용산구","은평구","종로구","중구","중랑구"
]

NEEDED_COLS = [
    "광역","구","법정동","계약년","계약월","계약일","거래금액(만원)","단지명","전용면적(㎡)","동","층"
]

APGU_SHEET = "압구정동"  # 원본 누적 탭 명

# ===================== 유틸 & 로깅 =====================
def ensure_logdir():
    if LOG_DIR.exists():
        if LOG_DIR.is_file():
            LOG_DIR.unlink()
            LOG_DIR.mkdir(parents=True, exist_ok=True)
    else:
        LOG_DIR.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    ensure_logdir()
    ts = datetime.now(ZoneInfo("Asia/Seoul")).strftime("[%H:%M:%S]")
    line = f"{ts} {msg}"
    print(line)
    with open(LOG_DIR/"latest.log","a",encoding="utf-8") as f:
        f.write(line+"\n")

def log_block(title: str):
    log(f"[{title.upper()}]")

def norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).strip()

def fmt_date_kor(d: date) -> str:
    # 구글시트 표시와 일치 (예: 2025. 9. 27)
    return f"{d.year}. {d.month}. {d.day}"

def ym_from_filename(fn: str):
    # '전국 2410_250926.xlsx' → ('전국 24년 10월','서울 24년 10월','24/10')
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
        return f"{(y-2000)-1}/12"
    return f"{yy}/{m-1}"

# ===================== Google Sheets =====================
from gspread.exceptions import APIError

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

def a1_col(idx: int) -> str:
    s = ""
    n = idx
    while n>0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def open_sheet(sheet_id: str, sa_path: str|None):
    log("[gspread] auth")
    sa_raw = os.environ.get("SA_JSON","").strip()
    if sa_path and Path(sa_path).exists():
        creds = Credentials.from_service_account_file(
            sa_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        )
    elif sa_raw:
        creds = Credentials.from_service_account_info(
            json.loads(sa_raw),
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        )
    else:
        raise RuntimeError("service account not provided (sa.json or SA_JSON)")

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    log("[gspread] spreadsheet opened")
    return sh

def fuzzy_ws(sh, title: str):
    tgt = norm(title)
    for ws in sh.worksheets():
        if norm(ws.title) == tgt:
            return ws
    return None

def batch_values_update(ws, payload):
    body = {"valueInputOption":"USER_ENTERED","data":payload}
    return _retry(ws.spreadsheet.values_batch_update, body=body)

# ===================== 파일 읽기 & 표준화 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=SHEET_NAME_DATA, dtype=str)
    df = df.fillna("")
    # 숫자형 변환
    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # 누락 컬럼 보정
    for c in NEEDED_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    return df[NEEDED_COLS].copy()

# ===================== 집계(건수·중앙값·평균) =====================
def eok_series(ser):
    s = pd.to_numeric(ser, errors="coerce").dropna()
    if s.empty: return pd.Series([], dtype=float)
    return s / 10000.0

def agg_all_stats(df: pd.DataFrame):
    counts = {col:0 for col in SUMMARY_COLS}
    med = {col:"" for col in SUMMARY_COLS}
    mean = {col:"" for col in SUMMARY_COLS}
    if df.empty:
        return counts, med, mean

    # 전국
    all_eok = eok_series(df["거래금액(만원)"])
    counts["전국"] = int(len(df))
    if not all_eok.empty:
        med["전국"] = f"{float(all_eok.median()):.2f}"
        mean["전국"] = f"{float(all_eok.mean()):.2f}"

    # 광역
    for prov, sub in df.groupby("광역"):
        prov_std = PROV_MAP.get(str(prov), str(prov))
        if prov_std in counts:
            counts[prov_std] += int(len(sub))
            s = eok_series(sub["거래금액(만원)"])
            if not s.empty:
                med[prov_std] = f"{float(s.median()):.2f}"
                mean[prov_std] = f"{float(s.mean()):.2f}"

    # 서울/구
    seoul = df[df["광역"]=="서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul)>0:
        s = eok_series(seoul["거래금액(만원)"])
        if not s.empty:
            med["서울"] = f"{float(s.median()):.2f}"
            mean["서울"] = f"{float(s.mean()):.2f}"

    for gu, sub in seoul.groupby("구"):
        gu = str(gu)
        if gu in counts:
            counts[gu] += int(len(sub))
            s = eok_series(sub["거래금액(만원)"])
            if not s.empty:
                med[gu] = f"{float(s.median()):.2f}"
                mean[gu] = f"{float(s.mean()):.2f}"

    # 압구정동
    ap = seoul[seoul["법정동"]=="압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap)>0:
        s = eok_series(ap["거래금액(만원)"])
        med["압구정동"] = f"{float(s.median()):.2f}"
        mean["압구정동"] = f"{float(s.mean()):.2f}"

    return counts, med, mean

# ===================== 탭 쓰기 (월별 전국/서울: 기존 유지) =====================
def find_or_append_date_row(ws, date_label: str) -> int:
    vals = _retry(ws.get_all_values)
    if not vals:
        return 2
    for i, row in enumerate(vals[1:], start=2):
        if (len(row)>0) and str(row[0]).strip()==date_label:
            return i
    return len(vals)+1

def write_month_sheet(ws, date_label: str, header: list[str], values_by_colname: dict[str,int]):
    hmap = {str(h).strip(): idx+1 for idx,h in enumerate(header) if str(h).strip()}
    row_idx = find_or_append_date_row(ws, date_label)
    payload = [{"range": f"A{row_idx}", "values": [[date_label]]}]
    total = 0
    for col_name, val in values_by_colname.items():
        if col_name not in hmap:
            continue
        c = hmap[col_name]
        payload.append({"range": f"{a1_col(c)}{row_idx}", "values": [[val]]})
        if col_name != "날짜" and col_name != "총합계":
            total += int(val or 0)
    # 총합계 열 보정
    if "총합계" in hmap and "총합계" not in values_by_colname:
        payload.append({"range": f"{a1_col(hmap['총합계'])}{row_idx}", "values": [[total]]})
    if payload:
        batch_values_update(ws, payload)
        log(f"[ws] {ws.title} -> {date_label} row={row_idx}")

# ===================== 거래요약 쓰기 =====================
def ensure_summary_header(ws):
    vals = _retry(ws.get_all_values) or []
    if not vals:
        _retry(ws.update, [["년월","구분"] + SUMMARY_COLS], "A1")

def find_summary_row(ws, ym: str, label: str) -> int:
    vals = _retry(ws.get_all_values) or []
    if not vals:
        ensure_summary_header(ws)
        vals = _retry(ws.get_all_values) or []
    for i, row in enumerate(vals[1:], start=2):
        a = str(row[0]).strip() if len(row)>0 else ""
        b = str(row[1]).strip() if len(row)>1 else ""
        if a==ym and b==label:
            return i
    return len(vals)+1

def get_header_map(ws) -> Dict[str,int]:
    header = _retry(ws.row_values, 1)
    return {str(h).strip(): i+1 for i,h in enumerate(header) if str(h).strip()}

def put_summary_line(ws, row_idx: int, line_map: dict):
    hmap = get_header_map(ws)
    payload = []
    for col in SUMMARY_COLS:
        if col not in hmap:
            continue
        payload.append({"range": f"{a1_col(hmap[col])}{row_idx}",
                        "values": [[line_map.get(col, "")]]})
    if payload:
        batch_values_update(ws, payload)

def color_diff_row(ws, row_idx: int, diff_line: dict):
    hmap = get_header_map(ws)
    reqs = []
    for col, v in diff_line.items():
        if col not in hmap: continue
        if v.startswith("+"):
            color = {"red":0.0, "green":0.35, "blue":1.0}  # 파랑
        elif v.startswith("-"):
            color = {"red":1.0, "green":0.0, "blue":0.0}   # 빨강
        else:
            continue
        c = hmap[col]-1
        reqs.append({
            "repeatCell":{
                "range":{
                    "sheetId": ws.id,
                    "startRowIndex": row_idx-1,
                    "endRowIndex": row_idx,
                    "startColumnIndex": c,
                    "endColumnIndex": c+1
                },
                "cell":{"userEnteredFormat":{"textFormat":{"foregroundColor":color}}},
                "fields":"userEnteredFormat.textFormat.foregroundColor"
            }
        })
    if reqs:
        _retry(ws.spreadsheet.batch_update, {"requests": reqs})

def write_summary_for_month(ws, ym: str,
                            counts: dict, med: dict, mean: dict,
                            prev_counts: dict|None):
    ensure_summary_header(ws)
    # 1) 거래건수
    r1 = find_summary_row(ws, ym, "거래건수")
    _retry(ws.update, [[ym]], f"A{r1}:A{r1}")
    _retry(ws.update, [["거래건수"]], f"B{r1}:B{r1}")
    put_summary_line(ws, r1, counts)
    log(f"[summary] {ym} 거래건수 -> row={r1}")

    # 2) 중앙값(단위:억)
    r2 = find_summary_row(ws, ym, "중앙값(단위:억)")
    _retry(ws.update, [[ym]], f"A{r2}:A{r2}")
    _retry(ws.update, [["중앙값(단위:억)"]], f"B{r2}:B{r2}")
    put_summary_line(ws, r2, med)
    log(f"[summary] {ym} 중앙값 -> row={r2}")

    # 3) 평균가(단위:억)
    r3 = find_summary_row(ws, ym, "평균가(단위:억)")
    _retry(ws.update, [[ym]], f"A{r3}:A{r3}")
    _retry(ws.update, [["평균가(단위:억)"]], f"B{r3}:B{r3}")
    put_summary_line(ws, r3, mean)
    log(f"[summary] {ym} 평균가 -> row={r3}")

    # 4) 전월대비 건수증감
    r4 = find_summary_row(ws, ym, "전월대비 건수증감")
    _retry(ws.update, [[ym]], f"A{r4}:A{r4}")
    _retry(ws.update, [["전월대비 건수증감"]], f"B{r4}:B{r4}")
    diffs = {col:"0" for col in SUMMARY_COLS}
    if prev_counts:
        for col in SUMMARY_COLS:
            cur = int(counts.get(col, 0) or 0)
            prv = int(prev_counts.get(col, 0) or 0)
            d = cur - prv
            diffs[col] = f"+{d}" if d>0 else (str(d) if d<0 else "0")
    put_summary_line(ws, r4, diffs)
    color_diff_row(ws, r4, diffs)
    log(f"[summary] {ym} 전월대비 -> row={r4}")

    # 5) 예상건수(비움)
    r5 = find_summary_row(ws, ym, "예상건수")
    _retry(ws.update, [[ym]], f"A{r5}:A{r5}")
    _retry(ws.update, [["예상건수"]], f"B{r5}:B{r5}")
    blanks = {col:"" for col in SUMMARY_COLS}
    put_summary_line(ws, r5, blanks)
    log(f"[summary] {ym} 예상건수 -> row={r5}")

# ===================== 압구정동(원본) 탭 =====================
def ensure_apgu_sheet(sh) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, APGU_SHEET)
    if ws: return ws
    return _retry(sh.add_worksheet, title=APGU_SHEET, rows=4000, cols=80)

def apgu_key(d: dict) -> str:
    # (계약년, 계약월, 계약일, 단지명, 전용면적(㎡), 동, 층, 거래금액(만원))
    parts = [
        d.get("계약년",""), d.get("계약월",""), d.get("계약일",""),
        d.get("단지명",""), d.get("전용면적(㎡)",""),
        d.get("동",""), d.get("층",""), d.get("거래금액(만원)",""),
    ]
    return "|".join(str(x).strip() for x in parts)

def upsert_apgu_month(ws: gspread.Worksheet, df: pd.DataFrame, y: int, m: int):
    # 해당 월(서울특별시 & 압구정동) 필터
    dd = df[(df.get("광역","")== "서울특별시") & (df.get("법정동","")== "압구정동")].copy()
    if "계약년" in dd and "계약월" in dd:
        dd = dd[(dd["계약년"]==y) & (dd["계약월"]==m)]
    if dd.empty:
        log(f"[압구정동] {y%100}/{m} no rows")
        return

    # 시트 헤더 준비
    vals = _retry(ws.get_all_values) or []
    if not vals:
        header = list(NEEDED_COLS) + ["기록일","변경구분","변동일"]
        _retry(ws.update, [header], "A1")
        vals = [header]
    else:
        header = vals[0]
        # 필요한 컬럼 추가
        for need in ["기록일","변경구분","변동일"]:
            if need not in header:
                header.append(need)
        if header != vals[0]:
            _retry(ws.update, [header], "A1")
            vals = _retry(ws.get_all_values) or [header]

    # 기존 키셋
    idx = {h:i for i,h in enumerate(header)}
    def row_to_dict(row):
        return {k:(row[i] if i<len(row) else "") for k,i in idx.items()}
    existing_rows = vals[1:]
    existing_keys = set()
    for r in existing_rows:
        d = row_to_dict(r)
        existing_keys.add(apgu_key(d))

    # 신규 셋(이번 파일)
    today_label = fmt_date_kor(datetime.now(ZoneInfo("Asia/Seoul")).date())
    new_rows, new_keys = [], set()
    for _, r in dd.iterrows():
        d = {k: r.get(k, "") for k in header if k in dd.columns}
        k = apgu_key(d)
        new_keys.add(k)
        if k in existing_keys:
            continue
        rec = [d.get(h, "") for h in header]
        # 기록일/변경구분/변동일 채우기
        if "기록일" in idx: rec[idx["기록일"]] = today_label
        if "변경구분" in idx: rec[idx["변경구분"]] = "(신규)"
        if "변동일" in idx: rec[idx["변동일"]] = today_label
        new_rows.append(rec)

    # 삭제 감지: 기존 - 신규
    removed_rows = []
    for r in existing_rows:
        d = row_to_dict(r)
        k = apgu_key(d)
        if k not in new_keys:
            # 삭제된 케이스 → 로그용으로 복사본 만들어 맨 아래 붙임
            cp = [d.get(h,"") for h in header]
            if "변경구분" in idx: cp[idx["변경구분"]] = "(삭제)"
            if "변동일" in idx:   cp[idx["변동일"]] = today_label
            removed_rows.append(cp)

    # 실제 추가/삭제 로그 쓰기 (맨 아래)
    start = len(vals) + 1
    rows_to_write = new_rows + removed_rows
    if rows_to_write:
        end = start + len(rows_to_write) - 1
        rng = f"A{start}:{a1_col(len(header))}{end}"
        _retry(ws.update, rows_to_write, rng)
        # 빨간 글자 포맷
        _retry(ws.spreadsheet.batch_update, {
            "requests":[{
                "repeatCell":{
                    "range":{
                        "sheetId": ws.id,
                        "startRowIndex": start-1,
                        "endRowIndex": end,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(header)
                    },
                    "cell":{"userEnteredFormat":{"textFormat":{"foregroundColor":{"red":1.0,"green":0.0,"blue":0.0}}}},
                    "fields":"userEnteredFormat.textFormat.foregroundColor"
                }
            }]
        })
    log(f"[압구정동] {y%100}/{m} new={len(new_rows)} removed={len(removed_rows)}")

# ===================== 메인 =====================
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts-dir", default=WORK_DIR_DEFAULT)
    parser.add_argument("--sheet-id", default=os.environ.get("SHEET_ID",""))
    parser.add_argument("--sa", default="sa.json")
    args = parser.parse_args()

    # 로그 초기화
    ensure_logdir()
    (LOG_DIR/"latest.log").write_text("", encoding="utf-8")
    log_block("main")
    log(f"artifacts_dir={args.artifacts_dir}")

    sh = open_sheet(args.sheet_id, args.sa)
    ws_summary = fuzzy_ws(sh, "거래요약")  # 없으면 skip하지 말고 생성
    if not ws_summary:
        ws_summary = _retry(sh.add_worksheet, title="거래요약", rows=2000, cols=60)
    ensure_summary_header(ws_summary)

    # 1) 파일 스캔
    work = Path(args.artifacts_dir)
    files = sorted([p for p in work.rglob("전국 *.xlsx") if p.is_file()])
    log(f"national files: {len(files)}")

    # 오늘 날짜(KST)
    today = datetime.now(ZoneInfo("Asia/Seoul")).date()
    today_label = fmt_date_kor(today)

    # 월별 캐시(전월대비를 위해 이번 실행 내에서 참고)
    month_cache: Dict[str, Dict] = {}

    for path in files:
        nat_title, se_title, ym = ym_from_filename(path.name)
        if not ym:
            continue
        log(f"[file] {path.name} -> {nat_title} / {se_title} / ym={ym}")

        # 엑셀 로드
        try:
            df = read_month_df(path)
        except Exception as e:
            log(f"[read ERROR] {e}")
            continue
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        # 집계
        counts, med_map, mean_map = agg_all_stats(df)
        month_cache[ym] = {
            "counts": {k:int(counts.get(k,0) or 0) for k in SUMMARY_COLS},
            "med": {k:med_map.get(k,"") for k in SUMMARY_COLS},
            "mean": {k:mean_map.get(k,"") for k in SUMMARY_COLS},
        }

        # ---- 월별 탭(전국/서울) 기록: 기존 성공 로직 유지 ----
        ws_nat = fuzzy_ws(sh, nat_title)
        ws_se  = fuzzy_ws(sh, se_title)
        if ws_nat:
            header_nat = _retry(ws_nat.row_values, 1)
            values_nat = {h:int(counts.get(h,0) or 0) for h in header_nat if h and h!="날짜"}
            if "총합계" in header_nat:
                values_nat["총합계"] = int(counts.get("전국",0) or 0)
            write_month_sheet(ws_nat, today_label, header_nat, values_nat)
        else:
            log(f"[전국] sheet not found: '{nat_title}' (skip)")

        if ws_se:
            header_se = _retry(ws_se.row_values, 1)
            values_se = {}
            total_se = 0
            for h in header_se:
                if not h or h=="날짜": continue
                if h=="총합계":
                    continue
                v = int(counts.get(h,0) or 0)
                values_se[h] = v
                total_se += v
            if "총합계" in header_se:
                values_se["총합계"] = total_se
            write_month_sheet(ws_se, today_label, header_se, values_se)
        else:
            log(f"[서울] sheet not found: '{se_title}' (skip)")

        # ---- 거래요약 기록 (이 파일의 ym만 처리) ----
        prev = month_cache.get(prev_ym(ym))  # 같은 실행 내 이전 월이 있으면 활용
        prev_counts = prev["counts"] if prev else None

        # 없으면 시트에서 ‘전월’의 거래건수 라인을 읽어 비교 (fallback)
        if prev_counts is None:
            py = prev_ym(ym)
            # 요약 시트에서 py / '거래건수' 라인 찾기
            try:
                r_prev = find_summary_row(ws_summary, py, "거래건수")
                if r_prev:
                    hmap = get_header_map(ws_summary)
                    vals = _retry(ws_summary.row_values, r_prev)
                    prev_counts = {}
                    for col in SUMMARY_COLS:
                        i = hmap.get(col)
                        if i and i <= len(vals):
                            prev_counts[col] = int((vals[i-1] or "0").replace(",","") or 0)
            except Exception:
                prev_counts = None

        write_summary_for_month(
            ws_summary,
            ym,
            month_cache[ym]["counts"],
            month_cache[ym]["med"],
            month_cache[ym]["mean"],
            prev_counts
        )

        # ---- 압구정동 원본(해당 월) 누적 + 변경 로그 ----
        ws_ap = ensure_apgu_sheet(sh)
        # 계약년/월은 숫자형으로 들어있음 → 정수 변환 후 비교
        try:
            # read_month_df 이미 숫자로 변환함. NaN 방지
            tdf = df.copy()
            for c in ["계약년","계약월"]:
                tdf[c] = pd.to_numeric(tdf[c], errors="coerce").fillna(0).astype(int)
            year = 2000 + int(ym.split("/")[0])
            month = int(ym.split("/")[1])
            upsert_apgu_month(ws_ap, tdf, year, month)
        except Exception as e:
            log(f"[압구정동 ERROR] {e}")

    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        try:
            ensure_logdir()
            with open(LOG_DIR/"latest.log","a",encoding="utf-8") as f:
                f.write(f"[ERROR] {repr(e)}\n")
        except Exception:
            pass
        print(f"[ERROR] {repr(e)}", file=sys.stderr)
        raise
