# -*- coding: utf-8 -*-
import os, sys, re, json, time, random
from pathlib import Path
from datetime import datetime, date
import pandas as pd

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

NEEDED_COLS = ["광역","구","법정동","계약년","계약월","계약일","거래금액(만원)"]

# ===================== 유틸 & 로깅 =====================
def ensure_logdir():
    if LOG_DIR.exists():
        if LOG_DIR.is_file():
            LOG_DIR.unlink()
            LOG_DIR.mkdir(parents=True, exist_ok=True)
        elif LOG_DIR.is_dir():
            return
        else:
            try:
                LOG_DIR.unlink()
            except Exception:
                pass
            LOG_DIR.mkdir(parents=True, exist_ok=True)
    else:
        LOG_DIR.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    ensure_logdir()
    ts = datetime.now().strftime("[%H:%M:%S]")
    line = f"{ts} {msg}"
    print(line)
    with open(LOG_DIR/"latest.log","a",encoding="utf-8") as f:
        f.write(line+"\n")

def log_block(title: str):
    log(f"[{title.upper()}]")

def norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).strip()

def fmt_date_kor(d: datetime) -> str:
    # 구글시트 표시와 일치 (예: 2025. 9. 27)
    return f"{d.year}. {d.month}. {d.day}"

def ym_from_filename(fn: str):
    # '전국 2410_250926.xlsx' → ('전국 24년 10월','서울 24년 10월','24/10')
    m = re.search(r"(\d{2})(\d{2})", fn)
    if not m:
        return None, None, None
    yy, mm = m.group(1), int(m.group(2))
    nat = f"전국 20{yy}년 {mm}월"
    se = f"서울 20{yy}년 {mm}월"
    ym = f"{yy}/{mm}"
    return nat, se, ym

def prev_ym(ym: str) -> str:
    # "24/10" → "24/09"
    yy, mm = ym.split("/")
    y = 2000 + int(yy)
    m = int(mm)
    if m == 1:
        return f"{(y-2000)-1}/12"
    return f"{yy}/{m-1}"

# ===================== Google Sheets =====================
import gspread
from google.oauth2.service_account import Credentials
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
    log("[gspread] auth with service account")
    if sa_path and Path(sa_path).exists():
        creds = Credentials.from_service_account_file(
            sa_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        )
    else:
        raw = os.environ.get("SA_JSON","").strip()
        creds = Credentials.from_service_account_info(
            json.loads(raw),
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    log("[gspread] spreadsheet opened")
    return sh

def fuzzy_ws(sh, title: str):
    tgt = norm(title)
    for ws in sh.worksheets():
        if norm(ws.title) == tgt:
            return ws
    return None  # 새로 만들지 않음 (기존 로직 존중)

def batch_values_update(ws, payload):
    body = {"valueInputOption":"USER_ENTERED","data":payload}
    return _retry(ws.spreadsheet.values_batch_update, body=body)

# ===================== 파일 읽기 & 표준화 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=SHEET_NAME_DATA, dtype=str)
    # 필요한 숫자형 변환
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
    """
    반환:
      counts_map: {SUMMARY_COL → 건수},  seoul_total, apg_cnt
      med_map   : {SUMMARY_COL → 중앙값(억) or ""}  (없으면 "")
      mean_map  : {SUMMARY_COL → 평균(억) or ""}    (없으면 "")
    """
    counts = {col:0 for col in SUMMARY_COLS}
    med = {col:"" for col in SUMMARY_COLS}
    mean = {col:"" for col in SUMMARY_COLS}

    if df.empty:
        return counts, 0, 0, med, mean

    # 전국
    all_eok = eok_series(df["거래금액(만원)"])
    counts["전국"] = int(len(df))
    if not all_eok.empty:
        med["전국"] = round(float(all_eok.median()), 2)
        mean["전국"] = round(float(all_eok.mean()), 2)

    # 광역 (→ PROV_MAP로 요약열 매핑)
    for prov, sub in df.groupby("광역"):
        prov_std = PROV_MAP.get(str(prov), str(prov))
        if prov_std in SUMMARY_COLS:
            counts[prov_std] += int(len(sub))
            s = eok_series(sub["거래금액(만원)"])
            if not s.empty:
                med[prov_std] = round(float(s.median()), 2)
                mean[prov_std] = round(float(s.mean()), 2)

    # 서울 합계 / 자치구
    seoul = df[df["광역"]=="서울특별시"].copy()
    seoul_total = int(len(seoul))
    counts["서울"] = seoul_total
    if seoul_total>0:
        s = eok_series(seoul["거래금액(만원)"])
        if not s.empty:
            med["서울"] = round(float(s.median()), 2)
            mean["서울"] = round(float(s.mean()), 2)

    for gu, sub in seoul.groupby("구"):
        gu = str(gu)
        if gu in SUMMARY_COLS:
            counts[gu] += int(len(sub))
            s = eok_series(sub["거래금액(만원)"])
            if not s.empty:
                med[gu] = round(float(s.median()), 2)
                mean[gu] = round(float(s.mean()), 2)

    # 압구정동
    ap = seoul[seoul["법정동"]=="압구정동"]
    apg_cnt = int(len(ap))
    counts["압구정동"] = apg_cnt
    if apg_cnt>0:
        s = eok_series(ap["거래금액(만원)"])
        med["압구정동"] = round(float(s.median()), 2)
        mean["압구정동"] = round(float(s.mean()), 2)

    return counts, seoul_total, apg_cnt, med, mean

# ===================== 탭 쓰기 (기존 규칙 유지) =====================
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
    for col_name, val in values_by_colname.items():
        if col_name not in hmap: 
            continue
        c = hmap[col_name]
        payload.append({"range": f"{a1_col(c)}{row_idx}", "values": [[val]]})
    if payload:
        batch_values_update(ws, payload)
        log(f"[ws] {ws.title} -> {date_label} row={row_idx}")

# ===================== 거래요약 쓰기 =====================
def find_summary_row(ws, ym: str, label: str) -> int:
    vals = _retry(ws.get_all_values)
    if not vals:
        return 2
    for i, row in enumerate(vals[1:], start=2):
        a = str(row[0]).strip() if len(row)>0 else ""
        b = str(row[1]).strip() if len(row)>1 else ""
        if a==ym and b==label:
            return i
    return len(vals)+1

def write_summary_for_month(ws, ym: str,
                            counts: dict, med: dict, mean: dict,
                            prev_counts: dict|None):
    header = _retry(ws.row_values, 1)
    hmap = {str(h).strip(): i+1 for i,h in enumerate(header) if str(h).strip()}

    def put_row(row_idx: int, label: str, line_map: dict):
        # A: 년월, B: 구분, 이후 열: SUMMARY_COLS 순서대로
        payload = [
            {"range": f"A{row_idx}", "values": [[ym]]},
            {"range": f"B{row_idx}", "values": [[label]]},
        ]
        for col in SUMMARY_COLS:
            if col not in hmap: 
                continue
            v = line_map.get(col, "")
            payload.append({"range": f"{a1_col(hmap[col])}{row_idx}", "values": [[v]]})
        batch_values_update(ws, payload)

    # 1) 거래건수
    row1 = find_summary_row(ws, ym, "거래건수")
    put_row(row1, "거래건수", counts)
    log(f"[summary] {ym} 거래건수 -> row={row1}")

    # 2) 중앙값(단위:억)
    row2 = find_summary_row(ws, ym, "중앙값(단위:억)")
    put_row(row2, "중앙값(단위:억)", med)
    log(f"[summary] {ym} 중앙값 -> row={row2}")

    # 3) 평균가(단위:억)
    row3 = find_summary_row(ws, ym, "평균가(단위:억)")
    put_row(row3, "평균가(단위:억)", mean)
    log(f"[summary] {ym} 평균가 -> row={row3}")

    # 4) 전월대비 건수증감
    row4 = find_summary_row(ws, ym, "전월대비 건수증감")
    diffs = {col:"0" for col in SUMMARY_COLS}
    if prev_counts:
        for col in SUMMARY_COLS:
            cur = int(counts.get(col, 0) or 0)
            prv = int(prev_counts.get(col, 0) or 0)
            d = cur - prv
            diffs[col] = f"+{d}" if d>0 else (str(d) if d<0 else "0")
    put_row(row4, "전월대비 건수증감", diffs)
    log(f"[summary] {ym} 전월대비 -> row={row4}")

    # 5) 예상건수 (빈칸)
    row5 = find_summary_row(ws, ym, "예상건수")
    blanks = {col:"" for col in SUMMARY_COLS}
    put_row(row5, "예상건수", blanks)
    log(f"[summary] {ym} 예상건수 -> row={row5}")

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
    ws_summary = fuzzy_ws(sh, "거래요약")  # 없으면 건너뜀

    # 1) 파일 스캔 (전월대비 계산을 위해 월별 결과 먼저 모두 모으고, 이후 요약 탭을 한꺼번에 기록)
    work = Path(args.artifacts_dir)
    files = sorted([p for p in work.rglob("전국 *.xlsx") if p.is_file()])
    log(f"found national files: {len(files)}")

    # 월별 결과 캐시
    month_cache = {}  # ym -> dict(counts/med/mean)
    today_label = fmt_date_kor(datetime.now())
    where_file = open(LOG_DIR/"where_written.txt","w",encoding="utf-8")

    for path in files:
        nat_title, se_title, ym = ym_from_filename(path.name)
        if not ym: 
            continue
        log(f"[file] {path.name} -> {nat_title} / {se_title} / ym={ym}")

        df = read_month_df(path)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        counts, seoul_total, apg_cnt, med_map, mean_map = agg_all_stats(df)

        # 월별 탭 쓰기 (기존 로직 유지: 존재하는 탭에만, 날짜행 찾아 쓰기/없으면 맨 아래 추가)
        ws_nat = fuzzy_ws(sh, nat_title)
        if ws_nat:
            header_nat = _retry(ws_nat.row_values, 1)
            values_nat = {k:int(counts.get(k,0)) for k in header_nat if k and k!="날짜"}
            # 총합계 있으면 보충
            if "총합계" in header_nat:
                values_nat["총합계"] = int(counts.get("전국",0))
            write_month_sheet(ws_nat, today_label, header_nat, values_nat)
            where_file.write(f"{ws_nat.title}\t{today_label}\tOK\n")
        # 없으면 건너뜀 (시트 not found 출력하지 않음)

        ws_se = fuzzy_ws(sh, se_title)
        if ws_se:
            header_se = _retry(ws_se.row_values, 1)
            values_se = {}
            for h in header_se:
                if not h or h=="날짜": 
                    continue
                if h=="총합계":
                    values_se[h] = int(counts.get("서울",0))
                else:
                    values_se[h] = int(counts.get(h,0))
            write_month_sheet(ws_se, today_label, header_se, values_se)
            where_file.write(f"{ws_se.title}\t{today_label}\tOK\n")

        # 요약 탭용 월별 결과 저장
        month_cache[ym] = {
            "counts": {col:int(counts.get(col,0)) for col in SUMMARY_COLS},
            "med": {col:med_map.get(col,"") for col in SUMMARY_COLS},
            "mean": {col:mean_map.get(col,"") for col in SUMMARY_COLS},
        }

    where_file.close()

    # 2) 거래요약 쓰기 (전월대비는 같은 캐시의 ‘전월’과 비교)
    if ws_summary and month_cache:
        # ym 정렬: "YY/MM" → 숫자 정렬
        def ym_key(ym): 
            yy, mm = ym.split("/")
            return (int(yy), int(mm))
        for ym in sorted(month_cache.keys(), key=ym_key):
            cur = month_cache[ym]
            prv = month_cache.get(prev_ym(ym))
            write_summary_for_month(
                ws_summary,
                ym,
                cur["counts"],
                cur["med"],
                cur["mean"],
                prv["counts"] if prv else None
            )

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
