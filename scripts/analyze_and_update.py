# -*- coding: utf-8 -*-
import os, sys, re, json, time, random
from pathlib import Path
from datetime import datetime
import pandas as pd

# ===================== 기본 설정 =====================
LOG_DIR = Path("analyze_report")
WORK_DIR_DEFAULT = "artifacts"
SHEET_NAME_DATA = "data"

# 요약 탭 열(왼→오 순서 유지)
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
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        if LOG_DIR.is_file():
            LOG_DIR.unlink()
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
    return f"{d.year}. {d.month}. {d.day}"

def ym_from_filename(fn: str):
    m = re.search(r"(\d{2})(\d{2})", fn)
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
    return None  # 기존 원칙: 새 탭 생성하지 않음(압구정동만 예외)

def batch_values_update(ws, payload):
    body = {"valueInputOption":"USER_ENTERED","data":payload}
    return _retry(ws.spreadsheet.values_batch_update, body=body)

# ======== 서식 적용(볼드/색상) ========
def format_row_bold(ws, row_idx: int, first_col: int, last_col: int):
    req = {
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": row_idx-1,
                "endRowIndex": row_idx,
                "startColumnIndex": first_col-1,
                "endColumnIndex": last_col
            },
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.textFormat.bold"
        }
    }
    _retry(ws.spreadsheet.batch_update, {"requests":[req]})

def format_diffs_colors(ws, row_idx: int, col_sign_map: dict):
    requests = []
    for col_idx, sign in col_sign_map.items():
        if sign == 0:
            continue
        color = {"red":0.85,"green":0.0,"blue":0.0} if sign<0 else {"red":0.0,"green":0.35,"blue":0.8}
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": row_idx-1,
                    "endRowIndex": row_idx,
                    "startColumnIndex": col_idx-1,
                    "endColumnIndex": col_idx
                },
                "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": color}}},
                "fields": "userEnteredFormat.textFormat.foregroundColor"
            }
        })
    if requests:
        _retry(ws.spreadsheet.batch_update, {"requests": requests})

# ===================== 파일 읽기 & 표준화 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=SHEET_NAME_DATA, dtype=str)
    # 숫자형 변환
    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in NEEDED_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    # 컬럼 순서 유지(원본 순)
    return df.copy()

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
        return counts, 0, 0, med, mean

    # 전국
    all_eok = eok_series(df["거래금액(만원)"])
    counts["전국"] = int(len(df))
    if not all_eok.empty:
        med["전국"] = round(float(all_eok.median()), 2)
        mean["전국"] = round(float(all_eok.mean()), 2)

    # 광역
    if "광역" in df.columns:
        for prov, sub in df.groupby("광역"):
            prov_std = PROV_MAP.get(str(prov), str(prov))
            if prov_std in SUMMARY_COLS:
                counts[prov_std] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[prov_std] = round(float(s.median()), 2)
                    mean[prov_std] = round(float(s.mean()), 2)

    # 서울/자치구
    seoul = df[df.get("광역","")=="서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul)>0:
        s = eok_series(seoul["거래금액(만원)"])
        if not s.empty:
            med["서울"] = round(float(s.median()), 2)
            mean["서울"] = round(float(s.mean()), 2)

    if "구" in seoul.columns:
        for gu, sub in seoul.groupby("구"):
            gu = str(gu)
            if gu in SUMMARY_COLS:
                counts[gu] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[gu] = round(float(s.median()), 2)
                    mean[gu] = round(float(s.mean()), 2)

    # 압구정동
    ap = seoul[seoul.get("법정동","")=="압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap)>0:
        s = eok_series(ap["거래금액(만원)"])
        med["압구정동"] = round(float(s.median()), 2)
        mean["압구정동"] = round(float(s.mean()), 2)

    return counts, int(len(seoul)), int(len(ap)), med, mean

# ===================== 월별 탭 쓰기 =====================
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
        print(f"[ws] {ws.title} -> {date_label} row={row_idx}")

# ===================== 거래요약 쓰기 & 서식 =====================
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

    # 거래건수
    row1 = find_summary_row(ws, ym, "거래건수")
    put_row(row1, "거래건수", counts)
    if SUMMARY_COLS and all(c in hmap for c in SUMMARY_COLS):
        first_c = min(hmap[c] for c in SUMMARY_COLS)
        last_c  = max(hmap[c] for c in SUMMARY_COLS)
        format_row_bold(ws, row1, first_c, last_c)
    print(f"[summary] {ym} 거래건수 -> row={row1}")

    # 중앙값
    row2 = find_summary_row(ws, ym, "중앙값(단위:억)")
    put_row(row2, "중앙값(단위:억)", med)
    print(f"[summary] {ym} 중앙값 -> row={row2}")

    # 평균가
    row3 = find_summary_row(ws, ym, "평균가(단위:억)")
    put_row(row3, "평균가(단위:억)", mean)
    print(f"[summary] {ym} 평균가 -> row={row3}")

    # 전월대비
    row4 = find_summary_row(ws, ym, "전월대비 건수증감")
    diffs = {col:"0" for col in SUMMARY_COLS}
    sign_map = {}
    if prev_counts:
        for col in SUMMARY_COLS:
            cur = int(counts.get(col, 0) or 0)
            prv = int(prev_counts.get(col, 0) or 0)
            d = cur - prv
            diffs[col] = f"+{d}" if d>0 else (str(d) if d<0 else "0")
    put_row(row4, "전월대비 건수증감", diffs)
    for col in SUMMARY_COLS:
        if col not in hmap: 
            continue
        txt = diffs.get(col, "0")
        sign = 1 if (isinstance(txt,str) and txt.startswith("+")) else (-1 if str(txt).startswith("-") else 0)
        if sign != 0:
            sign_map[hmap[col]] = sign
    if sign_map:
        format_diffs_colors(ws, row4, sign_map)
    print(f"[summary] {ym} 전월대비 -> row={row4}")

    # 예상건수 (빈칸)
    row5 = find_summary_row(ws, ym, "예상건수")
    blanks = {col:"" for col in SUMMARY_COLS}
    put_row(row5, "예상건수", blanks)
    print(f"[summary] {ym} 예상건수 -> row={row5}")

# ===================== 압구정동 탭(원본 행 그대로) =====================
def ensure_apgu_sheet(sh):
    ws = fuzzy_ws(sh, "압구정동")
    if ws:
        return ws
    return _retry(sh.add_worksheet, title="압구정동", rows=2000, cols=30)

def make_row_key(row_dict: dict, header: list[str]) -> str:
    # 중복 판정 키 (계약년/월/일 + 주요 항목)
    fields = ["계약년","계약월","계약일","광역","구","법정동","도로명","번지","본번","부번","단지명","전용면적(㎡)","층","거래금액(만원)"]
    parts = [str(row_dict.get(f,"")).strip() for f in fields if f in header]
    return "|".join(parts)

def number_or_blank(v):
    if v is None or (isinstance(v,float) and pd.isna(v)): return ""
    if isinstance(v, (int,float)) and float(v).is_integer():
        return int(v)
    return v

def upsert_apgu_raw(ws, df_all: pd.DataFrame, webhook_url: str|None):
    # 압구정동 행만
    df = df_all[(df_all.get("법정동","")==="압구정동") & (df_all.get("광역","")==="서울특별시")].copy()
    if df.empty:
        log("[압구정동] no rows in this file")
        return

    # 계약년/월/일 오름차순 정렬
    if set(["계약년","계약월","계약일"]).issubset(df.columns):
        df = df.sort_values(["계약년","계약월","계약일","거래금액(만원)"], ascending=[True,True,True,True])

    # 기존 시트 값 읽기
    vals = _retry(ws.get_all_values) or []
    if not vals:
        # 헤더 생성: 원본 파일의 컬럼 순서 그대로
        header = list(df.columns)
        _retry(ws.update, [header], "A1")
        vals = [header]
    else:
        header = vals[0]

    # 기존 중복키 셋/ 기존 월별 카운트
    existing_rows = vals[1:]
    idx_map = {name:i for i,name in enumerate(header)}
    def row_to_dict(row):
        d={}
        for k, i in idx_map.items():
            d[k] = row[i] if i < len(row) else ""
        return d
    existing_keys = set()
    for r in existing_rows:
        existing_keys.add(make_row_key(row_to_dict(r), header))

    def ym_of_row(dct):
        try:
            y = int(float(dct.get("계약년","") or 0))
            m = int(float(dct.get("계약월","") or 0))
            return f"{str(y%100).zfill(2)}/{m}"
        except Exception:
            return ""

    # 기존 월별 카운트
    from collections import Counter
    prev_counter = Counter()
    for r in existing_rows:
        d = row_to_dict(r)
        ym = ym_of_row(d)
        if ym:
            prev_counter[ym]+=1

    # 추가할 행 수집
    new_records = []
    for _, row in df.iterrows():
        d = {col: row.get(col, "") for col in header if col in df.columns}
        key = make_row_key(d, header)
        if key in existing_keys:
            continue
        rec = [number_or_blank(row.get(col, "")) for col in header]
        new_records.append(rec)
        existing_keys.add(key)

    if not new_records:
        log("[압구정동] no new rows to append")
    else:
        start_row = len(vals)+1
        end_row = start_row + len(new_records) - 1
        rng = f"A{start_row}:{a1_col(len(header))}{end_row}"
        _retry(ws.update, new_records, rng)
        log(f"[압구정동] appended {len(new_records)} rows")

        # 정렬(계약년, 계약월, 계약일 오름차순)
        sort_requests = []
        for col_name in ["계약년","계약월","계약일"]:
            if col_name in header:
                col_idx0 = header.index(col_name)
                sort_requests.append({
                    "sortSpec": {
                        "dimensionIndex": col_idx0,
                        "sortOrder": "ASCENDING"
                    }
                })
        if sort_requests:
            _retry(ws.spreadsheet.batch_update, {
                "requests": [{
                    "sortRange": {
                        "range": {
                            "sheetId": ws.id,
                            "startRowIndex": 1,  # 헤더 제외
                            "startColumnIndex": 0,
                            "endColumnIndex": len(header)
                        },
                        "sortSpecs": [s["sortSpec"] for s in sort_requests]
                    }
                }]
            })

        # 알림(월별 건수 증가 감지)
        if webhook_url:
            # 최신 전체 데이터 다시 읽기
            new_vals = _retry(ws.get_all_values) or []
            counter = Counter()
            if new_vals:
                hdr = new_vals[0]
                idx = {h:i for i,h in enumerate(hdr)}
                for r in new_vals[1:]:
                    d = {k:(r[i] if i<len(r) else "") for k,i in idx.items()}
                    ym = ym_of_row(d)
                    if ym: counter[ym]+=1
            notify_list = []
            for ym, cnt in counter.items():
                prv = prev_counter.get(ym, 0)
                if cnt > prv:
                    notify_list.append((ym, prv, cnt))
            if notify_list:
                send_webhook(webhook_url, notify_list)

# ------------------ 웹훅 ------------------
def send_webhook(url: str, notify_list):
    data = {
        "title": "압구정동 거래건수 증가",
        "items": [{"ym": ym, "old": old, "new": new} for ym,old,new in notify_list]
    }
    try:
        import requests
        requests.post(url, json=data, timeout=10)
        log(f"[webhook] sent via requests: {len(notify_list)} items")
    except Exception:
        try:
            import urllib.request
            req = urllib.request.Request(
                url,
                data=json.dumps(data).encode("utf-8"),
                headers={"Content-Type":"application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                _ = resp.read()
            log(f"[webhook] sent via urllib: {len(notify_list)} items")
        except Exception as e:
            log(f"[webhook] failed: {e!r}")

# ===================== 메인 =====================
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts-dir", default=WORK_DIR_DEFAULT)
    parser.add_argument("--sheet-id", default=os.environ.get("SHEET_ID",""))
    parser.add_argument("--sa", default="sa.json")
    args = parser.parse_args()

    ensure_logdir()
    (LOG_DIR/"latest.log").write_text("", encoding="utf-8")
    log_block("main")
    log(f"artifacts_dir={args.artifacts_dir}")

    sh = open_sheet(args.sheet_id, args.sa)
    ws_summary = fuzzy_ws(sh, "거래요약")  # 없으면 스킵
    webhook_url = os.environ.get("APGU_WEBHOOK_URL","").strip() or None

    # 파일 모으기
    work = Path(args.artifacts_dir)
    files = sorted([p for p in work.rglob("전국 *.xlsx") if p.is_file()])
    log(f"found national files: {len(files)}")

    month_cache = {}  # ym -> {counts/med/mean}
    apgu_df_all = []  # 압구정동 원본 누적 (모든 파일)

    today_label = fmt_date_kor(datetime.now())
    with open(LOG_DIR/"where_written.txt","w",encoding="utf-8") as wf:
        for path in files:
            nat_title, se_title, ym = ym_from_filename(path.name)
            if not ym:
                continue
            log(f"[file] {path.name} -> {nat_title} / {se_title} / ym={ym}")

            df = read_month_df(path)
            log(f"[read] rows={len(df)} cols={len(df.columns)}")

            # 집계
            counts, seoul_total, apg_cnt, med_map, mean_map = agg_all_stats(df)

            # 월별 탭(전국/서울): 총합계 포함
            ws_nat = fuzzy_ws(sh, nat_title)
            if ws_nat:
                header_nat = _retry(ws_nat.row_values, 1)
                vals_nat = {}
                for h in header_nat:
                    if not h or h=="날짜": continue
                    if h=="총합계":
                        vals_nat[h] = int(counts.get("전국",0))
                    else:
                        vals_nat[h] = int(counts.get(h,0))
                write_month_sheet(ws_nat, today_label, header_nat, vals_nat)
                wf.write(f"{ws_nat.title}\t{today_label}\tOK\n")

            ws_se = fuzzy_ws(sh, se_title)
            if ws_se:
                header_se = _retry(ws_se.row_values, 1)
                vals_se = {}
                for h in header_se:
                    if not h or h=="날짜": continue
                    if h=="총합계":
                        vals_se[h] = int(counts.get("서울",0))
                    else:
                        vals_se[h] = int(counts.get(h,0))
                write_month_sheet(ws_se, today_label, header_se, vals_se)
                wf.write(f"{ws_se.title}\t{today_label}\tOK\n")

            # 거래요약 캐시
            month_cache[ym] = {
                "counts": {col:int(counts.get(col,0)) for col in SUMMARY_COLS},
                "med": {col:med_map.get(col,"") for col in SUMMARY_COLS},
                "mean": {col:mean_map.get(col,"") for col in SUMMARY_COLS},
            }

            # 압구정동 원본 누적(후에 한 번에 merge/sort)
            ap = df[(df.get("광역","")==="서울특별시") & (df.get("법정동","")==="압구정동")].copy()
            if not ap.empty:
                apgu_df_all.append(ap)

    # 거래요약 채우기 + 서식
    if ws_summary and month_cache:
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

    # 압구정동: 원본 행 그대로 병합/정렬/중복제거 +(선택)알림
    if apgu_df_all:
        ws_ap = ensure_apgu_sheet(sh)
        all_ap = pd.concat(apgu_df_all, ignore_index=True)
        upsert_apgu_raw(ws_ap, all_ap, webhook_url)

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
