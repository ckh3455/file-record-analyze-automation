# scripts/analyze_and_update.py
# -*- coding: utf-8 -*-

import os, sys, re, json, time, math, random
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Tuple

import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

# ===================== 기본 설정 =====================
LOG_DIR = Path("analyze_report")
WORK_DIR_DEFAULT = "artifacts"
SHEET_NAME_DATA = "data"

SUMMARY_SHEET_NAME = "거래요약"

SUMMARY_COLS = [
    "전국","서울","강남구","압구정동","경기도","인천광역시","세종시","서초구","송파구",
    "용산구","강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구",
    "강서구","강북구","관악구","구로구","금천구","도봉구","동대문구","서대문구","성북구",
    "은평구","중구","중랑구","부산","대구","광주","대전","강원도","경남","경북","전남",
    "전북","충남","충북","제주"
]

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

APGU_SHEET = "압구정동"
APGU_CHANGE_HEADER = ["변경구분", "기록일", "계약년", "계약월", "계약일", "단지명", "전용면적(㎡)", "동", "층", "거래금액(만원)"]

NEEDED_COLS = [
    "광역","구","법정동","도로명","번지","본번","부번","단지명","전용면적(㎡)",
    "계약년","계약월","계약일","거래금액(만원)","동","층"
]

# ===================== 로깅 =====================
def ensure_logdir():
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        if LOG_DIR.exists() and LOG_DIR.is_file():
            LOG_DIR.unlink()
            LOG_DIR.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    ensure_logdir()
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line)
    (LOG_DIR / "latest.log").open("a", encoding="utf-8").write(line + "\n")

def log_block(title: str):
    log(f"[{title.upper()}]")

def fmt_date_kor(d: datetime) -> str:
    return f"{d.year}. {d.month}. {d.day}"

def norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).strip()

# ===================== gspread 공통 =====================
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
                time.sleep(base * (2**i) + random.uniform(0,0.4))
                continue
            raise

def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def open_sheet(sheet_id: str, sa_path: str|None):
    log("[gspread] auth")
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

def fuzzy_ws(sh: gspread.Spreadsheet, title: str) -> Optional[gspread.Worksheet]:
    tgt = norm(title)
    for ws in sh.worksheets():
        if norm(ws.title) == tgt:
            return ws
    return None

def _qualify_range(ws: gspread.Worksheet, rng: str) -> str:
    rng = str(rng)
    if "!" in rng:
        return rng
    return f"'{ws.title}'!{rng}"

def batch_values_update(ws: gspread.Worksheet, payload: List[Dict]):
    body = {
        "valueInputOption": "USER_ENTERED",
        "data": [{"range": _qualify_range(ws, p["range"]), "values": p["values"]} for p in payload],
    }
    try:
        return _retry(ws.spreadsheet.values_batch_update, body=body)
    except TypeError:
        return _retry(ws.client.values_batch_update, ws.spreadsheet.id, body=body)

def ensure_grid(ws: gspread.Worksheet, rows: Optional[int] = None, cols: Optional[int] = None):
    """필요한 행/열 수 만큼 그리드 확장."""
    if rows is not None and ws.row_count < rows:
        _retry(ws.add_rows, rows - ws.row_count)
    if cols is not None and ws.col_count < cols:
        _retry(ws.add_cols, cols - ws.col_count)

# ===================== 엑셀 읽기/표준화 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=SHEET_NAME_DATA)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    for c in NEEDED_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    return df.copy()

# ===================== 집계 =====================
def eok_series(ser) -> pd.Series:
    try:
        s = pd.to_numeric(ser, errors="coerce")
    except Exception:
        return pd.Series([], dtype=float)
    s = s.dropna()
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

    all_eok = eok_series(df["거래금액(만원)"])
    counts["전국"] = int(len(df))
    if not all_eok.empty:
        med["전국"] = round2(all_eok.median())
        mean["전국"] = round2(all_eok.mean())

    if "광역" in df.columns:
        for prov, sub in df.groupby("광역"):
            prov_std = PROV_MAP.get(str(prov), str(prov))
            if prov_std in counts:
                counts[prov_std] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[prov_std] = round2(s.median())
                    mean[prov_std] = round2(s.mean())

    seoul = df[df.get("광역","")=="서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul)>0:
        s = eok_series(seoul["거래금액(만원)"])
        if not s.empty:
            med["서울"] = round2(s.median())
            mean["서울"] = round2(s.mean())

    if "구" in seoul.columns:
        for gu, sub in seoul.groupby("구"):
            gu = str(gu)
            if gu in counts:
                counts[gu] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[gu] = round2(s.median())
                    mean[gu] = round2(s.mean())

    ap = seoul[seoul.get("법정동","")=="압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap)>0:
        s = eok_series(ap["거래금액(만원)"])
        med["압구정동"] = round2(s.median())
        mean["압구정동"] = round2(s.mean())

    return counts, med, mean

# ===================== 월별 탭 쓰기 =====================
def find_or_append_date_row(ws: gspread.Worksheet, date_label: str) -> int:
    vals = _retry(ws.get_all_values)
    if not vals:
        ensure_grid(ws, rows=2)
        return 2
    for i, row in enumerate(vals[1:], start=2):
        if (len(row)>0) and str(row[0]).strip()==date_label:
            return i
    row = len(vals)+1
    ensure_grid(ws, rows=row)
    return row

def write_month_sheet(ws: gspread.Worksheet, date_label: str, header: list[str], values_by_colname: Dict[str,int]):
    hmap = {str(h).strip(): idx+1 for idx,h in enumerate(header) if str(h).strip()}
    row_idx = find_or_append_date_row(ws, date_label)
    ensure_grid(ws, rows=row_idx, cols=len(header))
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
def ym_from_filename(fn: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    m = re.search(r"\s(\d{2})(\d{2})_", fn)
    if not m:
        return None, None, None
    yy, mm = m.group(1), int(m.group(2))
    nat = f"전국 20{yy}년 {mm}월"
    se = f"서울 20{yy}년 {mm}월"
    ym = f"{yy}/{mm}"
    return nat, se, ym

def prev_ym(ym: str) -> str:
    yy, mm = ym.split("/")
    y = int(yy)
    m = int(mm)
    if m == 1:
        return f"{str((y-1)%100).zfill(2)}/12"
    return f"{yy}/{m-1}"

def find_summary_row(ws: gspread.Worksheet, ym: str, label: str) -> int:
    vals = _retry(ws.get_all_values)
    if not vals:
        ensure_grid(ws, rows=2, cols=2+len(SUMMARY_COLS))
        return 2
    for i, row in enumerate(vals[1:], start=2):
        a = str(row[0]).strip() if len(row)>0 else ""
        b = str(row[1]).strip() if len(row)>1 else ""
        if a==ym and b==label:
            return i
    row = len(vals)+1
    ensure_grid(ws, rows=row, cols=2+len(SUMMARY_COLS))
    return row

def put_summary_line(ws: gspread.Worksheet, row_idx: int, line_map: dict, header: List[str]):
    ensure_grid(ws, rows=row_idx, cols=len(header))
    hmap = {str(h).strip(): i+1 for i,h in enumerate(header) if str(h).strip()}
    payload = []
    for col in SUMMARY_COLS:
        if col not in hmap: 
            continue
        payload.append({"range": f"{a1_col(hmap[col])}{row_idx}", "values": [[line_map.get(col,"")]]})
    if payload:
        batch_values_update(ws, payload)

def format_row_bold(ws: gspread.Worksheet, row_idx: int, first_col: int, last_col: int):
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
    _retry(ws.spreadsheet.batch_update, {"requests": [req]})

def color_cells_batch(ws: gspread.Worksheet, row: int, col_rgb_list: List[Tuple[int, Tuple[float,float,float]]]):
    if not col_rgb_list: return
    reqs = []
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
    _retry(ws.spreadsheet.batch_update, {"requests": reqs])

def write_month_summary(ws: gspread.Worksheet,
                        y: int, m: int,
                        counts: dict, med: dict, mean: dict,
                        prev_counts: Optional[dict]):
    ym_label = f"{str(y%100).zfill(2)}/{m}"
    vals = _retry(ws.get_all_values) or []
    if not vals:
        # 헤더 생성
        ensure_grid(ws, rows=1, cols=2+len(SUMMARY_COLS))
        batch_values_update(ws, [{"range": "A1", "values": [["년월","구분"] + SUMMARY_COLS]}])
        vals = [["년월","구분"] + SUMMARY_COLS]
    header = vals[0]

    # 1) 거래건수
    r1 = find_summary_row(ws, ym_label, "거래건수")
    batch_values_update(ws, [
        {"range": f"A{r1}", "values": [[ym_label]]},
        {"range": f"B{r1}", "values": [["거래건수"]]},
    ])
    put_summary_line(ws, r1, counts, header)
    if header:
        format_row_bold(ws, r1, 3, len(header))
    log(f"[summary] {ym_label} 거래건수 -> row={r1}")

    # 2) 중앙값
    r2 = find_summary_row(ws, ym_label, "중앙값(단위:억)")
    batch_values_update(ws, [
        {"range": f"A{r2}", "values": [[ym_label]]},
        {"range": f"B{r2}", "values": [["중앙값(단위:억)"]]},
    ])
    put_summary_line(ws, r2, med, header)
    log(f"[summary] {ym_label} 중앙값 -> row={r2}")

    # 3) 평균가
    r3 = find_summary_row(ws, ym_label, "평균가(단위:억)")
    batch_values_update(ws, [
        {"range": f"A{r3}", "values": [[ym_label]]},
        {"range": f"B{r3}", "values": [["평균가(단위:억)"]]},
    ])
    put_summary_line(ws, r3, mean, header)
    log(f"[summary] {ym_label} 평균가 -> row={r3}")

    # 4) 전월대비
    r4 = find_summary_row(ws, ym_label, "전월대비 건수증감")
    diffs = {}
    if prev_counts:
        for k in SUMMARY_COLS:
            cur = int(counts.get(k, 0) or 0)
            prv = int(prev_counts.get(k, 0) or 0)
            d = cur - prv
            diffs[k] = f"+{d}" if d>0 else (str(d) if d<0 else "0")
    else:
        diffs = {k:"" for k in SUMMARY_COLS}
    batch_values_update(ws, [
        {"range": f"A{r4}", "values": [[ym_label]]},
        {"range": f"B{r4}", "values": [["전월대비 건수증감"]]},
    ])
    put_summary_line(ws, r4, diffs, header)

    hmap = {h: i+1 for i,h in enumerate(header)}
    jobs = []
    for k, v in diffs.items():
        if k in hmap and v not in ("", "0"):
            col = hmap[k]
            if v.startswith("+"):
                jobs.append((col, (0.0, 0.35, 1.0)))
            elif v.startswith("-"):
                jobs.append((1.0, 0.0, 0.0))  # wrong tuple, fixed below

    # fix the above bug: build jobs correctly
    jobs = []
    for k, v in diffs.items():
        if k in hmap and v not in ("", "0"):
            col = hmap[k]
            if v.startswith("+"):
                jobs.append((col, (0.0, 0.35, 1.0)))  # blue
            elif v.startswith("-"):
                jobs.append((col, (1.0, 0.0, 0.0)))  # red
    color_cells_batch(ws, r4, jobs)
    log(f"[summary] {ym_label} 전월대비 -> row={r4}")

    # 5) 예상건수
    r5 = find_summary_row(ws, ym_label, "예상건수")
    batch_values_update(ws, [
        {"range": f"A{r5}", "values": [[ym_label]]},
        {"range": f"B{r5}", "values": [["예상건수"]]},
    ])
    blanks = {k:"" for k in SUMMARY_COLS}
    put_summary_line(ws, r5, blanks, header)
    log(f"[summary] {ym_label} 예상건수 -> row={r5}")

# ===================== 압구정동 원본 누적 + 변동 로그 =====================
def ensure_apgu_sheet(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, APGU_SHEET)
    if ws: return ws
    return _retry(sh.add_worksheet, title=APGU_SHEET, rows=4000, cols=80)

def make_row_key(d: dict) -> str:
    parts = [
        d.get("계약년",""), d.get("계약월",""), d.get("계약일",""),
        d.get("광역",""), d.get("구",""), d.get("법정동",""),
        d.get("단지명",""), d.get("전용면積(㎡)","") if "전용면積(㎡)" in d else d.get("전용면적(㎡)",""),
        d.get("층",""), d.get("거래금액(만원)","")
    ]
    return "|".join(str(x).strip() for x in parts)

def number_or_blank(v):
    if v is None: return ""
    if isinstance(v, float) and (pd.isna(v) or math.isnan(v)): return ""
    return v

def append_change_log(ws: gspread.Worksheet, added_rows: list[list], removed_rows: list[list], header: list[str]):
    all_vals = _retry(ws.get_all_values) or []
    start = len(all_vals) + 1
    now_label = fmt_date_kor(datetime.now())

    idx = {h:i for i,h in enumerate(header)}
    def take(row, col):
        return row[idx[col]] if col in idx and idx[col] < len(row) else ""

    def to_log_row(kind, row):
        return [
            kind, now_label,
            take(row, "계약년"), take(row, "계약월"), take(row, "계약일"),
            take(row, "단지명"), take(row, "전용면적(㎡)"),
            take(row, "동"), take(row, "층"), take(row, "거래금액(만원)")
        ]

    rows = []
    rows += [to_log_row("(신규)", r) for r in added_rows]
    rows += [to_log_row("(삭제)", r) for r in removed_rows]
    if not rows: return

    end = start + len(rows)
    ensure_grid(ws, rows=end, cols=10)

    batch_values_update(ws, [
        {"range": f"A{start}:J{start}", "values": [APGU_CHANGE_HEADER]},
        {"range": f"A{start+1}:J{end}", "values": rows},
    ])

    req = {
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": start-1, "endRowIndex": end, "startColumnIndex": 0, "endColumnIndex": 10},
            "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red":1.0,"green":0.0,"blue":0.0}}}},
            "fields": "userEnteredFormat.textFormat.foregroundColor"
        }
    }
    _retry(ws.spreadsheet.batch_update, {"requests": [req]})

def upsert_apgu_raw(ws: gspread.Worksheet, df_all: pd.DataFrame):
    cond = (df_all.get("광역","")=="서울특별시") & (df_all.get("법정동","")=="압구정동")
    df = df_all[cond].copy()
    log(f"[압구정동] filtered {len(df)} rows")
    if df.empty:
        log("[압구정동] no rows in files")
        return

    for c in ["계약년","계약월","계약일"]:
        if c not in df.columns: df[c] = pd.NA
    df = df.sort_values(["계약년","계약월","계약일"], ascending=[True,True,True], kind="mergesort")

    vals = _retry(ws.get_all_values) or []
    if not vals:
        header = list(df.columns) + ["기록일"]
        ensure_grid(ws, rows=1, cols=len(header))
        batch_values_update(ws, [{"range": "A1", "values": [header]}])
        vals = [header]
    else:
        header = vals[0]
        if "기록일" not in header:
            header = header + ["기록일"]
            ensure_grid(ws, rows=1, cols=len(header))
            batch_values_update(ws, [{"range": "A1", "values": [header]}])
            vals = _retry(ws.get_all_values) or [header]
        union = list(dict.fromkeys(header + [c for c in df.columns if c not in header]))
        if union != header:
            header = union
            ensure_grid(ws, rows=1, cols=len(header))
            batch_values_update(ws, [{"range": "A1", "values": [header]}])
            vals = _retry(ws.get_all_values) or [header]

    existing_rows = vals[1:]
    idx_map = {h:i for i,h in enumerate(header)}
    def row_to_dict(row):
        return {k:(row[i] if i<len(row) else "") for k,i in idx_map.items()}

    existing_keys = set()
    for r in existing_rows:
        existing_keys.add(make_row_key(row_to_dict(r)))

    today_label = fmt_date_kor(datetime.now())
    new_records = []
    for _, r in df.iterrows():
        d = {k: r.get(k, "") for k in header if k in df.columns}
        k = make_row_key(d)
        if k in existing_keys:
            continue
        rec = [number_or_blank(r.get(col, "")) for col in header if col != "기록일"]
        rec.append(today_label)
        new_records.append(rec)
        existing_keys.add(k)

    file_keys = set()
    for _, r in df.iterrows():
        d = {k: r.get(k, "") for k in header if k in df.columns}
        file_keys.add(make_row_key(d))

    removed = []
    for r in existing_rows:
        d = row_to_dict(r)
        if make_row_key(d) not in file_keys:
            removed.append([d.get(h,"") for h in header])

    if new_records:
        start_row = len(vals) + 1
        end_row = start_row + len(new_records) - 1
        end_col = len(header)
        ensure_grid(ws, rows=end_row, cols=end_col)  # ★ 그리드 확장
        rng = f"A{start_row}:{a1_col(end_col)}{end_row}"
        batch_values_update(ws, [{"range": rng, "values": new_records}])  # ★ 오타 수정
        log(f"[압구정동] appended {len(new_records)} rows")
    else:
        log("[압구정동] no new rows to append")

    append_change_log(ws, new_records, removed, header)

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

    # 1) 파일 모으기
    files = sorted([p for p in Path(args.artifacts_dir).rglob("전국 *.xlsx") if p.is_file()])
    log(f"[collect] found {len(files)} xlsx files")

    month_cache: Dict[Tuple[int,int], Dict] = {}
    apgu_all: List[pd.DataFrame] = []
    write_date = datetime.now()
    today_label = fmt_date_kor(write_date)

    for path in files:
        nat_title, se_title, ym = ym_from_filename(path.name)
        if not ym:
            continue
        log(f"[read] {path.name}")
        df = read_month_df(path)

        counts, med, mean = agg_all_stats(df)

        # 월별 탭 기록 (존재하는 탭만)
        if nat_title:
            ws_nat = fuzzy_ws(sh, nat_title)
            if ws_nat:
                header_nat = _retry(ws_nat.row_values, 1)
                ensure_grid(ws_nat, rows=2, cols=len(header_nat) if header_nat else 2)
                vals_nat = {}
                for h in header_nat:
                    if not h or h == "날짜": continue
                    if h == "총합계":
                        vals_nat[h] = int(counts.get("전국", 0))
                    else:
                        vals_nat[h] = int(counts.get(h, 0))
                write_month_sheet(ws_nat, today_label, header_nat, vals_nat)
                log(f"[전국] {nat_title} -> {today_label}")

        if se_title:
            ws_se = fuzzy_ws(sh, se_title)
            if ws_se:
                header_se = _retry(ws_se.row_values, 1)
                ensure_grid(ws_se, rows=2, cols=len(header_se) if header_se else 2)
                vals_se = {}
                for h in header_se:
                    if not h or h == "날짜": continue
                    if h == "총합계":
                        vals_se[h] = int(counts.get("서울", 0))
                    else:
                        vals_se[h] = int(counts.get(h, 0))
                write_month_sheet(ws_se, today_label, header_se, vals_se)
                log(f"[서울] {se_title} -> {today_label}")

        yy, mm = ym.split("/")
        y = 2000 + int(yy); m = int(mm)
        month_cache[(y, m)] = {
            "counts": {col:int(counts.get(col,0)) for col in SUMMARY_COLS},
            "med": {col:med.get(col,"") for col in SUMMARY_COLS},
            "mean": {col:mean.get(col,"") for col in SUMMARY_COLS},
        }

        ap = df[(df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")].copy()
        if not ap.empty:
            apgu_all.append(ap)

    # 2) 거래요약
    ws_sum = fuzzy_ws(sh, SUMMARY_SHEET_NAME)
    if ws_sum and month_cache:
        vals = _retry(ws_sum.get_all_values) or []
        if not vals:
            ensure_grid(ws_sum, rows=1, cols=2+len(SUMMARY_COLS))
            batch_values_update(ws_sum, [{"range": "A1", "values": [["년월","구분"] + SUMMARY_COLS]}])
        for (y,m) in sorted(month_cache.keys()):
            cur = month_cache[(y,m)]
            prev = month_cache.get((y, m-1)) if m>1 else month_cache.get((y-1, 12))
            prev_counts = prev["counts"] if prev else None
            write_month_summary(ws_sum, y, m, cur["counts"], cur["med"], cur["mean"], prev_counts)
            time.sleep(0.2)

    # 3) 압구정동
    if apgu_all:
        ws_ap = ensure_apgu_sheet(sh)
        all_df = pd.concat(apgu_all, ignore_index=True)
        upsert_apgu_raw(ws_ap, all_df)

    log("[main] done")

if __name__ == "__main__":
    main()
