# scripts/analyze_and_update.py
# -*- coding: utf-8 -*-

import os, re, json, time, math, random, sys
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Tuple

import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

# ===================== 기본 설정 =====================
LOG_DIR = Path("analyze_report")
ART_DIR_DEFAULT = "artifacts"
DATA_SHEET_NAME = "data"

SUMMARY_SHEET_NAME = "거래요약"

# 거래요약 헤더(좌→우 순서 보존)
SUMMARY_COLS = [
    "전국","서울","강남구","압구정동","경기도","인천광역시","세종시","서초구","송파구","용산구",
    "강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구","강서구",
    "강북구","관악구","구로구","금천구","도봉구","동대문구","서대문구","성북구","은평구",
    "중구","중랑구","부산","대구","광주","대전","강원도","경남","경북","전남","전북","충남","충북","제주"
]

# 광역명 → 요약 컬럼명 매핑(표준화)
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

# 압구정동 탭/로그
APGU_SHEET = "압구정동"
APGU_CHANGE_HEADER = ["변경구분","기록일","계약년","계약월","계약일","단지명","전용면적(㎡)","동","층","거래금액(만원)"]

NEEDED_COLS = [
    "광역","구","법정동","도로명","번지","본번","부번","단지명","전용면적(㎡)",
    "계약년","계약월","계약일","거래금액(만원)","동","층"
]

# ===================== 유틸/로깅 =====================
def _ensure_log_dir():
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        if LOG_DIR.exists() and LOG_DIR.is_file():
            LOG_DIR.unlink()
            LOG_DIR.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    _ensure_log_dir()
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n"
    p = LOG_DIR / "latest.log"
    if p.exists():
        p.write_text(p.read_text(encoding="utf-8") + line, encoding="utf-8")
    else:
        p.write_text(line, encoding="utf-8")
    print(line, end="")

def log_block(title: str):
    log(f"[{title.upper()}]")

def fmt_date_kor(dt: datetime) -> str:
    # 구글 시트 표시 형식과 동일: 2025. 9. 28
    return f"{dt.year}. {dt.month}. {dt.day}"

def norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).strip()

# ===================== gspread 헬퍼(쿼터/리트라이/배치) =====================
_last_req_ts = 0.0
def _throttle(min_interval=0.45):
    global _last_req_ts
    now = time.time()
    if now - _last_req_ts < min_interval:
        time.sleep(min_interval - (now - _last_req_ts))
    _last_req_ts = time.time()

def _retry(fn, *a, **kw):
    base = 0.8
    for i in range(6):
        try:
            _throttle()
            return fn(*a, **kw)
        except APIError as e:
            s = str(e)
            if any(x in s for x in ("429", "500", "502", "503")):
                time.sleep(base*(2**i) + random.uniform(0, 0.3))
                continue
            raise

def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def batch_values_update(ws: gspread.Worksheet, payload: List[Dict]):
    body = {"valueInputOption": "USER_ENTERED", "data": payload}
    # gspread 6.x
    try:
        return _retry(ws.spreadsheet.values_batch_update, body=body)
    except TypeError:
        # gspread 5.x
        return _retry(ws.client.values_batch_update, ws.spreadsheet.id, body)

def batch_format(ws: gspread.Worksheet, requests: List[dict]):
    if not requests: return
    _retry(ws.spreadsheet.batch_update, {"requests": requests})

def open_sheet(sheet_id: str, sa_path: Optional[str]) -> gspread.Spreadsheet:
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
    sh = _retry(gc.open_by_key, sheet_id)
    log("[gspread] spreadsheet opened")
    return sh

def fuzzy_ws(sh: gspread.Spreadsheet, title: str) -> Optional[gspread.Worksheet]:
    tgt = norm(title)
    # 1) 완전 일치(공백무시)
    for ws in sh.worksheets():
        if norm(ws.title) == tgt:
            return ws
    # 2) 부분 포함(안전하게)
    for ws in sh.worksheets():
        if tgt in norm(ws.title):
            return ws
    return None  # 없으면 건너뜀 (기존 시트만 사용)

# ===================== Worksheet 캐시(읽기 최소화) =====================
class WorksheetCache:
    def __init__(self, ws: gspread.Worksheet):
        self.ws = ws
        self._vals: Optional[List[List[str]]] = None
        self._header: Optional[List[str]] = None
        self._hmap: Optional[Dict[str,int]] = None

    def values(self) -> List[List[str]]:
        if self._vals is None:
            self._vals = _retry(self.ws.get_all_values) or []
        return self._vals

    def header(self) -> List[str]:
        if self._header is None:
            vals = self.values()
            self._header = vals[0] if vals else []
        return self._header

    def header_map(self) -> Dict[str,int]:
        if self._hmap is None:
            self._hmap = {h: i+1 for i,h in enumerate(self.header())}
        return self._hmap

    def row_count(self) -> int:
        return len(self.values())

    def invalidate(self):
        self._vals = None
        self._header = None
        self._hmap = None

# ===================== 파일 읽기/표준화 & 집계 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=DATA_SHEET_NAME)
    # 중복 컬럼 제거
    df = df.loc[:, ~df.columns.duplicated()].copy()
    # 누락 컬럼 보정
    for c in NEEDED_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    return df.copy()

def eok_series(ser) -> pd.Series:
    try:
        s = pd.to_numeric(ser, errors="coerce").dropna()
    except Exception:
        return pd.Series([], dtype=float)
    if s.empty:
        return pd.Series([], dtype=float)
    return s / 10000.0

def _fmt2(v) -> str:
    try:
        return f"{float(v):.2f}"
    except Exception:
        return ""

def agg_all_stats(df: pd.DataFrame):
    counts = {col:0 for col in SUMMARY_COLS}
    med = {col:"" for col in SUMMARY_COLS}
    mean = {col:"" for col in SUMMARY_COLS}

    # 전국
    counts["전국"] = int(len(df))
    s_all = eok_series(df["거래금액(만원)"])
    if not s_all.empty:
        med["전국"] = _fmt2(s_all.median())
        mean["전국"] = _fmt2(s_all.mean())

    # 광역
    if "광역" in df.columns:
        for prov, sub in df.groupby("광역"):
            prov_std = PROV_MAP.get(str(prov), str(prov))
            if prov_std in counts:
                counts[prov_std] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[prov_std] = _fmt2(s.median())
                    mean[prov_std] = _fmt2(s.mean())

    # 서울/자치구
    seoul = df[df.get("광역","")=="서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul) > 0:
        s = eok_series(seoul["거래금액(만원)"])
        if not s.empty:
            med["서울"] = _fmt2(s.median())
            mean["서울"] = _fmt2(s.mean())

    if "구" in seoul.columns:
        for gu, sub in seoul.groupby("구"):
            gu = str(gu)
            if gu in counts:
                counts[gu] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[gu] = _fmt2(s.median())
                    mean[gu] = _fmt2(s.mean())

    # 압구정동
    ap = seoul[seoul.get("법정동","")=="압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap) > 0:
        s = eok_series(ap["거래금액(만원)"])
        med["압구정동"] = _fmt2(s.median())
        mean["압구정동"] = _fmt2(s.mean())

    return counts, med, mean

# ===================== 월별 탭 쓰기 =====================
def detect_date_col(header: List[str]) -> int:
    for i, v in enumerate(header, start=1):
        if str(v).strip() == "날짜":
            return i
    return 1

def normalize_sheet_date_cell(v: str) -> Optional[str]:
    s = str(v).strip()
    if not s: return None
    m = re.match(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})", s)
    if m:
        y, mo, d = map(int, m.groups())
        return f"{y:04d}-{mo:02d}-{d:02d}"
    m2 = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m2:
        return s
    return None

def find_row_by_date(cache: WorksheetCache, date_col: int, target: datetime) -> Optional[int]:
    target_key = f"{target.year:04d}-{target.month:02d}-{target.day:02d}"
    for idx, row in enumerate(cache.values()[1:], start=2):
        cell = row[date_col-1] if date_col-1 < len(row) else ""
        key = normalize_sheet_date_cell(cell)
        if key == target_key:
            return idx
    return None

def write_counts_to_month_sheet(
    ws: gspread.Worksheet,
    cache: WorksheetCache,
    write_date: datetime,
    header_names: List[str],
    series_map: Dict[str,int],
    sum_col_name: str = "총합계",
    prefer_total_key: str = "전국",
):
    header = cache.header()
    if not header:
        return
    date_col = detect_date_col(header)
    row = find_row_by_date(cache, date_col, write_date)
    if row is None:
        row = cache.row_count() + 1
        # 날짜(표시용)
        label = fmt_date_kor(write_date)
        batch_values_update(ws, [{"range": f"A{row}:A{row}", "values": [[label]]}])
        cache.invalidate()

    hmap = {h: i+1 for i,h in enumerate(cache.header())}
    payload = []
    # 값 쓰기
    total_sum = 0
    for name in header_names:
        if name in hmap and name != "날짜":
            col = hmap[name]
            v = int(series_map.get(name, 0) or 0)
            payload.append({"range": f"{a1_col(col)}{row}", "values": [[v]]})
            if name != prefer_total_key:
                total_sum += v
    # 총합계
    if sum_col_name in hmap:
        val = series_map.get(prefer_total_key, total_sum)
        payload.append({"range": f"{a1_col(hmap[sum_col_name])}{row}", "values": [[int(val or 0)]]})

    if payload:
        batch_values_update(ws, payload)
        cache.invalidate()
        log(f"[ws] {ws.title} -> {fmt_date_kor(write_date)} row={row}")

# ===================== 거래요약(월 단위) =====================
def parse_file_meta(p: Path) -> Optional[Tuple[int,int]]:
    # "전국 2509_250928.xlsx" → (2025, 9)
    m = re.search(r"\s(\d{2})(\d{1,2})_", p.stem)
    if not m: return None
    yy, mm = int(m.group(1)), int(m.group(2))
    return 2000+yy, mm

def build_summary_index(vals: List[List[str]]) -> Dict[Tuple[str,str], int]:
    idx = {}
    for i, r in enumerate(vals[1:], start=2):
        ym = (r[0] if len(r)>0 else "").strip()
        lb = (r[1] if len(r)>1 else "").strip()
        if ym and lb: idx[(ym, lb)] = i
    return idx

def ensure_summary_header(ws: gspread.Worksheet, cache: WorksheetCache):
    if not cache.values():
        _retry(ws.update, [["년월","구분"] + SUMMARY_COLS], "A1")
        cache.invalidate()

def ensure_summary_row(ws: gspread.Worksheet, cache: WorksheetCache, ym: str, what: str) -> int:
    vals = cache.values()
    idx = build_summary_index(vals)
    if (ym, what) in idx:
        return idx[(ym, what)]
    row = cache.row_count() + 1
    _retry(ws.update, [[ym, what]], f"A{row}:B{row}", value_input_option="USER_ENTERED")
    cache.invalidate()
    return row

def put_summary_line(ws: gspread.Worksheet, cache: WorksheetCache, row_idx: int, line_map: dict):
    hmap = {h: i+1 for i,h in enumerate(cache.header())}
    payload = []
    for col in SUMMARY_COLS:
        if col in hmap:
            payload.append({"range": f"{a1_col(hmap[col])}{row_idx}", "values": [[line_map.get(col, "")]]})
    if payload:
        batch_values_update(ws, payload)
        cache.invalidate()

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
    batch_format(ws, [req])

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
    batch_format(ws, reqs)

def prev_month(y: int, m: int) -> Tuple[int,int]:
    if m == 1: return (y-1, 12)
    return (y, m-1)

def write_month_summary(
    ws: gspread.Worksheet,
    cache: WorksheetCache,
    y: int, m: int,
    counts: dict, med: dict, mean: dict,
    prev_counts: Optional[dict]
):
    ym = f"{str(y%100).zfill(2)}/{m}"

    # 헤더 보장
    ensure_summary_header(ws, cache)

    # 거래건수(볼드)
    r1 = ensure_summary_row(ws, cache, ym, "거래건수")
    put_summary_line(ws, cache, r1, counts)
    header = cache.header()
    if header:
        format_row_bold(ws, r1, 3, len(header))
    log(f"[summary] {ym} 거래건수 -> row={r1}")

    # 중앙값 / 평균가
    r2 = ensure_summary_row(ws, cache, ym, "중앙값(단위:억)")
    put_summary_line(ws, cache, r2, med)
    log(f"[summary] {ym} 중앙값 -> row={r2}")

    r3 = ensure_summary_row(ws, cache, ym, "평균가(단위:억)")
    put_summary_line(ws, cache, r3, mean)
    log(f"[summary] {ym} 평균가 -> row={r3}")

    # 전월대비 건수증감 (+파랑 / -빨강)
    r4 = ensure_summary_row(ws, cache, ym, "전월대비 건수증감")
    diffs = {}
    if prev_counts:
        for k in SUMMARY_COLS:
            cur = int(counts.get(k, 0) or 0)
            prv = int(prev_counts.get(k, 0) or 0)
            d = cur - prv
            diffs[k] = (f"+{d}" if d>0 else (str(d) if d<0 else "0"))
    else:
        diffs = {k:"" for k in SUMMARY_COLS}
    put_summary_line(ws, cache, r4, diffs)

    # 일괄 색칠
    hmap = {h: i+1 for i,h in enumerate(cache.header())}
    jobs = []
    for k,v in diffs.items():
        if k in hmap and v not in ("", "0"):
            col = hmap[k]
            if v.startswith("+"):
                jobs.append((col, (0.0, 0.35, 1.0)))  # 파랑
            elif v.startswith("-"):
                jobs.append((col, (1.0, 0.0, 0.0)))  # 빨강
    color_cells_batch(ws, r4, jobs)
    log(f"[summary] {ym} 전월대비 -> row={r4}")

    # 예상건수(placeholder)
    r5 = ensure_summary_row(ws, cache, ym, "예상건수")
    log(f"[summary] {ym} 예상건수 -> row={r5}")

# ===================== 압구정동 원본 + 변경 로그 =====================
def ensure_apgu_sheet(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, APGU_SHEET)
    if ws: return ws
    return _retry(sh.add_worksheet, title=APGU_SHEET, rows=4000, cols=80)

def number_or_blank(v):
    if v is None: return ""
    if isinstance(v, float) and (pd.isna(v) or math.isnan(v)): return ""
    return v

def make_row_key(d: dict) -> str:
    parts = [
        d.get("계약년",""), d.get("계약월",""), d.get("계약일",""),
        d.get("광역",""), d.get("구",""), d.get("법정동",""),
        d.get("단지명",""), d.get("전용면적(㎡)",""), d.get("층",""),
        d.get("거래금액(만원)","")
    ]
    return "|".join(str(x).strip() for x in parts)

def append_change_log(ws: gspread.Worksheet, added_rows: List[List], removed_rows: List[List], header: List[str]):
    vals = _retry(ws.get_all_values) or []
    start = len(vals) + 1
    now_label = fmt_date_kor(datetime.now())

    idx = {h:i for i,h in enumerate(header)}
    def to_log_row(kind, row):
        def take(c): return row[idx[c]] if c in idx and idx[c] < len(row) else ""
        return [kind, now_label, take("계약년"),take("계약월"),take("계약일"),take("단지명"),
                take("전용면적(㎡)"),take("동"),take("층"),take("거래금액(만원)")]

    rows = []
    for r in added_rows:  rows.append(to_log_row("(신규)", r))
    for r in removed_rows: rows.append(to_log_row("(삭제)", r))
    if not rows: return

    need = start + len(rows)
    if need > ws.row_count:
        _retry(ws.add_rows, need - ws.row_count)

    end = start + len(rows)
    _retry(ws.update, [APGU_CHANGE_HEADER], f"A{start}:J{start}")
    _retry(ws.update, rows, f"A{start+1}:J{end}")

    req = {
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": start-1, "endRowIndex": end, "startColumnIndex": 0, "endColumnIndex": 10},
            "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red":1.0,"green":0.0,"blue":0.0}}}},
            "fields": "userEnteredFormat.textFormat.foregroundColor"
        }
    }
    batch_format(ws, [req])

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
        _retry(ws.update, [header], "A1")
        vals = [header]
    else:
        header = vals[0]
        if "기록일" not in header:
            header = header + ["기록일"]
            _retry(ws.update, [header], "A1")
            vals = _retry(ws.get_all_values) or [header]
        # 파일에만 있는 새 컬럼 등장 시 헤더 확장
        union = list(dict.fromkeys(header + [c for c in df.columns if c not in header]))
        if union != header:
            header = union
            _retry(ws.update, [header], "A1")
            vals = _retry(ws.get_all_values) or [header]

    existing_rows = vals[1:]
    idx_map = {h:i for i,h in enumerate(header)}
    def row_to_dict(row):
        return {k:(row[i] if i<len(row) else "") for k,i in idx_map.items()}

    # 기존 키
    existing_keys = set()
    for r in existing_rows:
        existing_keys.add(make_row_key(row_to_dict(r)))

    today_label = fmt_date_kor(datetime.now())
    # 신규 추가 (중복 방지)
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

    # 삭제 감지(파일 기준에 없으면 삭제로 기록)
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
        start = len(vals)+1
        end = start + len(new_records) - 1
        rng = f"A{start}:{a1_col(len(header))}{end}"
        _retry(ws.update, new_records, rng)
        log(f"[압구정동] appended {len(new_records)} rows")
    else:
        log("[압구정동] no new rows to append")

    append_change_log(ws, new_records, removed, header)

# ===================== 메인 =====================
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts-dir", default=ART_DIR_DEFAULT)
    parser.add_argument("--sheet-id", default=os.environ.get("SHEET_ID",""))
    parser.add_argument("--sa", default=os.environ.get("SA_PATH","sa.json"))
    args = parser.parse_args()

    # 로그 초기화
    _ensure_log_dir()
    (LOG_DIR/"latest.log").write_text("", encoding="utf-8")
    log_block("main")
    log(f"artifacts_dir={args.artifacts_dir}")

    sh = open_sheet(args.sheet_id, args.sa)

    # 스캔
    files = sorted(Path(args.artifacts_dir).rglob("전국 *.xlsx"))
    log(f"[collect] found {len(files)} xlsx files")

    month_cache: Dict[Tuple[int,int], Dict] = {}
    apgu_all: List[pd.DataFrame] = []
    write_date = datetime.now()

    for f in files:
        meta = parse_file_meta(f)
        if not meta: 
            continue
        y, m = meta
        nat_title = f"전국 {y%100:02d}년 {m}월"
        se_title  = f"서울 {y%100:02d}년 {m}월"
        log(f"[read] {f.name}")

        df = read_month_df(f)
        counts, med, mean = agg_all_stats(df)
        month_cache[(y,m)] = {"counts":counts, "med":med, "mean":mean}

        # 전국/서울 탭 쓰기(있을 때만)
        ws_nat = fuzzy_ws(sh, nat_title)
        if ws_nat:
            cache_nat = WorksheetCache(ws_nat)
            header_nat = cache_nat.header()
            nat_cols = [h for h in header_nat if h and h!="날짜"]
            # 전국 탭: 총합계 = 전국(또는 합계 fallback)
            write_counts_to_month_sheet(ws_nat, cache_nat, write_date, nat_cols, counts,
                                        sum_col_name="총합계", prefer_total_key="전국")
            log(f"[전국] {ws_nat.title} -> {fmt_date_kor(write_date)}")

        ws_se = fuzzy_ws(sh, se_title)
        if ws_se:
            cache_se = WorksheetCache(ws_se)
            header_se = cache_se.header()
            se_cols = [h for h in header_se if h and h!="날짜"]
            # 서울 탭: 총합계 = 서울(또는 합계 fallback)
            write_counts_to_month_sheet(ws_se, cache_se, write_date, se_cols, counts,
                                        sum_col_name="총합계", prefer_total_key="서울")
            log(f"[서울] {ws_se.title} -> {fmt_date_kor(write_date)}")

        # 압구정동 원본 누적
        ap = df[(df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")].copy()
        if not ap.empty:
            apgu_all.append(ap)

    # 거래요약: 월 전체 한 번에(전월대비 포함)
    ws_sum = fuzzy_ws(sh, SUMMARY_SHEET_NAME)
    if ws_sum and month_cache:
        cache_sum = WorksheetCache(ws_sum)
        ensure_summary_header(ws_sum, cache_sum)
        for (yy, mm) in sorted(month_cache.keys()):
            cur = month_cache[(yy, mm)]
            py, pm = prev_month(yy, mm)
            prv = month_cache.get((py, pm))
            prev_counts = prv["counts"] if prv else None
            write_month_summary(ws_sum, cache_sum, yy, mm, cur["counts"], cur["med"], cur["mean"], prev_counts)
            time.sleep(0.2)  # API 분산

    # 압구정동: 중복 없이 원본 추가 + 변동 로그
    if apgu_all:
        ws_ap = ensure_apgu_sheet(sh)
        all_df = pd.concat(apgu_all, ignore_index=True)
        upsert_apgu_raw(ws_ap, all_df)

    log("[main] done")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        try:
            _ensure_log_dir()
            with open(LOG_DIR/"latest.log","a",encoding="utf-8") as f:
                f.write(f"[ERROR] {repr(e)}\n")
        except Exception:
            pass
        print(f"[ERROR] {repr(e)}", file=sys.stderr)
        raise
