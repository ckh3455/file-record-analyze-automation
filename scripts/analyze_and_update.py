# scripts/analyze_and_update.py
# -*- coding: utf-8 -*-

import os, re, json, time, math, unicodedata
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, Dict, List

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ===================== 상수/설정 =====================
LOG_DIR = Path("analyze_report")
ART_DIR_DEFAULT = "artifacts"

SUMMARY_SHEET_NAME = "거래요약"

SUMMARY_COLS = [
    "전국","서울","강남구","압구정동","경기도","인천광역시","세종시","서초구","송파구","용산구",
    "강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구","강서구",
    "강북구","관악구","구로구","금천구","도봉구","동대문구","서대문구","성북구","은평구",
    "중구","중랑구","부산","대구","광주","대전","강원도","경남","경북","전남","전북","충남","충북","제주"
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

NEEDED_COLS = ["광역","구","법정동","도로명","번지","본번","부번","단지명","전용면적(㎡)",
               "계약년","계약월","계약일","거래금액(만원)","동","층"]

# ===================== 로깅 =====================
def fmt_now(): return datetime.now().strftime("%H:%M:%S")
def fmt_date_kor(dt: datetime) -> str: return f"{dt.year}. {dt.month}. {dt.day}"

def _ensure_log_dir():
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        if LOG_DIR.exists() and LOG_DIR.is_file():
            LOG_DIR.unlink()
            LOG_DIR.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    _ensure_log_dir()
    line = f"[{fmt_now()}] {msg}\n"
    p = LOG_DIR / "latest.log"
    if p.exists():
        p.write_text(p.read_text(encoding="utf-8") + line, encoding="utf-8")
    else:
        p.write_text(line, encoding="utf-8")
    print(line, end="")

def log_block(title: str):
    log(f"[{title.upper()}]")

# ===================== gspread 헬퍼/리트라이 =====================
def _retry(fn, *a, **kw):
    # Read/Write 공통 지수 백오프
    delays = [1.0, 2.0, 4.0, 7.0, 12.0, 18.0]
    last = None
    for i, d in enumerate(delays):
        try:
            return fn(*a, **kw)
        except Exception as e:
            last = e
            if i == len(delays)-1:
                raise
            time.sleep(d)
    raise last

def _values_batch_update_compat(ws, body: dict):
    # gspread 6.x / 5.x 호환
    try:
        return _retry(ws.spreadsheet.values_batch_update, body)   # 6.x
    except TypeError:
        return _retry(ws.client.values_batch_update, ws.spreadsheet.id, body)  # 5.x

def batch_values_update(ws, payload: List[Dict]):
    body = {
        "valueInputOption": "USER_ENTERED",
        "data": [{"range": p["range"], "values": p["values"]} for p in payload],
    }
    _values_batch_update_compat(ws, body)

def batch_format(ws, requests: List[dict]):
    if not requests: return
    _retry(ws.spreadsheet.batch_update, {"requests": requests})

# ===================== 캐시 유틸 =====================
class WorksheetCache:
    """한 실행 내에서 Worksheet의 값을 한 번만 읽고 재사용."""
    def __init__(self, ws: gspread.Worksheet):
        self.ws = ws
        self._values: Optional[List[List[str]]] = None
        self._header_map: Optional[Dict[str,int]] = None

    def values(self) -> List[List[str]]:
        if self._values is None:
            self._values = _retry(self.ws.get_all_values) or []
        return self._values

    def header(self) -> List[str]:
        vals = self.values()
        return vals[0] if vals else []

    def header_map(self) -> Dict[str,int]:
        if self._header_map is None:
            hdr = self.header()
            self._header_map = {h: i+1 for i,h in enumerate(hdr)}
        return self._header_map

    def row_count(self) -> int:
        return len(self.values())

    def invalidate(self):
        self._values = None
        self._header_map = None

# ===================== 시트 유틸 =====================
def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def fuzzy_ws(sh: gspread.Spreadsheet, wanted: str) -> Optional[gspread.Worksheet]:
    norm = re.sub(r"\s+", "", wanted)
    for ws in sh.worksheets():
        if re.sub(r"\s+", "", ws.title) == norm:
            return ws
    for ws in sh.worksheets():
        if norm in re.sub(r"\s+", "", ws.title):
            return ws
    return None

def ensure_ws_exists(sh: gspread.Spreadsheet, title: str) -> Optional[gspread.Worksheet]:
    ws = fuzzy_ws(sh, title)
    if not ws:
        log(f"[ws] not found: '{title}' (skip)")
        return None
    return ws

def detect_date_col_from_header(header: List[str]) -> int:
    for i, v in enumerate(header, start=1):
        if str(v).strip() == "날짜":
            return i
    return 1

def normalize_sheet_date_cell(v: str):
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

def find_row_by_date_cached(cache: WorksheetCache, date_col: int, target: datetime) -> Optional[int]:
    vals = cache.values()
    target_key = f"{target.year:04d}-{target.month:02d}-{target.day:02d}"
    for idx, row in enumerate(vals[1:], start=2):
        cell = row[date_col-1] if date_col-1 < len(row) else ""
        key = normalize_sheet_date_cell(cell)
        if key == target_key:
            return idx
    return None

def write_date_cell(ws: gspread.Worksheet, row_idx: int, date_obj: datetime):
    label = fmt_date_kor(date_obj)
    _retry(ws.update, [[label]], f"A{row_idx}:A{row_idx}", value_input_option="USER_ENTERED")

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

# ===================== 문자열 정규화(중복행 방지) =====================
def _norm_txt(s: str) -> str:
    if s is None: return ""
    t = unicodedata.normalize("NFC", str(s))
    t = t.replace("\u00A0", " ")
    t = re.sub(r"\s+", "", t)
    return t

# ===================== 엑셀 읽기/집계 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data")
    df = df.loc[:, ~df.columns.duplicated()].copy()
    for c in NEEDED_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    return df.copy()

def eok_series(ser) -> pd.Series:
    try:
        s = pd.to_numeric(ser, errors="coerce")
    except Exception:
        return pd.Series([], dtype=float)
    s = s.dropna()
    if s.empty: return pd.Series([], dtype=float)
    return s / 10000.0

def round2(v):
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

# ===================== 전국/서울 탭 기록 =====================
def write_counts_to_sheet(ws: gspread.Worksheet, cache: WorksheetCache, write_date: datetime,
                          header_names: list[str], series_map: dict, sum_col_name: str = "총합계"):
    header = cache.header()
    if not header:
        return
    date_col = detect_date_col_from_header(header)
    row = find_row_by_date_cached(cache, date_col, write_date)
    if row is None:
        row = cache.row_count() + 1
        write_date_cell(ws, row, write_date)
        cache.invalidate()

    hmap = {h: i+1 for i,h in enumerate(header)}
    payload = []
    total = 0
    for name in header_names:
        if name in hmap:
            col = hmap[name]
            v = int(series_map.get(name, 0) or 0)
            payload.append({"range": f"{a1_col(col)}{row}", "values": [[v]]})
            if name != "전국":
                total += v
    if sum_col_name in hmap:
        payload.append({"range": f"{a1_col(hmap[sum_col_name])}{row}",
                        "values": [[series_map.get('전국', total)]]})
    if payload:
        batch_values_update(ws, payload)
        cache.invalidate()

# ===================== 거래요약 탭 =====================
def parse_file_meta(p: Path):
    # "전국 2507_250927.xlsx" → (2025, 7)
    name = p.stem
    m = re.search(r"\s(\d{2})(\d{1,2})_", name)
    if not m: return None
    yy, mm = int(m.group(1)), int(m.group(2))
    return 2000+yy, mm

def build_summary_index(cache: WorksheetCache) -> Dict[Tuple[str,str], int]:
    """정규화된 (년월,구분) -> '가장 위에 있는' rowIdx"""
    idx: Dict[Tuple[str,str], int] = {}
    vals = cache.values()
    for i, r in enumerate(vals[1:], start=2):
        ym_raw = (r[0] if len(r)>0 else "").strip()
        lb_raw = (r[1] if len(r)>1 else "").strip()
        ym = _norm_txt(ym_raw)
        lb = _norm_txt(lb_raw)
        if ym and lb and (ym, lb) not in idx:
            idx[(ym, lb)] = i
    return idx

def ensure_summary_row(ws: gspread.Worksheet, cache: WorksheetCache,
                       ym_label: str, what: str) -> int:
    vals = cache.values()
    if not vals:
        _retry(ws.update, [["년월","구분"] + SUMMARY_COLS], "A1")
        cache.invalidate()
        vals = cache.values()

    ym_key = _norm_txt(ym_label)
    what_key = _norm_txt(what)

    idx_map = build_summary_index(cache)
    if (ym_key, what_key) in idx_map:
        return idx_map[(ym_key, what_key)]

    # 혹시 중복행이 있다면 정리(맨 위만 남김)
    dup_rows = []
    for i, r in enumerate(cache.values()[1:], start=2):
        if _norm_txt(r[0] if len(r)>0 else "") == ym_key and _norm_txt(r[1] if len(r)>1 else "") == what_key:
            dup_rows.append(i)
    if len(dup_rows) > 1:
        reqs = []
        for rr in sorted(dup_rows[1:], reverse=True):
            reqs.append({"deleteDimension": {
                "range": {"sheetId": ws.id, "dimension": "ROWS", "startIndex": rr-1, "endIndex": rr}
            }})
        if reqs:
            batch_format(ws, reqs)
            cache.invalidate()
        idx_map = build_summary_index(cache)
        if (ym_key, what_key) in idx_map:
            return idx_map[(ym_key, what_key)]

    row = cache.row_count() + 1
    _retry(ws.update, [[ym_label, what]], f"A{row}:B{row}", value_input_option="USER_ENTERED")
    cache.invalidate()
    return row

def put_summary_line(ws: gspread.Worksheet, cache: WorksheetCache,
                     row_idx: int, line_map: dict):
    header = cache.header()
    hmap = {h: i+1 for i,h in enumerate(header)}
    payload = []
    for col_name in SUMMARY_COLS:
        if col_name in hmap:
            col = hmap[col_name]
            payload.append({"range": f"{a1_col(col)}{row_idx}",
                            "values": [[line_map.get(col_name, "")]]})
    if payload:
        batch_values_update(ws, payload)
        cache.invalidate()

def write_month_summary(ws: gspread.Worksheet, cache: WorksheetCache,
                        y: int, m: int, counts: dict, med: dict, mean: dict,
                        prev_counts: Optional[dict]):
    ym_label = f"{str(y%100).zfill(2)}/{m}"

    r1 = ensure_summary_row(ws, cache, ym_label, "거래건수")
    put_summary_line(ws, cache, r1, counts)
    header = cache.header()
    if header:
        format_row_bold(ws, r1, 3, len(header))
    log(f"[summary] {ym_label} 거래건수 -> row={r1}")

    r2 = ensure_summary_row(ws, cache, ym_label, "중앙값(단위:억)")
    put_summary_line(ws, cache, r2, med)
    log(f"[summary] {ym_label} 중앙값 -> row={r2}")

    r3 = ensure_summary_row(ws, cache, ym_label, "평균가(단위:억)")
    put_summary_line(ws, cache, r3, mean)
    log(f"[summary] {ym_label} 평균가 -> row={r3}")

    r4 = ensure_summary_row(ws, cache, ym_label, "전월대비 건수증감")
    diff_line = {}
    if prev_counts:
        for k in SUMMARY_COLS:
            cur = int(counts.get(k, 0) or 0)
            prv = int(prev_counts.get(k, 0) or 0)
            delta = cur - prv
            diff_line[k] = f"+{delta}" if delta > 0 else (str(delta) if delta < 0 else "0")
    else:
        for k in SUMMARY_COLS:
            diff_line[k] = ""
    put_summary_line(ws, cache, r4, diff_line)

    header_map = {h: i+1 for i,h in enumerate(cache.header())}
    jobs = []
    for k,v in diff_line.items():
        if k in header_map and v not in ("", "0"):
            col = header_map[k]
            if v.startswith("+"):
                jobs.append((col, (0.0, 0.35, 1.0)))  # 파랑
            elif v.startswith("-"):
                jobs.append((col, (1.0, 0.0, 0.0)))  # 빨강
    color_cells_batch(ws, r4, jobs)
    log(f"[summary] {ym_label} 전월대비 -> row={r4}")

    r5 = ensure_summary_row(ws, cache, ym_label, "예상건수")
    log(f"[summary] {ym_label} 예상건수 -> row={r5}")

# ===================== 압구정동 탭 (원본 누적 + 변경 로그) =====================
def ensure_apgu_sheet(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, APGU_SHEET)
    if ws:
        return ws
    return _retry(sh.add_worksheet, title=APGU_SHEET, rows=4000, cols=80)

def make_row_key(d: dict) -> str:
    parts = [
        d.get("계약년",""), d.get("계약월",""), d.get("계약일",""),
        d.get("광역",""), d.get("구",""), d.get("법정동",""),
        d.get("단지명",""), d.get("전용면적(㎡)",""), d.get("층",""),
        d.get("거래금액(만원)","")
    ]
    return "|".join(str(x).strip() for x in parts)

def number_or_blank(v):
    if v is None: return ""
    if isinstance(v, float) and (pd.isna(v) or math.isnan(v)): return ""
    return v

def append_change_log(ws: gspread.Worksheet, added_rows: list[list], removed_rows: list[list], header: list[str]):
    # 맨 아래에 빨간 글자로 변동 기록
    all_vals = _retry(ws.get_all_values) or []
    start = len(all_vals) + 1
    now_label = fmt_date_kor(datetime.now())

    def to_log_row(kind, row):
        idx = {h:i for i,h in enumerate(header)}
        def take(col):
            return row[idx[col]] if col in idx and idx[col] < len(row) else ""
        return [
            kind, now_label,
            take("계약년"), take("계약월"), take("계약일"), take("단지명"),
            take("전용면적(㎡)"), take("동"), take("층"), take("거래금액(만원)")
        ]

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
            "range": {
                "sheetId": ws.id,
                "startRowIndex": start-1,
                "endRowIndex": end,
                "startColumnIndex": 0,
                "endColumnIndex": 10
            },
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
        union = list(dict.fromkeys(header + [c for c in df.columns if c not in header]))
        if union != header:
            header = union
            _retry(ws.update, [header], "A1")
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
        start_row = len(vals)+1
        end_row = start_row + len(new_records) - 1
        rng = f"A{start_row}:{a1_col(len(header))}{end_row}"
        _retry(ws.update, new_records, rng)
        log(f"[압구정동] appended {len(new_records)} rows")
    else:
        log("[압구정동] no new rows to append")

    append_change_log(ws, new_records, removed, header)

# ===================== 메인 =====================
def main():
    log_block("main")

    artifacts_dir = os.environ.get("ARTIFACTS_DIR", ART_DIR_DEFAULT)
    sheet_id = os.environ.get("SHEET_ID", "").strip()
    sa_json_env = os.environ.get("SA_JSON", "").strip()
    sa_path = os.environ.get("SA_PATH", "sa.json")

    log(f"artifacts_dir={artifacts_dir}")

    # 인증
    if sa_json_env:
        creds = Credentials.from_service_account_info(
            json.loads(sa_json_env),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
    else:
        if not Path(sa_path).exists():
            raise RuntimeError("service account not provided")
        creds = Credentials.from_service_account_file(
            sa_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, sheet_id)
    log("[gspread] spreadsheet opened")

    files = sorted(Path(artifacts_dir).rglob("*.xlsx"))
    log(f"[collect] found {len(files)} xlsx files")

    month_cache: Dict[Tuple[int,int], Dict] = {}
    apgu_all: List[pd.DataFrame] = []
    write_date = datetime.now()

    for f in files:
        meta = parse_file_meta(f)
        if not meta:
            continue
        y, m = meta
        log(f"[read] {f.name}")
        df = read_month_df(f)

        counts, med, mean = agg_all_stats(df)
        month_cache[(y, m)] = {"counts": counts, "med": med, "mean": mean}

        nat_title = f"전국 {y%100:02d}년 {m}월"
        se_title  = f"서울 {y%100:02d}년 {m}월"

        ws_nat = ensure_ws_exists(sh, nat_title)
        ws_se  = ensure_ws_exists(sh, se_title)

        if ws_nat:
            cache_nat = WorksheetCache(ws_nat)
            header_nat = cache_nat.header()
            nat_cols = [c for c in header_nat if (c in PROV_MAP.values()) or c=="전국"]
            write_counts_to_sheet(ws_nat, cache_nat, write_date, nat_cols, counts, sum_col_name="총합계")
            log(f"[전국] {ws_nat.title} -> {fmt_date_kor(write_date)}")

        if ws_se:
            cache_se = WorksheetCache(ws_se)
            header_se = cache_se.header()
            se_cols = [c for c in header_se if (c in SUMMARY_COLS) or c=="서울"]
            write_counts_to_sheet(ws_se, cache_se, write_date, se_cols, counts, sum_col_name="총합계")
            log(f"[서울] {ws_se.title} -> {fmt_date_kor(write_date)}")

        ap = df[(df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")].copy()
        if not ap.empty:
            apgu_all.append(ap)

    # 거래요약
    ws_sum = ensure_ws_exists(sh, SUMMARY_SHEET_NAME)
    if ws_sum:
        cache_sum = WorksheetCache(ws_sum)
        if not cache_sum.values():
            _retry(ws_sum.update, [["년월","구분"] + SUMMARY_COLS], "A1")
            cache_sum.invalidate()
        for (y,m) in sorted(month_cache.keys()):
            cur = month_cache[(y,m)]
            prev = month_cache.get((y, m-1)) if m>1 else month_cache.get((y-1, 12))
            prev_counts = prev["counts"] if prev else None
            write_month_summary(ws_sum, cache_sum, y, m, cur["counts"], cur["med"], cur["mean"], prev_counts)
            time.sleep(0.5)  # API 부하 분산

    # 압구정동
    if apgu_all:
        ws_ap = ensure_apgu_sheet(sh)
        all_df = pd.concat(apgu_all, ignore_index=True)
        upsert_apgu_raw(ws_ap, all_df)

    log("[main] done")

if __name__ == "__main__":
    main()
