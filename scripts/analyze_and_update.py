# -*- coding: utf-8 -*-
from __future__ import annotations

"""
analyze_and_update.py (최종본: Drive -> Local -> Sheets)

✅ 변경 요약(중요)
- 아티팩트(깃허브 액션 artifact)에서 파일을 찾는 로직을 완전히 제거
- Google Drive의 특정 폴더(DRIVE_FOLDER_ID)에서 "아파트 YYYYMM.xlsx" 파일만 검색/다운로드
- 최신 12개월만 처리 (과거→현재 순으로 기록)
- 시트 업데이트 구조(월탭/거래요약/압구정동)는 기존과 동일
- pandas NaN/Inf로 인한 "Out of range float values are not JSON compliant" 방지:
  -> 구글시트에 쓰기 직전 모든 값을 안전 문자열로 변환

환경변수(Workflow env):
- SHEET_ID: 대상 구글시트 ID
- SA_JSON 또는 SA_PATH: 서비스계정 JSON(문자열 또는 파일경로)
- DRIVE_FOLDER_ID: "아파트" 폴더 ID (권장/필수)
- DRIVE_SUPPORTS_ALL_DRIVES: 공유드라이브면 "true" 권장
- DOWNLOAD_DIR: 다운로드 받을 로컬 폴더 (기본 drive_files)
- MAX_SCAN_ROWS: 날짜 스캔 범위(기본 900)
"""

import os, re, json, time, random, math
from io import BytesIO
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional, Union

import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ===================== 설정 =====================
LOG_DIR = Path("analyze_report")
SUMMARY_SHEET_NAME = "거래요약"
MAX_SCAN_ROWS = int(os.environ.get("MAX_SCAN_ROWS", "900"))

DOWNLOAD_DIR = Path(os.environ.get("DOWNLOAD_DIR", "drive_files"))

SUMMARY_COLS = [
    "전국", "서울", "서울특별시",
    "강남구", "압구정동",
    "경기도", "인천광역시", "세종특별자치시", "울산광역시",
    "서초구", "송파구", "용산구", "강동구", "성동구", "마포구", "양천구", "동작구", "영등포구", "종로구", "광진구",
    "강서구", "강북구", "관악구", "구로구", "금천구", "도봉구", "노원구",
    "동대문구", "서대문구", "성북구", "은평구", "중구", "중랑구",
    "부산광역시", "대구광역시", "광주광역시", "대전광역시",
    "강원특별자치도", "경상남도", "경상북도", "전라남도", "전북특별자치도", "충청남도", "충청북도", "제주특별자치도"
]

SEOUL_REGIONS = [
    "강남구","강동구","강북구","강서구","관악구","광진구","구로구","금천구","노원구","도봉구",
    "동대문구","동작구","마포구","서대문구","서초구","성동구","성북구","송파구","양천구","영등포구",
    "용산구","은평구","종로구","중구","중랑구","총합계"
]
NATION_REGIONS = [
    "강원특별자치도","경기도","경상남도","경상북도","광주광역시","대구광역시","대전광역시","부산광역시",
    "서울특별시","세종특별자치시","울산광역시","인천광역시","전라남도","전북특별자치도","제주특별자치도",
    "충청남도","충청북도","총합계"
]

# ===================== 로깅/리트라이 =====================
def _ensure_logdir():
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

_ensure_logdir()
RUN_LOG = LOG_DIR / "latest.log"

def log(msg: str):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    try:
        with RUN_LOG.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

_LAST = 0.0
def _throttle(sec: float = 0.60):
    import time as _t
    global _LAST
    now = _t.time()
    if now - _LAST < sec:
        _t.sleep(sec - (now - _LAST))
    _LAST = _t.time()

def _retry(fn, *a, **kw):
    base = 0.8
    for i in range(7):
        try:
            _throttle()
            return fn(*a, **kw)
        except APIError as e:
            s = str(e)
            if any(x in s for x in ("429", "500", "502", "503")):
                time.sleep(base * (2 ** i) + random.uniform(0, 0.25))
                continue
            raise

# ===================== 유틸 =====================
_DATE_PATS = [
    re.compile(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})"),
    re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})"),
    re.compile(r"(\d{4})/(\d{1,2})/(\d{1,2})"),
]

def parse_any_date(x) -> Optional[date]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    s = str(x).strip()
    if not s:
        return None
    for pat in _DATE_PATS:
        m = pat.search(s)
        if m:
            try:
                return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except Exception:
                return None
    return None

def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _safe_cell(v) -> str:
    """Google API JSON에서 NaN/Inf 터지는 것 방지."""
    if v is None:
        return ""
    if isinstance(v, (np.floating, float)):
        if math.isnan(float(v)) or math.isinf(float(v)):
            return ""
    if isinstance(v, (np.integer, int)):
        return str(int(v))
    # Timestamp 등
    return str(v)

def _safe_values_2d(values: List[List[object]]) -> List[List[str]]:
    return [[_safe_cell(c) for c in row] for row in values]

# ===================== gspread wrappers =====================
def ws_update(ws: gspread.Worksheet, values, range_name: str):
    return _retry(ws.update, _safe_values_2d(values), range_name)

def ws_clear(ws: gspread.Worksheet):
    return _retry(ws.clear)

def values_batch_update(ws: gspread.Worksheet, data: List[Dict]):
    # values는 모두 safe string으로 강제
    safe_data = []
    for d in data:
        vals = d.get("values", [])
        safe_data.append({"range": d["range"], "values": _safe_values_2d(vals)})
    body = {"valueInputOption": "USER_ENTERED", "data": safe_data}
    return _retry(ws.spreadsheet.values_batch_update, body=body)

# ===================== sheet helpers =====================
def _norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "").replace("\u3000", "").strip())

def fuzzy_ws(sh: gspread.Spreadsheet, wanted: str) -> Optional[gspread.Worksheet]:
    tgt = _norm(wanted)
    for ws in sh.worksheets():
        if _norm(ws.title) == tgt:
            return ws
    return None

def get_or_create_ws(sh: gspread.Spreadsheet, title: str, rows: int = 100, cols: int = 20) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, title)
    if ws is None:
        ws = _retry(sh.add_worksheet, title=title, rows=rows, cols=cols)
        log(f"[ws] created: {title}")
    return ws

# ===================== 월 타이틀/ym =====================
def ym_from_apartment_filename(fname: str):
    """
    Drive 파일명 예: '아파트 202510.xlsx'
    return: ('전국 2025년 10월', '서울 2025년 10월', '25/10')
    """
    s = str(fname or "")
    m = re.search(r"(20\d{2})(\d{2})", s)
    if not m:
        return None, None, None
    y, mm = int(m.group(1)), int(m.group(2))
    if not (1 <= mm <= 12):
        return None, None, None
    return f"전국 {y}년 {mm}월", f"서울 {y}년 {mm}월", f"{y%100:02d}/{mm:02d}"

# ===================== 날짜 행 찾기(고정범위 스캔) =====================
def find_or_append_date_row(ws: gspread.Worksheet, date_label: Union[str, date, datetime]) -> int:
    target = parse_any_date(date_label) or parse_any_date(str(date_label))
    if not target:
        return 2

    rng = f"A2:A{MAX_SCAN_ROWS}"
    col = _retry(ws.get, rng) or []
    first_empty = None
    for offset, row in enumerate(col, start=2):
        v = row[0] if row else ""
        if not v:
            if first_empty is None:
                first_empty = offset
            continue
        d = parse_any_date(v)
        if d and d == target:
            return offset

    if first_empty is not None:
        return first_empty
    return min(MAX_SCAN_ROWS + 1, 5000)

def ensure_month_ws(sh: gspread.Spreadsheet, title: str, level: str) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, title)
    if ws is not None:
        return ws
    ws = get_or_create_ws(sh, title, rows=800, cols=40)
    header = ["날짜"] + (NATION_REGIONS if level == "전국" else SEOUL_REGIONS)
    ws_update(ws, [header], "A1")
    log(f"[ws] created from scratch: {title}")
    return ws

def write_month_sheet(ws: gspread.Worksheet, date_iso: str, header: List[str], values_by_colname: Dict[str, int]):
    hmap = {str(h).strip(): idx + 1 for idx, h in enumerate(header) if str(h).strip()}
    row_idx = find_or_append_date_row(ws, date_iso)

    sheet_prefix = f"'{ws.title}'!"
    payload = [{"range": f"{sheet_prefix}A{row_idx}", "values": [[date_iso]]}]
    for col_name, val in values_by_colname.items():
        if col_name in hmap:
            c = hmap[col_name]
            payload.append({"range": f"{sheet_prefix}{a1_col(c)}{row_idx}", "values": [[int(val)]]})
    values_batch_update(ws, payload)
    log(f"[ws] {ws.title} -> {date_iso} row={row_idx} wrote_cells={len(payload)}")

# ===================== 데이터 읽기/집계 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    # 기존 processed xlsx는 sheet_name="data"였음. Drive 파일이 다르면 fallback.
    try:
        df = pd.read_excel(path, sheet_name="data", dtype=str)
    except Exception:
        df = pd.read_excel(path, sheet_name=0, dtype=str)
    df = df.fillna("")
    for c in ["계약년", "계약월", "계약일", "거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def eok_series(ser) -> pd.Series:
    s = pd.Series(ser)
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return pd.Series([], dtype=float)
    return s / 10000.0

def round2(v) -> str:
    try:
        return f"{float(v):.2f}"
    except Exception:
        return ""

def _strip_col(df: pd.DataFrame, col: str):
    if col in df.columns:
        df[col] = df[col].astype(str).map(lambda x: str(x).replace("\u3000"," ").strip())
    return df

def agg_all_stats(df: pd.DataFrame):
    counts = {col: 0 for col in SUMMARY_COLS}
    med = {col: "" for col in SUMMARY_COLS}
    mean = {col: "" for col in SUMMARY_COLS}

    if df is None or df.empty:
        return counts, med, mean

    df = df.copy()
    _strip_col(df, "광역")
    _strip_col(df, "구")
    _strip_col(df, "법정동")

    counts["전국"] = int(len(df))
    all_eok = eok_series(df.get("거래금액(만원)", []))
    if not all_eok.empty:
        med["전국"] = round2(all_eok.median())
        mean["전국"] = round2(all_eok.mean())

    if "광역" in df.columns:
        for prov, sub in df.groupby("광역"):
            prov = str(prov).strip()
            if prov in counts:
                counts[prov] += int(len(sub))
                se = eok_series(sub.get("거래금액(만원)", []))
                if not se.empty:
                    med[prov] = round2(se.median())
                    mean[prov] = round2(se.mean())

    seoul = df[df.get("광역", "") == "서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul) > 0:
        se = eok_series(seoul.get("거래금액(만원)", []))
        if not se.empty:
            med["서울"] = round2(se.median())
            mean["서울"] = round2(se.mean())
        if "구" in seoul.columns:
            for gu, sub in seoul.groupby("구"):
                gu = str(gu).strip()
                if gu in counts:
                    counts[gu] += int(len(sub))
                    se2 = eok_series(sub.get("거래금액(만원)", []))
                    if not se2.empty:
                        med[gu] = round2(se2.median())
                        mean[gu] = round2(se2.mean())

    ap = seoul[seoul.get("법정동", "") == "압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap) > 0:
        s = eok_series(ap.get("거래금액(만원)", []))
        if not s.empty:
            med["압구정동"] = round2(s.median())
            mean["압구정동"] = round2(s.mean())

    return counts, med, mean

# ===================== Google 인증 =====================
def load_creds():
    sa_json = os.environ.get("SA_JSON", "").strip()
    sa_path = os.environ.get("SA_PATH", "").strip()

    if sa_json:
        info = json.loads(sa_json)
    elif sa_path:
        info = json.loads(Path(sa_path).read_text(encoding="utf-8"))
    else:
        raise RuntimeError("SA_JSON 또는 SA_PATH 환경변수가 필요합니다.")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return Credentials.from_service_account_info(info, scopes=scopes)

# ===================== Drive: 폴더에서 아파트 YYYYMM.xlsx 찾기/다운로드 =====================
def _supports_all_drives() -> bool:
    v = os.environ.get("DRIVE_SUPPORTS_ALL_DRIVES", "").strip().lower()
    return v in ("1","true","yes","y","on")

def list_drive_files(drive, folder_id: str) -> List[dict]:
    supports = _supports_all_drives()
    q = (
        f"'{folder_id}' in parents and trashed=false and "
        "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    )
    files = []
    page_token = None
    while True:
        resp = drive.files().list(
            q=q,
            spaces="drive",
            fields="nextPageToken, files(id,name,modifiedTime,size,parents)",
            pageToken=page_token,
            supportsAllDrives=supports,
            includeItemsFromAllDrives=supports,
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files

def pick_latest_12_month_files(files: List[dict]) -> Dict[str, dict]:
    """
    files 중에서 이름에 YYYYMM 있는 '아파트' xlsx만 추려서,
    ym별(YY/MM) 1개(최신 modifiedTime) 선택 후 최근 12개월만.
    """
    best: Dict[str, dict] = {}
    for f in files:
        name = f.get("name", "")
        if "아파트" not in name:
            continue
        nat_title, seoul_title, ym = ym_from_apartment_filename(name)
        if not ym:
            continue
        prev = best.get(ym)
        if prev is None or f.get("modifiedTime", "") > prev.get("modifiedTime", ""):
            best[ym] = f

    def ym_key(ym: str):
        yy, mm = ym.split("/")
        return (2000 + int(yy), int(mm))

    ym_sorted = sorted(best.keys(), key=ym_key, reverse=True)[:12]
    out = {ym: best[ym] for ym in sorted(ym_sorted, key=ym_key)}
    return out

def download_drive_file(drive, file_id: str, out_path: Path):
    supports = _supports_all_drives()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=supports)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    out_path.write_bytes(fh.getvalue())

def download_latest_12_months_from_drive(creds: Credentials) -> List[Path]:
    folder_id = os.environ.get("DRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        raise RuntimeError("DRIVE_FOLDER_ID(아파트 폴더)가 필요합니다.")

    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    all_files = list_drive_files(drive, folder_id)
    log(f"[drive] files in folder={len(all_files)}")
    best = pick_latest_12_month_files(all_files)

    if not best:
        raise RuntimeError(
            "Drive에서 '아파트 YYYYMM.xlsx' 파일을 찾지 못했습니다. "
            "1) 폴더 ID가 맞는지, 2) 서비스계정이 해당 폴더에 공유돼 있는지, "
            "3) 공유드라이브면 DRIVE_SUPPORTS_ALL_DRIVES=true 인지 확인하세요."
        )

    paths: List[Path] = []
    for ym, meta in best.items():
        name = meta["name"]
        fid = meta["id"]
        out = DOWNLOAD_DIR / name
        download_drive_file(drive, fid, out)
        log(f"[drive] downloaded {ym} -> {out}")
        paths.append(out)

    return paths

# ===================== 압구정동 탭(기존 구조 유지) =====================
APGU_SHEET_NAME = "압구정동"
APGU_BASE_SHEET_NAME = "압구정동_base"

APGU_KEY_COLS = [
    "광역","구","법정동",
    "본번","부번",
    "단지명","전용면적(m²)",
    "계약년","계약월","계약일",
    "거래금액(만원)",
    "동","층",
]

def _canon_col(s: str) -> str:
    return str(s or "").strip().replace("\u00a0"," ").replace("\u3000"," ")

def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = { _canon_col(c): c for c in df.columns }
    for cand in candidates:
        cc = _canon_col(cand)
        if cc in cols:
            return cols[cc]
    def norm2(x):
        x = _canon_col(x)
        x = re.sub(r"\s+","",x)
        x = x.replace("㎡","m²").replace("m2","m²")
        x = x.replace("(","").replace(")","").replace("[","").replace("]","")
        x = x.replace(".","").replace(",","")
        return x
    cols2 = { norm2(c): c for c in df.columns }
    for cand in candidates:
        k = norm2(cand)
        if k in cols2:
            return cols2[k]
    return None

def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    area_col = _pick_col(df, ["전용면적(m²)", "전용면적(m2)", "전용면적(㎡)", "전용면적"])
    if area_col and area_col != "전용면적(m²)":
        df["전용면적(m²)"] = df[area_col]
    elif "전용면적(m²)" not in df.columns and area_col is None:
        df["전용면적(m²)"] = ""

    dong_col = _pick_col(df, ["동"])
    floor_col = _pick_col(df, ["층", "층수"])
    if dong_col and dong_col != "동":
        df["동"] = df[dong_col]
    if floor_col and floor_col != "층":
        df["층"] = df[floor_col]

    main_no = _pick_col(df, ["본번"])
    sub_no = _pick_col(df, ["부번"])
    if main_no and main_no != "본번":
        df["본번"] = df[main_no]
    if sub_no and sub_no != "부번":
        df["부번"] = df[sub_no]

    comp_col = _pick_col(df, ["단지명"])
    if comp_col and comp_col != "단지명":
        df["단지명"] = df[comp_col]

    for c in ["광역","구","법정동"]:
        if c not in df.columns:
            df[c] = ""

    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        if c not in df.columns:
            df[c] = ""

    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    for c in ["본번","부번"]:
        df[c] = df[c].astype(str).str.strip().replace({"nan":""})

    df["동"] = df["동"].astype(str).str.strip().replace({"nan":""})
    df["층"] = df["층"].astype(str).str.strip().replace({"nan":""})

    df["_면적_num"] = pd.to_numeric(df["전용면적(m²)"], errors="coerce")
    return df

def _make_key_df(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2["_면적_key"] = df2["_면적_num"].round(2).fillna(-1)
    key_cols = [
        "광역","구","법정동","본번","부번","단지명",
        "_면적_key","계약년","계약월","계약일","거래금액(만원)","동","층"
    ]
    for c in key_cols:
        if c not in df2.columns:
            df2[c] = ""
    return df2[key_cols].astype(str)

_WS_VALUES_CACHE: Dict[int, List[List[str]]] = {}

def _get_all_values_cached(ws: gspread.Worksheet) -> List[List[str]]:
    if ws.id in _WS_VALUES_CACHE:
        return _WS_VALUES_CACHE[ws.id]
    vals = _retry(ws.get_all_values) or []
    _WS_VALUES_CACHE[ws.id] = vals
    return vals

def _ws_to_df(ws: gspread.Worksheet) -> pd.DataFrame:
    vals = _get_all_values_cached(ws)
    if not vals:
        return pd.DataFrame()
    header = [str(x).strip() for x in vals[0]]
    rows = vals[1:]
    if not header:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=header[:len(rows[0])] if rows else header)

def _df_to_values(df: pd.DataFrame, header: List[str]) -> List[List[str]]:
    df2 = df.copy()
    for h in header:
        if h not in df2.columns:
            df2[h] = ""
    df2 = df2[header]
    # 안전 문자열로
    return [[_safe_cell(x) for x in row] for row in df2.values.tolist()]

def update_apgujong_tab(sh: gspread.Spreadsheet, df_all: pd.DataFrame):
    if df_all is None or df_all.empty:
        log("[apgu] skipped: df_all empty")
        return

    df_all = _ensure_cols(df_all)
    cur = df_all[(df_all["법정동"].astype(str).str.strip() == "압구정동")].copy()
    if cur.empty:
        log("[apgu] no rows for 압구정동")
        return

    cur["_dt"] = pd.to_datetime(
        cur["계약년"].astype(str) + "-" + cur["계약월"].astype(str) + "-" + cur["계약일"].astype(str),
        errors="coerce",
    )
    cur = cur.sort_values(["_dt", "거래금액(만원)"], ascending=[True, False]).drop(columns=["_dt"])

    cur_key = _make_key_df(cur)
    cur_key["__k"] = cur_key.apply(lambda r: "|".join(r.values.tolist()), axis=1)
    cur_set = set(cur_key["__k"].tolist())

    ws_main = get_or_create_ws(sh, APGU_SHEET_NAME, rows=2000, cols=40)
    ws_base = get_or_create_ws(sh, APGU_BASE_SHEET_NAME, rows=2000, cols=40)

    prev = _ws_to_df(ws_base)
    if prev.empty:
        prev_set = set()
    else:
        if "__k" in prev.columns:
            prev_set = set(prev["__k"].astype(str).tolist())
        else:
            prev2 = _ensure_cols(prev)
            pk = _make_key_df(prev2)
            pk["__k"] = pk.apply(lambda r: "|".join(r.values.tolist()), axis=1)
            prev_set = set(pk["__k"].tolist())

    added_keys = sorted(list(cur_set - prev_set))
    removed_keys = sorted(list(prev_set - cur_set))
    log(f"[apgu] snapshot rows={len(cur)} added={len(added_keys)} removed={len(removed_keys)}")

    main_vals = _get_all_values_cached(ws_main)
    if main_vals and main_vals[0]:
        header = [str(x).strip() for x in main_vals[0] if str(x).strip()]
    else:
        header = list(cur.columns)

    if "변동" not in header:
        header2 = ["변동"] + header
    else:
        header2 = header

    ws_clear(ws_main)
    ws_update(ws_main, [header2], "A1")

    cur_out = cur.copy()
    if "변동" not in cur_out.columns:
        cur_out.insert(0, "변동", "")
    values_cur = _df_to_values(cur_out, header2)

    if values_cur:
        ws_update(ws_main, values_cur, f"A2:{a1_col(len(header2))}{len(values_cur)+1}")

    # base 저장(키 + 최소컬럼)
    base_header = ["__k"] + [c for c in APGU_KEY_COLS if c in cur.columns] + ["전용면적(m²)"]
    base_header = list(dict.fromkeys(base_header))
    ws_clear(ws_base)
    ws_update(ws_base, [base_header], "A1")

    base_df = _ensure_cols(cur)
    kdf = _make_key_df(base_df)
    kdf["__k"] = kdf.apply(lambda r: "|".join(r.values.tolist()), axis=1)
    base_df["__k"] = kdf["__k"].values
    if "전용면적(m²)" not in base_df.columns:
        base_df["전용면적(m²)"] = ""

    base_vals = _df_to_values(base_df, base_header)
    if base_vals:
        ws_update(ws_base, base_vals, f"A2:{a1_col(len(base_header))}{len(base_vals)+1}")

    log("[apgu] updated main/base (diff 표시 제거: 안정 우선)")

# ===================== MAIN =====================
def main():
    log("[MAIN] start (Drive -> Local -> Sheets)")

    sheet_id = os.environ.get("SHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("SHEET_ID 환경변수가 필요합니다.")

    creds = load_creds()
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, sheet_id)

    today_iso = datetime.now().date().isoformat()

    # 1) Drive에서 최신 12개월 xlsx 다운로드 (아티팩트 사용 안 함)
    xlsx_paths = download_latest_12_months_from_drive(creds)

    # 2) 월별 집계/시트 업데이트
    df_all_frames: List[pd.DataFrame] = []
    summary_rows = []  # (ym, counts, med, mean)

    # 정렬: ym을 파일명에서 파생
    ym_map: List[Tuple[str, Path, str, str]] = []
    for p in xlsx_paths:
        nat_title, seoul_title, ym = ym_from_apartment_filename(p.name)
        if ym:
            ym_map.append((ym, p, nat_title, seoul_title))

    def ym_key(ym: str):
        yy, mm = ym.split("/")
        return (2000 + int(yy), int(mm))

    ym_map.sort(key=lambda x: ym_key(x[0]))

    log(f"[input] months_to_process={[x[0] for x in ym_map]}")

    for ym, p, nat_title, seoul_title in ym_map:
        log(f"[file] {p.name}")
        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")
        df_all_frames.append(df)

        counts, med, mean = agg_all_stats(df)
        summary_rows.append((ym, counts, med, mean))

        ws_nat = ensure_month_ws(sh, nat_title, "전국")
        header_nat = ["날짜"] + NATION_REGIONS
        values_nat = {k: int(counts.get(k, 0)) for k in NATION_REGIONS if k != "총합계"}
        values_nat["총합계"] = int(counts.get("전국", 0))
        write_month_sheet(ws_nat, today_iso, header_nat, values_nat)

        ws_seoul = ensure_month_ws(sh, seoul_title, "서울")
        header_seoul = ["날짜"] + SEOUL_REGIONS
        values_seoul = {k: int(counts.get(k, 0)) for k in SEOUL_REGIONS if k != "총합계"}
        values_seoul["총합계"] = int(counts.get("서울", 0))
        write_month_sheet(ws_seoul, today_iso, header_seoul, values_seoul)

    # 3) 거래요약 탭
    ws_sum = get_or_create_ws(sh, SUMMARY_SHEET_NAME, rows=400, cols=60)
    months = [ym for ym, *_ in ym_map]
    header = ["구분"] + months
    ws_update(ws_sum, [header], "A1")

    lookup = {ym: (c, m1, m2) for ym, c, m1, m2 in summary_rows}
    row_map = {
        "전국 거래건수": [],
        "전국 중앙값(억)": [],
        "전국 평균가(억)": [],
        "서울 거래건수": [],
        "서울 중앙값(억)": [],
        "서울 평균가(억)": [],
        "압구정동 거래건수": [],
        "압구정동 중앙값(억)": [],
        "압구정동 평균가(억)": [],
    }
    for ym in months:
        c, md, mn = lookup[ym]
        row_map["전국 거래건수"].append(int(c.get("전국", 0)))
        row_map["전국 중앙값(억)"].append(md.get("전국", ""))
        row_map["전국 평균가(억)"].append(mn.get("전국", ""))
        row_map["서울 거래건수"].append(int(c.get("서울", 0)))
        row_map["서울 중앙값(억)"].append(md.get("서울", ""))
        row_map["서울 평균가(억)"].append(mn.get("서울", ""))
        row_map["압구정동 거래건수"].append(int(c.get("압구정동", 0)))
        row_map["압구정동 중앙값(억)"].append(md.get("압구정동", ""))
        row_map["압구정동 평균가(억)"].append(mn.get("압구정동", ""))

    out_rows = [[k] + v for k, v in row_map.items()]
    ws_update(ws_sum, out_rows, f"A2:{a1_col(len(header))}{len(out_rows)+1}")
    log(f"[summary] wrote rows={len(out_rows)} months={len(months)}")

    # 4) 압구정동 탭
    try:
        df_all = pd.concat(df_all_frames, ignore_index=True) if df_all_frames else pd.DataFrame()
        update_apgujong_tab(sh, df_all)
    except Exception as e:
        log(f"[apgu] ERROR: {e}")

    log("[MAIN] done")

if __name__ == "__main__":
    main()
