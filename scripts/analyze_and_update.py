# -*- coding: utf-8 -*-
from __future__ import annotations

"""
analyze_and_update.py (정리본 / 폴더ID 확정 버전)

전제:
- DRIVE_FOLDER_ID는 '아파트' 폴더 자체 ID(또는 폴더 URL)로 정확히 설정되어 있음
- 서비스계정에 해당 폴더 접근 권한이 있음(검증 완료)

목표:
- Drive에서 '아파트 YYYYMM.xlsx' 최신 12개월만 내려받아 처리 (Drive 호출 최소화)
- Google Sheets 업데이트: 전국/서울 월탭 + 거래요약 + 압구정동/압구정동_base

필수 ENV:
- SHEET_ID
- SA_JSON 또는 SA_PATH (또는 GDRIVE_SA_JSON도 허용)
- DRIVE_FOLDER_ID
- DRIVE_SUPPORTS_ALL_DRIVES: "true" 권장(공유드라이브면 사실상 필수)

선택 ENV:
- DRIVE_FILE_REGEX: 기본 r"^아파트\\s*(\\d{6})\\.xlsx$"
- DRIVE_SCAN_MAX_FILES: list pageSize(기본 1000, Drive API 최대 1000)
- DOWNLOAD_DIR: 기본 "_drive_downloads"
- MAX_SCAN_ROWS: 기본 900
"""

import os, re, json, time, random
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional, Union

import numpy as np
import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

# ===================== 설정 =====================
LOG_DIR = Path("analyze_report")
SUMMARY_SHEET_NAME = "거래요약"

MAX_SCAN_ROWS = int(os.environ.get("MAX_SCAN_ROWS", "900"))
DOWNLOAD_DIR = Path(os.environ.get("DOWNLOAD_DIR", "_drive_downloads"))

DRIVE_FILE_REGEX = os.environ.get("DRIVE_FILE_REGEX", r"^아파트\s*(\d{6})\.xlsx$")
APT_FILE_RE = re.compile(DRIVE_FILE_REGEX)

# Drive files.list pageSize max = 1000
DRIVE_SCAN_MAX_FILES = int(os.environ.get("DRIVE_SCAN_MAX_FILES", "1000"))

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
        if LOG_DIR.exists() and not LOG_DIR.is_dir():
            LOG_DIR.unlink()
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
def _bool_env(name: str, default: bool = False) -> bool:
    v = str(os.environ.get(name, str(default))).strip().lower()
    return v in ("1","true","yes","y","on")

def _extract_id(x: str) -> str:
    if not x:
        return ""
    x = x.strip()
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", x)
    if m: return m.group(1)
    m = re.search(r"id=([a-zA-Z0-9_-]+)", x)
    if m: return m.group(1)
    return x

def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

# ===================== 인증 =====================
def _get_sa_json_env() -> str:
    return (os.environ.get("SA_JSON") or os.environ.get("GDRIVE_SA_JSON") or "").strip()

def load_creds():
    sa_json = _get_sa_json_env()
    sa_path = os.environ.get("SA_PATH", "").strip()

    if sa_json:
        info = json.loads(sa_json)
    elif sa_path:
        info = json.loads(Path(sa_path).read_text(encoding="utf-8"))
    else:
        raise RuntimeError("SA_JSON(또는 GDRIVE_SA_JSON) 또는 SA_PATH 환경변수가 필요합니다.")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return Credentials.from_service_account_info(info, scopes=scopes)

def build_drive(creds):
    return build("drive", "v3", credentials=creds, cache_discovery=False)

# ===================== Drive (폴더ID 확정/최소호출) =====================
def drive_list_files(
    drive,
    q: str,
    supports_all_drives: bool,
    corpora: str = "allDrives",
    drive_id: Optional[str] = None,
    page_size: int = 1000,
    fields: str = "nextPageToken, files(id,name,mimeType,driveId,parents,modifiedTime,createdTime,size)"
):
    # Drive API hard limit: 1000
    try:
        page_size = int(page_size)
    except Exception:
        page_size = 1000
    page_size = max(1, min(page_size, 1000))

    kwargs = dict(
        q=q,
        fields=fields,
        supportsAllDrives=supports_all_drives,
        includeItemsFromAllDrives=supports_all_drives,
        pageSize=page_size,
        corpora=corpora,
    )
    if corpora == "drive" and drive_id:
        kwargs["driveId"] = drive_id

    out, token = [], None
    while True:
        if token:
            kwargs["pageToken"] = token
        resp = drive.files().list(**kwargs).execute()
        out.extend(resp.get("files", []))
        token = resp.get("nextPageToken")
        if not token:
            break
    return out

def get_folder_meta(drive, folder_id: str, supports_all_drives: bool) -> dict:
    return drive.files().get(
        fileId=folder_id,
        fields="id,name,mimeType,driveId,parents",
        supportsAllDrives=supports_all_drives
    ).execute()

def pick_latest_12_months_from_folder(drive, folder_id: str, supports_all_drives: bool) -> List[dict]:
    """
    폴더 내 xlsx만 타이트하게 list -> 파일명 regex로 YYYYMM 추출 -> 최신 12개월 선택
    Drive 호출: files.list (보통 1회, 많아도 페이지 토큰만큼)
    """
    meta = get_folder_meta(drive, folder_id, supports_all_drives)
    drive_id = meta.get("driveId")

    corpora = "drive" if (supports_all_drives and drive_id) else "allDrives"

    # 폴더 내 xlsx만(가능하면 mimeType으로 먼저 거름)
    q = (
        f"'{folder_id}' in parents and trashed=false "
        f"and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    )

    items = drive_list_files(
        drive, q, supports_all_drives,
        corpora=corpora, drive_id=drive_id,
        page_size=min(DRIVE_SCAN_MAX_FILES, 1000),
    )

    matched = []
    for it in items:
        name = it.get("name", "")
        m = APT_FILE_RE.match(name)
        if m:
            ym = m.group(1)  # YYYYMM
            matched.append((ym, it))

    log(f"[drive] listed_xlsx={len(items)} matched_apt_xlsx={len(matched)} folder={folder_id}")

    if not matched:
        raise RuntimeError(
            "Drive 폴더에서 '아파트 YYYYMM.xlsx' 파일을 찾지 못했습니다.\n"
            "- DRIVE_FOLDER_ID가 '아파트 폴더 자체'인지\n"
            "- 파일명이 '아파트 202602.xlsx' 형식인지\n"
            "- DRIVE_FILE_REGEX가 맞는지 확인하세요."
        )

    def ts(it):
        s = it.get("modifiedTime") or it.get("createdTime") or ""
        try:
            return datetime.fromisoformat(s.replace("Z","+00:00")).timestamp()
        except Exception:
            return 0.0

    # 동일 월(YYYYMM)은 최신 modifiedTime만
    best_by_ym: Dict[str, dict] = {}
    for ym, it in matched:
        cur = best_by_ym.get(ym)
        if not cur or ts(it) > ts(cur):
            best_by_ym[ym] = it

    yms = sorted(best_by_ym.keys(), reverse=True)[:12]
    yms = sorted(yms)
    log(f"[drive] months_to_process={yms}")
    return [best_by_ym[ym] for ym in yms]

def download_file_from_drive(drive, file_id: str, out_path: Path, supports_all_drives: bool):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=supports_all_drives)
    with out_path.open("wb") as f:
        downloader = MediaIoBaseDownload(f, request, chunksize=1024 * 1024)
        done = False
        while not done:
            status, done = downloader.next_chunk()
    return out_path

def download_latest_12_months_from_drive(creds) -> List[Path]:
    supports_all_drives = _bool_env("DRIVE_SUPPORTS_ALL_DRIVES", True)

    folder_env = os.environ.get("DRIVE_FOLDER_ID", "").strip()
    if not folder_env:
        raise RuntimeError("DRIVE_FOLDER_ID 환경변수가 필요합니다. ('아파트' 폴더 자체 ID/URL)")

    folder_id = _extract_id(folder_env)
    if not folder_id:
        raise RuntimeError("DRIVE_FOLDER_ID에서 폴더 ID를 추출하지 못했습니다. 폴더 URL 또는 ID를 확인하세요.")

    drive = build_drive(creds)

    picked = pick_latest_12_months_from_folder(drive, folder_id, supports_all_drives)

    paths: List[Path] = []
    for it in picked:
        name = it.get("name", "")
        fid = it.get("id", "")
        if not fid:
            continue
        out = DOWNLOAD_DIR / name  # ✅ 원본 파일명 유지
        log(f"[drive] downloading: {name}")
        download_file_from_drive(drive, fid, out, supports_all_drives)
        paths.append(out)

    log(f"[drive] downloaded files={len(paths)} -> {DOWNLOAD_DIR}")
    return paths

# ===================== 시트/캐시 =====================
_WS_VALUES_CACHE: Dict[int, List[List[str]]] = {}

def _invalidate_cache(ws: Optional[gspread.Worksheet]):
    try:
        if ws is not None:
            _WS_VALUES_CACHE.pop(ws.id, None)
    except Exception:
        pass

def _get_all_values_cached(ws: gspread.Worksheet) -> List[List[str]]:
    if ws.id in _WS_VALUES_CACHE:
        return _WS_VALUES_CACHE[ws.id]
    vals = _retry(ws.get_all_values) or []
    _WS_VALUES_CACHE[ws.id] = vals
    return vals

def ws_update(ws: gspread.Worksheet, values, range_name: str):
    resp = _retry(ws.update, values, range_name)
    _invalidate_cache(ws)
    return resp

def ws_clear(ws: gspread.Worksheet):
    resp = _retry(ws.clear)
    _invalidate_cache(ws)
    return resp

def values_batch_update(ws: gspread.Worksheet, data: List[Dict]):
    safe_data = []
    for d in data:
        vals = d.get("values", [])
        safe_vals = []
        for row in vals:
            safe_row = []
            for v in row:
                if v is None:
                    safe_row.append("")
                elif isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                    safe_row.append("")
                else:
                    safe_row.append(v)
            safe_vals.append(safe_row)
        safe_data.append({"range": d["range"], "values": safe_vals})

    body = {"valueInputOption": "USER_ENTERED", "data": safe_data}
    resp = _retry(ws.spreadsheet.values_batch_update, body=body)
    _invalidate_cache(ws)
    return resp

def batch_format(ws: gspread.Worksheet, requests: List[dict]):
    if not requests:
        return None
    def _clean(obj):
        if isinstance(obj, dict):
            return {k: _clean(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_clean(v) for v in obj]
        if isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
            return 0.0
        return obj

    payload = {"requests": _clean(requests)}
    return _retry(ws.spreadsheet.batch_update, payload)

def ws_batch_clear(ws: gspread.Worksheet, ranges: List[str]):
    resp = _retry(ws.batch_clear, ranges)
    _invalidate_cache(ws)
    return resp

def fuzzy_ws(sh: gspread.Spreadsheet, wanted: str) -> Optional[gspread.Worksheet]:
    tgt = re.sub(r"\s+", "", wanted.strip())
    for ws in sh.worksheets():
        if re.sub(r"\s+", "", ws.title.strip()) == tgt:
            log(f"[ws] matched: '{ws.title}' (wanted='{wanted}')")
            return ws
    return None

def get_or_create_ws(sh: gspread.Spreadsheet, title: str, rows: int = 100, cols: int = 20) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, title)
    if ws is None:
        ws = _retry(sh.add_worksheet, title=title, rows=rows, cols=cols)
        log(f"[ws] created: {title}")
    return ws

# ===================== 월탭 이름/파일명 처리 =====================
def ym_from_apt_filename(fname: str):
    s = str(fname or "")
    m = re.search(r"(20\d{2})(\d{2})", s)
    if not m:
        return None, None, None
    y, mm = int(m.group(1)), int(m.group(2))
    if not (1 <= mm <= 12):
        return None, None, None
    return f"전국 {y}년 {mm}월", f"서울 {y}년 {mm}월", f"{y%100:02d}/{mm:02d}"

# ===================== 날짜 파싱 =====================
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

# ===================== 월탭 A열 고정 스캔 =====================
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

def ensure_month_ws(sh: gspread.Spreadsheet, title: str, level: str) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, title)
    if ws is not None:
        return ws
    ws = get_or_create_ws(sh, title, rows=800, cols=40)
    header = ["날짜"] + (NATION_REGIONS if level == "전국" else SEOUL_REGIONS)
    ws_update(ws, [header], "A1")
    log(f"[ws] created from scratch: {title}")
    return ws

# ===================== 파일 읽기/집계 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="Sheet1", dtype=str)
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

# ===================== 압구정동 탭(스냅샷/변동) =====================
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

def _ws_to_df(ws: gspread.Worksheet) -> pd.DataFrame:
    vals = _get_all_values_cached(ws)
    if not vals:
        return pd.DataFrame()
    header = [str(x).strip() for x in vals[0]]
    rows = vals[1:]
    if not header:
        return pd.DataFrame()
    maxw = max([len(r) for r in rows], default=len(header))
    header = header + [""] * (maxw - len(header))
    norm_rows = []
    for r in rows:
        rr = r + [""] * (maxw - len(r))
        norm_rows.append(rr)
    return pd.DataFrame(norm_rows, columns=header[:maxw])

def _df_to_values(df: pd.DataFrame, header: List[str]) -> List[List[str]]:
    df2 = df.copy()
    for h in header:
        if h not in df2.columns:
            df2[h] = ""
    df2 = df2[header]
    return df2.replace([np.inf, -np.inf], "").fillna("").astype(str).values.tolist()

def _hide_sheet(ws: gspread.Worksheet):
    try:
        batch_format(ws, [{
            "updateSheetProperties": {
                "properties": {"sheetId": ws.id, "hidden": True},
                "fields": "hidden"
            }
        }])
    except Exception:
        pass

def _set_text_color(ws: gspread.Worksheet, start_row: int, end_row: int, start_col: int, end_col: int, rgb: Tuple[float,float,float]):
    r,g,b = rgb
    req = [{
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": start_row-1,
                "endRowIndex": end_row,
                "startColumnIndex": start_col-1,
                "endColumnIndex": end_col,
            },
            "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red": float(r), "green": float(g), "blue": float(b)}}}},
            "fields": "userEnteredFormat.textFormat.foregroundColor"
        }
    }]
    batch_format(ws, req)

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
    _hide_sheet(ws_base)

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

    header2 = ["변동"] + header if "변동" not in header else header

    ws_clear(ws_main)
    ws_update(ws_main, [header2], "A1")

    cur_out = cur.copy()
    if "변동" not in cur_out.columns:
        cur_out.insert(0, "변동", "")
    values_cur = _df_to_values(cur_out, header2)

    df_all_key = _make_key_df(_ensure_cols(df_all))
    df_all_key["__k"] = df_all_key.apply(lambda r: "|".join(r.values.tolist()), axis=1)

    need = set(added_keys) | set(removed_keys)
    key_to_row = {}
    if need:
        mask = df_all_key["__k"].isin(list(need))
        sub = df_all.loc[mask.values].copy()
        sub = _ensure_cols(sub)
        sub_key = _make_key_df(sub)
        sub_key["__k"] = sub_key.apply(lambda r: "|".join(r.values.tolist()), axis=1)
        for i, k in enumerate(sub_key["__k"].tolist()):
            if k not in key_to_row:
                key_to_row[k] = sub.iloc[i]

    def row_from_key(k: str) -> pd.Series:
        if k in key_to_row:
            return key_to_row[k]
        parts = k.split("|")
        cols = ["광역","구","법정동","본번","부번","단지명","_면적_key","계약년","계약월","계약일","거래금액(만원)","동","층"]
        d = {c: parts[i] if i < len(parts) else "" for i,c in enumerate(cols)}
        d["전용면적(m²)"] = d.pop("_면적_key","")
        return pd.Series(d)

    diff_rows = []
    for k in removed_keys:
        rr = row_from_key(k).to_dict()
        rr["변동"] = "삭제"
        diff_rows.append(rr)
    for k in added_keys:
        rr = row_from_key(k).to_dict()
        rr["변동"] = "추가"
        diff_rows.append(rr)

    start_row = 2
    if values_cur:
        ws_update(ws_main, values_cur, f"A{start_row}:{a1_col(len(header2))}{start_row+len(values_cur)-1}")

    diff_start = start_row + len(values_cur) + 2
    if diff_rows:
        df_diff = pd.DataFrame(diff_rows)
        df_diff = _ensure_cols(df_diff)
        if "변동" not in df_diff.columns:
            df_diff.insert(0, "변동", "")
        values_diff = _df_to_values(df_diff, header2)

        ws_update(ws_main, [["변동사항(삭제=빨강, 추가=파랑)"] + [""]*(len(header2)-1)],
                  f"A{diff_start-1}:{a1_col(len(header2))}{diff_start-1}")
        ws_update(ws_main, values_diff, f"A{diff_start}:{a1_col(len(header2))}{diff_start+len(values_diff)-1}")

        del_n = len(removed_keys)
        add_n = len(added_keys)
        if del_n:
            _set_text_color(ws_main, diff_start, diff_start+del_n-1, 1, len(header2), (0.85,0.0,0.0))
        if add_n:
            _set_text_color(ws_main, diff_start+del_n, diff_start+del_n+add_n-1, 1, len(header2), (0.0,0.2,0.85))

    base_header = ["__k"] + [c for c in APGU_KEY_COLS if c in cur.columns] + ["전용면적(m²)"]
    base_header = list(dict.fromkeys(base_header))
    ws_clear(ws_base)
    ws_update(ws_base, [base_header], "A1")

    base_df2 = _ensure_cols(cur.copy())
    kdf = _make_key_df(base_df2)
    kdf["__k"] = kdf.apply(lambda r: "|".join(r.values.tolist()), axis=1)
    base_df2["__k"] = kdf["__k"].values
    if "전용면적(m²)" not in base_df2.columns:
        base_df2["전용면적(m²)"] = ""

    base_vals = _df_to_values(base_df2, base_header)
    if base_vals:
        ws_update(ws_base, base_vals, f"A2:{a1_col(len(base_header))}{len(base_vals)+1}")
    _hide_sheet(ws_base)
    log("[apgu] updated main/base")

# ===================== 메인 =====================
def main():
    log("[MAIN] start (Drive -> Local -> Sheets)")

    sheet_id = os.environ.get("SHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("SHEET_ID 환경변수가 필요합니다.")

    if not _get_sa_json_env() and not os.environ.get("SA_PATH", "").strip():
        raise RuntimeError("SA_JSON(또는 GDRIVE_SA_JSON) 또는 SA_PATH 환경변수가 필요합니다.")

    creds = load_creds()

    # 1) Drive에서 최신 12개월 xlsx 다운로드
    xlsx_paths = download_latest_12_months_from_drive(creds)
    if not xlsx_paths:
        log("[drive] no files downloaded. stop.")
        return

    # 2) Sheets 접속
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, sheet_id)

    today_iso = datetime.now().date().isoformat()

    # 3) 월별 처리
    df_all_frames: List[pd.DataFrame] = []
    summary_rows = []

    def ym_key(yymm: str):
        yy, mm = yymm.split("/")
        return (2000 + int(yy), int(mm))

    file_map: Dict[str, Path] = {}
    for p in xlsx_paths:
        nat_title, seoul_title, yymm = ym_from_apt_filename(p.name)
        if yymm:
            file_map[yymm] = p

    yms = sorted(file_map.keys(), key=ym_key)
    log(f"[input] months_to_process={yms}")

    for yymm in yms:
        p = file_map[yymm]
        nat_title, seoul_title, _ = ym_from_apt_filename(p.name)
        log(f"[file] {p.name}")

        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")
        df_all_frames.append(df)

        counts, med, mean = agg_all_stats(df)
        summary_rows.append((yymm, counts, med, mean))

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

    # 4) 거래요약 탭
    ws_sum = get_or_create_ws(sh, SUMMARY_SHEET_NAME, rows=400, cols=60)
    months = [x[0] for x in summary_rows]
    header = ["구분"] + months
    ws_update(ws_sum, [header], "A1")

    lookup = {ym: (c, md, mn) for ym, c, md, mn in summary_rows}
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

    out_rows = [[k] + arr for k, arr in row_map.items()]
    ws_update(ws_sum, out_rows, f"A2:{a1_col(len(header))}{len(out_rows)+1}")
    log(f"[summary] wrote rows={len(out_rows)} months={len(months)}")

    # 5) 압구정동 탭
    try:
        df_all = pd.concat(df_all_frames, ignore_index=True) if df_all_frames else pd.DataFrame()
        update_apgujong_tab(sh, df_all)
    except Exception as e:
        log(f"[apgu] ERROR: {e}")

    log("[MAIN] done")

if __name__ == "__main__":
    main()
