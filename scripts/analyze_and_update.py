# -*- coding: utf-8 -*-
from __future__ import annotations

"""
analyze_and_update.py (전체본/수정본)

목표(이번 이슈 대응 + Drive 연동):
- Drive 폴더에서 '아파트 YYYYMM.xlsx' 파일을 월별로 찾아 최신 N개월 다운로드
- 로컬에 artifacts/output/전국 YYMM_YYMMDD.xlsx 로 저장 (기존 로직이 그대로 읽게)
- 이후 집계/월탭 업데이트/요약탭 업데이트/압구정동 탭(스냅샷+변동) 로직은 기존 유지

핵심 수정(기존 이슈 대응):
1) '오늘 날짜' 라벨을 ISO("YYYY-MM-DD")로 기록 (월탭/요약탭 공통)
2) 날짜 행 탐색을 A2:A{MAX_SCAN_ROWS} 고정 범위로 스캔
3) 쓰기 직후 해당 행 verify 로그
4) 지역/구/법정동 strip()
5) Google Sheets values_batch_update에서 NaN/Inf JSON 에러 방지(sanitize)

환경변수(깃허브 액션):
- SHEET_ID: 대상 구글시트 ID
- SA_JSON 또는 SA_PATH: 서비스계정 JSON(문자열 또는 파일경로)
- ARTIFACTS_DIR: 기본 'artifacts'
- MAX_SCAN_ROWS: 기본 900
- DRIVE_FOLDER_ID: (권장) Drive 폴더 ID
- DRIVE_FOLDER_NAME: 폴더 ID 없을 때 폴더명 탐색(동명이면 위험)
- DRIVE_MAX_MONTHS: 다운로드할 최신 개월 수(기본 12)
"""

import os, re, json, time, random, io
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional, Union

import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# Drive API
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# ===================== 설정 =====================
LOG_DIR = Path("analyze_report")
ARTIFACTS_DIR = Path(os.environ.get("ARTIFACTS_DIR", "artifacts"))
SUMMARY_SHEET_NAME = "거래요약"

MAX_SCAN_ROWS = int(os.environ.get("MAX_SCAN_ROWS", "900"))
DRIVE_MAX_MONTHS = int(os.environ.get("DRIVE_MAX_MONTHS", "12"))

# Drive folder config
DRIVE_FOLDER_ID = (os.environ.get("DRIVE_FOLDER_ID", "") or "").strip()
DRIVE_FOLDER_NAME = (os.environ.get("DRIVE_FOLDER_NAME", "") or "아파트").strip()

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

# ===================== 인증 =====================
def load_creds(scopes: Optional[List[str]] = None) -> Credentials:
    sa_json = os.environ.get("SA_JSON", "").strip()
    sa_path = os.environ.get("SA_PATH", "").strip()

    if sa_json:
        info = json.loads(sa_json)
    elif sa_path:
        info = json.loads(Path(sa_path).read_text(encoding="utf-8"))
    else:
        raise RuntimeError("SA_JSON 또는 SA_PATH 환경변수가 필요합니다.")

    if scopes is None:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
    return Credentials.from_service_account_info(info, scopes=scopes)

# ===================== Drive에서 아파트 파일 내려받기 =====================
APT_RE = re.compile(r"아파트[\s_\-]*((20\d{2})(\d{2}))", re.IGNORECASE)  # group1=YYYYMM

def _drive_service(creds: Credentials):
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def _find_folder_id_by_name(svc, name: str) -> Optional[str]:
    q = (
        f"name = '{name}' and "
        "mimeType = 'application/vnd.google-apps.folder' and "
        "trashed = false"
    )
    res = svc.files().list(
        q=q,
        fields="files(id,name)",
        pageSize=10,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = res.get("files", [])
    if not files:
        return None
    if len(files) > 1:
        log(f"[drive] WARNING: folder name '{name}' matched {len(files)} folders. using first: {files[0]['id']}")
    return files[0]["id"]

def _list_xlsx_in_folder(svc, folder_id: str) -> List[dict]:
    q = (
        f"'{folder_id}' in parents and "
        "trashed = false and "
        "mimeType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    )
    out = []
    token = None
    while True:
        res = svc.files().list(
            q=q,
            fields="nextPageToken, files(id,name,modifiedTime,size)",
            pageSize=1000,
            pageToken=token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        out.extend(res.get("files", []))
        token = res.get("nextPageToken")
        if not token:
            break
    return out

def _download_drive_file(svc, file_id: str, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    request = svc.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.FileIO(dest, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

def _ym_key_yyyymm(yyyymm: str) -> Tuple[int, int]:
    return (int(yyyymm[:4]), int(yyyymm[4:]))

def fetch_drive_apartment_files_to_artifacts(today_iso: str) -> List[Path]:
    """
    Drive 폴더에서 '아파트 YYYYMM.xlsx'를 찾아 최신 N개월만 다운로드.
    저장명: artifacts/output/전국 YYMM_YYMMDD.xlsx  (기존 analyze 로직이 읽기 쉬운 형태로 맞춤)
    """
    # output이 파일로 잘못 생긴 경우 방어
    try:
        out_root = ARTIFACTS_DIR
        out_root.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    out_dir = ARTIFACTS_DIR / "output"
    if out_dir.exists() and out_dir.is_file():
        out_dir.unlink()
    out_dir.mkdir(parents=True, exist_ok=True)

    drive_creds = load_creds(scopes=["https://www.googleapis.com/auth/drive.readonly"])
    svc = _drive_service(drive_creds)

    folder_id = DRIVE_FOLDER_ID
    if not folder_id:
        folder_id = _find_folder_id_by_name(svc, DRIVE_FOLDER_NAME)
        if not folder_id:
            raise RuntimeError(
                f"[drive] folder not found: name='{DRIVE_FOLDER_NAME}'. "
                f"권장: DRIVE_FOLDER_ID를 secrets로 설정하세요."
            )

    files = _list_xlsx_in_folder(svc, folder_id)

    apt_files = []
    for f in files:
        nm = f.get("name", "")
        m = APT_RE.search(nm)
        if m:
            apt_files.append((m.group(1), f))  # (YYYYMM, file)

    if not apt_files:
        raise RuntimeError(f"[drive] no '아파트 YYYYMM.xlsx' matches in folder_id={folder_id}")

    # month별 최신( modifiedTime 기준 ) 1개만
    best: Dict[str, dict] = {}
    for yyyymm, f in apt_files:
        prev = best.get(yyyymm)
        if not prev or (f.get("modifiedTime", "") > prev.get("modifiedTime", "")):
            best[yyyymm] = f

    months = sorted(best.keys(), key=_ym_key_yyyymm, reverse=True)[:DRIVE_MAX_MONTHS]
    months = sorted(months, key=_ym_key_yyyymm)  # 과거→현재

    downloaded: List[Path] = []
    yymmdd = today_iso.replace("-", "")[2:]  # YYMMDD

    for yyyymm in months:
        f = best[yyyymm]
        yy = yyyymm[2:4]
        mm = yyyymm[4:6]
        dest = out_dir / f"전국 {yy}{mm}_{yymmdd}.xlsx"
        log(f"[drive] download: {f.get('name')} (modified={f.get('modifiedTime')}) -> {dest}")
        _download_drive_file(svc, f["id"], dest)
        downloaded.append(dest)

    log(f"[drive] downloaded_files={len(downloaded)} months={months}")
    return downloaded

# ===================== gspread 캐시 =====================
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

# ===================== gspread wrappers =====================
def ws_update(ws: gspread.Worksheet, values, range_name: str):
    resp = _retry(ws.update, values, range_name)
    _invalidate_cache(ws)
    return resp

def ws_clear(ws: gspread.Worksheet):
    resp = _retry(ws.clear)
    _invalidate_cache(ws)
    return resp

def ws_add_rows(ws: gspread.Worksheet, n: int):
    resp = _retry(ws.add_rows, n)
    _invalidate_cache(ws)
    return resp

def batch_format(ws: gspread.Worksheet, requests: List[dict]):
    if not requests:
        return None
    return _retry(ws.spreadsheet.batch_update, {"requests": requests})

def ws_batch_clear(ws: gspread.Worksheet, ranges: List[str]):
    resp = _retry(ws.batch_clear, ranges)
    _invalidate_cache(ws)
    return resp

def values_batch_update(ws: gspread.Worksheet, data: List[Dict]):
    # JSON sanitize (NaN/Inf 방지)
    def _clean(v):
        if v is None:
            return ""
        if isinstance(v, float):
            if np.isnan(v) or np.isinf(v):
                return ""
        return v

    clean_data = []
    for item in data:
        vals = item.get("values", [])
        new_vals = []
        for row in vals:
            new_vals.append([_clean(x) for x in row])
        clean_data.append({"range": item.get("range"), "values": new_vals})

    body = {"valueInputOption": "USER_ENTERED", "data": clean_data}
    resp = _retry(ws.spreadsheet.values_batch_update, body=body)
    _invalidate_cache(ws)
    return resp

def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "").replace("\u3000", "").strip())

YM_RE = re.compile(r"(\d{4})년\s*(\d{1,2})월")

def yymm_from_title(title: str) -> Optional[str]:
    m = YM_RE.search(title or "")
    if not m:
        return None
    y, mm = int(m.group(1)), int(m.group(2))
    if not (1 <= mm <= 12):
        return None
    return f"{y%100:02d}/{mm:02d}"

def ym_from_filename(fname: str):
    s = str(fname or "")
    # 기존: 전국 2503_260205.xlsx
    m = re.search(r"\b(\d{2})(\d{2})[_\-\.\s]", s)
    if m:
        yy, mm = int(m.group(1)), int(m.group(2))
        if 1 <= mm <= 12:
            y = 2000 + yy
            return f"전국 {y}년 {mm}월", f"서울 {y}년 {mm}월", f"{yy:02d}/{mm:02d}"
    # Drive 원본명 그대로 저장될 경우 대비: 아파트 202510.xlsx
    m = re.search(r"\b(20\d{2})(\d{2})\b", s)
    if m:
        y, mm = int(m.group(1)), int(m.group(2))
        if 1 <= mm <= 12:
            return f"전국 {y}년 {mm}월", f"서울 {y}년 {mm}월", f"{y%100:02d}/{mm:02d}"
    m = re.search(r"(20\d{2})\s*년\s*(\d{1,2})\s*월", s)
    if m:
        y, mm = int(m.group(1)), int(m.group(2))
        if 1 <= mm <= 12:
            return f"전국 {y}년 {mm}월", f"서울 {y}년 {mm}월", f"{y%100:02d}/{mm:02d}"
    return None, None, None

def fuzzy_ws(sh: gspread.Spreadsheet, wanted: str) -> Optional[gspread.Worksheet]:
    tgt = _norm(wanted)
    for ws in sh.worksheets():
        if _norm(ws.title) == tgt:
            log(f"[ws] matched: '{ws.title}' (wanted='{wanted}')")
            return ws
    return None

def get_or_create_ws(sh: gspread.Spreadsheet, title: str, rows: int = 100, cols: int = 20) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, title)
    if ws is None:
        ws = _retry(sh.add_worksheet, title=title, rows=rows, cols=cols)
        log(f"[ws] created: {title}")
    return ws

from urllib.parse import quote

def log_focus_link(ws: gspread.Worksheet, row_idx: int, last_col_index: int, sheet_id: str):
    try:
        a1_last = a1_col(last_col_index if last_col_index >= 1 else 1)
        range_a1 = f"{ws.title}!A{row_idx}:{a1_last}{row_idx}"
        link = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={ws.id}&range={quote(range_a1)}"
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with (LOG_DIR / "where_written.txt").open("a", encoding="utf-8") as f:
            f.write(f"[{ws.title}] wrote row {row_idx} → {range_a1}\n")
            f.write(f"Open here: {link}\n")
    except Exception:
        pass

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

# ===================== 파일/읽기/집계 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data", dtype=str)
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

# ===================== 월 시트 생성/확보 =====================
def ensure_month_ws(sh: gspread.Spreadsheet, title: str, level: str) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, title)
    if ws is not None:
        return ws
    ws = get_or_create_ws(sh, title, rows=800, cols=40)
    header = ["날짜"] + (NATION_REGIONS if level == "전국" else SEOUL_REGIONS)
    ws_update(ws, [header], "A1")
    _invalidate_cache(ws)
    log(f"[ws] created from scratch: {title}")
    return ws

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

def write_month_sheet(ws: gspread.Worksheet, date_iso: str, header: List[str], values_by_colname: Dict[str, int], sheet_id: str):
    hmap = {str(h).strip(): idx + 1 for idx, h in enumerate(header) if str(h).strip()}
    row_idx = find_or_append_date_row(ws, date_iso)

    sheet_prefix = f"'{ws.title}'!"
    payload = [{"range": f"{sheet_prefix}A{row_idx}", "values": [[date_iso]]}]
    for col_name, val in values_by_colname.items():
        if col_name in hmap:
            c = hmap[col_name]
            payload.append({"range": f"{sheet_prefix}{a1_col(c)}{row_idx}", "values": [[int(val)]]})

    values_batch_update(ws, payload)
    log(f"[ws] {ws.title} -> {date_iso} row={row_idx} (wrote {len(payload)} cells incl. date)")
    log_focus_link(ws, row_idx, len(header or []), sheet_id)

    try:
        last_col = a1_col(max(1, len(header)))
        vrng = f"A{row_idx}:{last_col}{row_idx}"
        got = _retry(ws.get, vrng) or []
        if got:
            row = got[0]
            view = row[:6] + (["..."] if len(row) > 10 else []) + (row[-2:] if len(row) > 8 else [])
            log(f"[verify] {ws.title} {vrng} -> {view}")
    except Exception as e:
        log(f"[verify] failed: {e}")

# ===================== 아티팩트 수집 =====================
def collect_input_files(root: Path) -> List[Path]:
    if not root.exists():
        return []
    candidates = []
    for pat in ("**/전국*.xlsx", "**/서울시*.xlsx", "**/서울 *.xlsx", "**/*전국*.xlsx", "**/*서울시*.xlsx", "**/아파트*.xlsx", "**/*아파트*.xlsx"):
        candidates += list(root.rglob(pat))
    uniq = []
    seen = set()
    for p in candidates:
        rp = str(p.resolve())
        if rp not in seen:
            seen.add(rp)
            uniq.append(p)
    return sorted(uniq)

# ===================== 압구정동(법정동) 스냅샷/변동사항 기록 =====================
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
    if rows:
        out = pd.DataFrame(rows, columns=header[:len(rows[0])] if rows[0] else header)
    else:
        out = pd.DataFrame(columns=header)
    return out

def _df_to_values(df: pd.DataFrame, header: List[str]) -> List[List[str]]:
    df2 = df.copy()
    for h in header:
        if h not in df2.columns:
            df2[h] = ""
    df2 = df2[header]
    return df2.astype(str).values.tolist()

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
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "foregroundColor": {"red": r, "green": g, "blue": b}
                    }
                }
            },
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
        prev_key = prev.copy()
        if "__k" in prev_key.columns:
            prev_set = set(prev_key["__k"].astype(str).tolist())
        else:
            prev_key = _ensure_cols(prev_key)
            pk = _make_key_df(prev_key)
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

    df_all_key = _make_key_df(_ensure_cols(df_all))
    df_all_key["__k"] = df_all_key.apply(lambda r: "|".join(r.values.tolist()), axis=1)

    key_to_row = {}
    need = set(added_keys) | set(removed_keys)
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
        r = row_from_key(k)
        rr = r.to_dict()
        rr["변동"] = "삭제"
        diff_rows.append(rr)
    for k in added_keys:
        r = row_from_key(k)
        rr = r.to_dict()
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

        ws_update(ws_main, [["변동사항(삭제=빨강, 추가=파랑)"] + [""]*(len(header2)-1)], f"A{diff_start-1}:{a1_col(len(header2))}{diff_start-1}")
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

    base_df = cur.copy()
    base_df2 = _ensure_cols(base_df)
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
    log("[MAIN] start")

    sheet_id = os.environ.get("SHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("SHEET_ID 환경변수가 필요합니다.")

    # 오늘 날짜 ISO
    today_iso = datetime.now().date().isoformat()

    # 1) Drive -> artifacts/output 으로 다운로드 (핵심)
    try:
        fetch_drive_apartment_files_to_artifacts(today_iso)
    except Exception as e:
        # Drive 설정이 아직 없을 수도 있으니, 오류를 명확히 로그로 남기고 계속(로컬 artifacts로라도 진행)
        log(f"[drive] ERROR: {e}")

    # 2) Sheets 접속
    creds = load_creds()
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, sheet_id)

    # 3) artifacts에서 파일 수집
    files = collect_input_files(ARTIFACTS_DIR)
    xlsx = [p for p in files if p.suffix.lower() == ".xlsx"]
    log(f"[input] artifacts_root={ARTIFACTS_DIR} xlsx_files={len(xlsx)}")
    if not xlsx:
        log("[input] no xlsx files found. stop.")
        return

    # 월별 최신 파일 선택
    best_by_ym: Dict[str, Path] = {}
    for p in xlsx:
        nat_title, seoul_title, ym = ym_from_filename(p.name)
        if not ym:
            continue
        prev = best_by_ym.get(ym)
        if prev is None or p.stat().st_mtime > prev.stat().st_mtime:
            best_by_ym[ym] = p

    def ym_key(ym: str):
        yy, mm = ym.split("/")
        return (2000 + int(yy), int(mm))

    ym_sorted = sorted(best_by_ym.keys(), key=ym_key, reverse=True)[:DRIVE_MAX_MONTHS]
    ym_sorted = sorted(ym_sorted, key=ym_key)
    log(f"[input] months_to_process={ym_sorted}")

    df_all_frames: List[pd.DataFrame] = []
    summary_rows = []  # (ym, counts, med, mean)

    for ym in ym_sorted:
        p = best_by_ym[ym]
        nat_title, seoul_title, _ = ym_from_filename(p.name)
        if not nat_title or not seoul_title:
            continue

        log(f"[file] {p.name}")
        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")
        df_all_frames.append(df)

        counts, med, mean = agg_all_stats(df)
        summary_rows.append((ym, counts, med, mean))

        # ---- 전국 월탭 ----
        ws_nat = ensure_month_ws(sh, nat_title, "전국")
        header_nat = ["날짜"] + NATION_REGIONS
        values_nat = {k: int(counts.get(k, 0)) for k in NATION_REGIONS if k != "총합계"}
        values_nat["총합계"] = int(counts.get("전국", 0))
        write_month_sheet(ws_nat, today_iso, header_nat, values_nat, sheet_id)

        # ---- 서울 월탭 ----
        ws_seoul = ensure_month_ws(sh, seoul_title, "서울")
        header_seoul = ["날짜"] + SEOUL_REGIONS
        values_seoul = {k: int(counts.get(k, 0)) for k in SEOUL_REGIONS if k != "총합계"}
        values_seoul["총합계"] = int(counts.get("서울", 0))
        write_month_sheet(ws_seoul, today_iso, header_seoul, values_seoul, sheet_id)

    # ---- 거래요약 탭 업데이트 ----
    ws_sum = get_or_create_ws(sh, SUMMARY_SHEET_NAME, rows=400, cols=60)

    months = ym_sorted
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

    out_rows = []
    for k, arr in row_map.items():
        out_rows.append([k] + arr)

    ws_update(ws_sum, out_rows, f"A2:{a1_col(len(header))}{len(out_rows)+1}")
    log(f"[summary] wrote rows={len(out_rows)} months={len(months)}")

    # ---- 압구정동 탭 ----
    try:
        df_all = pd.concat(df_all_frames, ignore_index=True) if df_all_frames else pd.DataFrame()
        update_apgujong_tab(sh, df_all)
    except Exception as e:
        log(f"[apgu] ERROR: {e}")

    log("[MAIN] done")

if __name__ == "__main__":
    main()
