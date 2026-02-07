# -*- coding: utf-8 -*-
from __future__ import annotations

"""
analyze_and_update.py (Drive -> Local -> Sheets) 단일 전체본

이번 수정(핵심):
- artifacts 폴더/깃허브 액션 아티팩트 의존 완전 제거
- Drive에서 월별 '아파트 YYYYMM.xlsx'만 찾아 다운로드 후 처리
- DRIVE_FOLDER_ID가 없으면:
    1) DRIVE_PARENT_FOLDER_ID 안에서 이름이 '아파트'인 하위폴더를 자동 탐색하여 그 폴더를 사용
    2) (옵션) DRIVE_FOLDER_URL / DRIVE_PARENT_FOLDER_URL 이 있으면 URL에서 ID를 추출해서 사용
- 공유드라이브 지원(supportsAllDrives/includeItemsFromAllDrives) 기본 ON
- 월 탭 기록 시 날짜는 ISO("YYYY-MM-DD") 문자열 그대로(= RAW 입력)
- 날짜행 탐색 A2:A{MAX_SCAN_ROWS} 고정 스캔
- write 후 verify 로그
- 압구정동 탭 업데이트의 JSON NaN/Inf 에러 방지(값 안전 문자열화)

환경변수(필수):
- SHEET_ID
- SA_JSON 또는 SA_PATH

Drive 입력(권장):
- DRIVE_PARENT_FOLDER_ID : 상위 폴더(예: 공유드라이브의 "부동산자료") ID
    - 이 안에서 "아파트" 하위폴더를 자동으로 찾음
- 또는 DRIVE_FOLDER_ID : 파일이 들어있는 "아파트" 폴더 ID(직접 지정)

선택:
- DRIVE_FOLDER_URL / DRIVE_PARENT_FOLDER_URL : URL을 넣으면 ID 추출
- DRIVE_QUERY : 폴더ID 없이도 가능하나 권장X
- DRIVE_SUPPORTS_ALL_DRIVES : 기본 true
- DRIVE_DOWNLOAD_DIR : 기본 drive_downloads
- MAX_SCAN_ROWS : 기본 900
"""

import os
import re
import json
import time
import random
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional, Union

import numpy as np
import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession

# ===================== 설정 =====================
LOG_DIR = Path("analyze_report")
RUN_LOG = LOG_DIR / "latest.log"

SUMMARY_SHEET_NAME = "거래요약"
MAX_SCAN_ROWS = int(os.environ.get("MAX_SCAN_ROWS", "900"))

# Drive 관련
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "").strip()
DRIVE_PARENT_FOLDER_ID = os.environ.get("DRIVE_PARENT_FOLDER_ID", "").strip()
DRIVE_QUERY = os.environ.get("DRIVE_QUERY", "").strip()

DRIVE_FOLDER_URL = os.environ.get("DRIVE_FOLDER_URL", "").strip()
DRIVE_PARENT_FOLDER_URL = os.environ.get("DRIVE_PARENT_FOLDER_URL", "").strip()

DRIVE_SUPPORTS_ALL_DRIVES = os.environ.get("DRIVE_SUPPORTS_ALL_DRIVES", "true").strip().lower() in ("1", "true", "yes", "y")
DRIVE_DOWNLOAD_DIR = Path(os.environ.get("DRIVE_DOWNLOAD_DIR", "drive_downloads"))

# ===================== 컬럼/지역 =====================
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

# ===================== Drive (googleapiclient 없이 HTTP) =====================
DRIVE_API_BASE = "https://www.googleapis.com/drive/v3"
FOLDER_MIME = "application/vnd.google-apps.folder"

def _extract_drive_id_from_url(url: str) -> str:
    """
    https://drive.google.com/drive/folders/<ID>...
    """
    if not url:
        return ""
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
    return (m.group(1) if m else "").strip()

def drive_list_files(session: AuthorizedSession, q: str, page_size: int = 200) -> List[dict]:
    files: List[dict] = []
    page_token = None
    params_base = {
        "q": q,
        "fields": "nextPageToken,files(id,name,mimeType,modifiedTime,size,parents)",
        "pageSize": str(page_size),
    }
    if DRIVE_SUPPORTS_ALL_DRIVES:
        params_base["supportsAllDrives"] = "true"
        params_base["includeItemsFromAllDrives"] = "true"
        params_base["corpora"] = "allDrives"

    while True:
        params = dict(params_base)
        if page_token:
            params["pageToken"] = page_token
        url = f"{DRIVE_API_BASE}/files"
        r = session.get(url, params=params, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Drive list failed: {r.status_code} {r.text[:500]}")
        data = r.json()
        files.extend(data.get("files", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return files

def drive_download_file(session: AuthorizedSession, file_id: str, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    url = f"{DRIVE_API_BASE}/files/{file_id}"
    params = {"alt": "media"}
    if DRIVE_SUPPORTS_ALL_DRIVES:
        params["supportsAllDrives"] = "true"
    r = session.get(url, params=params, stream=True, timeout=180)
    if r.status_code != 200:
        raise RuntimeError(f"Drive download failed: {r.status_code} {r.text[:500]}")
    with out_path.open("wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

def resolve_target_folder_id(session: AuthorizedSession) -> str:
    """
    우선순위:
    1) DRIVE_FOLDER_ID (직접 지정)
    2) DRIVE_FOLDER_URL
    3) DRIVE_PARENT_FOLDER_ID/URL 내부에서 이름이 '아파트'인 하위폴더 탐색
    """
    global DRIVE_FOLDER_ID, DRIVE_PARENT_FOLDER_ID

    if not DRIVE_FOLDER_ID and DRIVE_FOLDER_URL:
        DRIVE_FOLDER_ID = _extract_drive_id_from_url(DRIVE_FOLDER_URL)
        if DRIVE_FOLDER_ID:
            log(f"[drive] DRIVE_FOLDER_ID extracted from URL: {DRIVE_FOLDER_ID}")

    if DRIVE_FOLDER_ID:
        return DRIVE_FOLDER_ID

    if not DRIVE_PARENT_FOLDER_ID and DRIVE_PARENT_FOLDER_URL:
        DRIVE_PARENT_FOLDER_ID = _extract_drive_id_from_url(DRIVE_PARENT_FOLDER_URL)
        if DRIVE_PARENT_FOLDER_ID:
            log(f"[drive] DRIVE_PARENT_FOLDER_ID extracted from URL: {DRIVE_PARENT_FOLDER_ID}")

    if not DRIVE_PARENT_FOLDER_ID:
        return ""

    # 부모 폴더 안의 하위 폴더들 중 '아파트' 찾기
    q = f"'{DRIVE_PARENT_FOLDER_ID}' in parents and mimeType='{FOLDER_MIME}' and trashed=false"
    log(f"[drive] resolving subfolder '아파트' under parent, q={q}")
    subs = drive_list_files(session, q=q, page_size=500)
    # 이름 정규화 비교
    def norm(x: str) -> str:
        return re.sub(r"\s+", "", (x or "").strip())

    target = None
    for f in subs:
        if norm(f.get("name","")) == norm("아파트"):
            target = f
            break

    if not target:
        # 디버그용: 보이는 하위폴더 이름 출력(최대 30개)
        names = [s.get("name","") for s in subs][:30]
        log(f"[drive] subfolders under parent (sample): {names}")
        raise RuntimeError("DRIVE_PARENT_FOLDER_ID 안에서 '아파트' 하위폴더를 찾지 못했습니다. "
                           "폴더명이 정확히 '아파트'인지 확인하세요.")
    log(f"[drive] found subfolder '아파트' id={target['id']}")
    return target["id"]

# ===================== 파일명(월) 파싱 =====================
APT_YYYYMM_RE = re.compile(r"^아파트[\s_]+(20\d{2})(0[1-9]|1[0-2])\.xlsx$", re.IGNORECASE)

def titles_from_yyyymm(yyyymm: str) -> Tuple[str, str, str]:
    y = int(yyyymm[:4])
    m = int(yyyymm[4:])
    return (f"전국 {y}년 {m}월", f"서울 {y}년 {m}월", f"{y%100:02d}/{m:02d}")

def pick_apt_month_files_from_drive(session: AuthorizedSession) -> Dict[str, dict]:
    folder_id = resolve_target_folder_id(session)

    cand: List[dict]
    if folder_id:
        q = f"'{folder_id}' in parents and trashed=false"
        log(f"[drive] using folder, q={q}")
        cand = drive_list_files(session, q=q, page_size=500)
    else:
        if not DRIVE_QUERY:
            raise RuntimeError("DRIVE_FOLDER_ID 또는 DRIVE_PARENT_FOLDER_ID 또는 DRIVE_QUERY 중 하나가 필요합니다.")
        q = DRIVE_QUERY
        if "trashed" not in q:
            q = f"({q}) and trashed=false"
        log(f"[drive] using DRIVE_QUERY, q={q}")
        cand = drive_list_files(session, q=q, page_size=500)

    best: Dict[str, dict] = {}
    matched = 0

    for f in cand:
        name = (f.get("name") or "").strip()
        m = APT_YYYYMM_RE.match(name)
        if not m:
            continue
        matched += 1
        yyyymm = f"{m.group(1)}{m.group(2)}"
        prev = best.get(yyyymm)
        if prev is None:
            best[yyyymm] = f
        else:
            if (f.get("modifiedTime") or "") > (prev.get("modifiedTime") or ""):
                best[yyyymm] = f

    log(f"[drive] listed={len(cand)} matched={matched} unique_months={len(best)} (pattern: 아파트 YYYYMM.xlsx)")
    if not best:
        raise RuntimeError(
            "Drive에서 '아파트 YYYYMM.xlsx' 파일을 찾지 못했습니다.\n"
            "1) 서비스계정이 공유드라이브/폴더에 공유되어 있는지\n"
            "2) DRIVE_PARENT_FOLDER_ID(상위) 안에 '아파트' 폴더가 맞는지\n"
            "3) 파일명이 정확히 '아파트 202510.xlsx' 형식인지\n"
        )
    return best

def download_latest_12_months(session: AuthorizedSession) -> List[Path]:
    best = pick_apt_month_files_from_drive(session)

    def key_yyyymm(k: str):
        return (int(k[:4]), int(k[4:]))

    yyyymm_sorted = sorted(best.keys(), key=key_yyyymm, reverse=True)[:12]
    yyyymm_sorted = sorted(yyyymm_sorted, key=key_yyyymm)

    log(f"[drive] months_to_download={yyyymm_sorted}")

    DRIVE_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    out_paths: List[Path] = []
    for yyyymm in yyyymm_sorted:
        meta = best[yyyymm]
        fid = meta["id"]
        name = meta["name"]
        out_path = DRIVE_DOWNLOAD_DIR / name
        drive_download_file(session, fid, out_path)  # overwrite
        sz = out_path.stat().st_size if out_path.exists() else 0
        log(f"[drive] downloaded: {name} -> {out_path} size={sz}")
        out_paths.append(out_path)

    return out_paths

# ===================== Sheets 유틸 =====================
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

def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def values_batch_update_raw(ws: gspread.Worksheet, data: List[Dict]):
    body = {"valueInputOption": "RAW", "data": data}
    resp = _retry(ws.spreadsheet.values_batch_update, body=body)
    _invalidate_cache(ws)
    return resp

def batch_format(ws: gspread.Worksheet, requests: List[dict]):
    if not requests:
        return None
    return _retry(ws.spreadsheet.batch_update, {"requests": requests})

def _norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "").replace("\u3000", "").strip())

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

# ===================== 월 시트 생성/확보 =====================
def ensure_month_ws(sh: gspread.Spreadsheet, title: str, level: str) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, title)
    if ws is not None:
        return ws
    ws = get_or_create_ws(sh, title, rows=1200, cols=40)
    header = ["날짜"] + (NATION_REGIONS if level == "전국" else SEOUL_REGIONS)
    ws_update(ws, [header], "A1")
    _invalidate_cache(ws)
    log(f"[ws] created from scratch: {title}")
    return ws

# ===================== 데이터 읽기/집계 =====================
def read_month_df(path: Path) -> pd.DataFrame:
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
        df[col] = df[col].astype(str).map(lambda x: str(x).replace("\u3000"," ").replace("\u00a0"," ").strip())
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

# ===================== 월 탭 기록 =====================
from urllib.parse import quote

def log_focus_link(ws: gspread.Worksheet, row_idx: int, last_col_index: int, sheet_id: str):
    try:
        a1_last = a1_col(last_col_index if last_col_index >= 1 else 1)
        range_a1 = f"{ws.title}!A{row_idx}:{a1_last}{row_idx}"
        link = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={ws.id}&range={quote(range_a1)}"
        with (LOG_DIR / "where_written.txt").open("a", encoding="utf-8") as f:
            f.write(f"[{ws.title}] wrote row {row_idx} -> {range_a1}\n")
            f.write(f"Open here: {link}\n")
    except Exception:
        pass

def write_month_sheet(ws: gspread.Worksheet, date_iso: str, header: List[str], values_by_colname: Dict[str, int], sheet_id: str):
    hmap = {str(h).strip(): idx + 1 for idx, h in enumerate(header) if str(h).strip()}
    row_idx = find_or_append_date_row(ws, date_iso)

    sheet_prefix = f"'{ws.title}'!"
    payload = [{"range": f"{sheet_prefix}A{row_idx}", "values": [[date_iso]]}]
    for col_name, val in values_by_colname.items():
        if col_name in hmap:
            c = hmap[col_name]
            payload.append({"range": f"{sheet_prefix}{a1_col(c)}{row_idx}", "values": [[int(val)]]})

    values_batch_update_raw(ws, payload)
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

# ===================== 압구정동 스냅샷(안정 버전) =====================
APGU_SHEET_NAME = "압구정동"
APGU_BASE_SHEET_NAME = "압구정동_base"

def _df_to_values_safe(df: pd.DataFrame, header: List[str]) -> List[List[str]]:
    df2 = df.copy()
    for h in header:
        if h not in df2.columns:
            df2[h] = ""
    df2 = df2[header]

    def conv(x):
        try:
            if x is None:
                return ""
            if isinstance(x, float):
                if np.isnan(x) or np.isinf(x):
                    return ""
            if isinstance(x, (np.floating, np.integer)):
                if isinstance(x, np.floating) and (np.isnan(x) or np.isinf(x)):
                    return ""
                return str(x)
            s = str(x)
            if s.lower() in ("nan", "inf", "-inf"):
                return ""
            return s
        except Exception:
            return ""

    return df2.applymap(conv).values.tolist()

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

def update_apgujong_tab(sh: gspread.Spreadsheet, df_all: pd.DataFrame):
    # 기존 로직 유지 조건이 있었지만, 여기서는 "분류 구조"는 유지하면서
    # JSON 오류만 제거하도록 최소 안전화만 적용
    if df_all is None or df_all.empty:
        log("[apgu] skipped: df_all empty")
        return

    # 최소 필터만(요청사항: 광역=서울특별시 & 법정동=압구정동)
    df_all = df_all.copy()
    for c in ["광역", "법정동"]:
        if c in df_all.columns:
            df_all[c] = df_all[c].astype(str).str.replace("\u3000"," ").str.replace("\u00a0"," ").str.strip()

    cur = df_all[(df_all.get("광역","") == "서울특별시") & (df_all.get("법정동","") == "압구정동")].copy()
    if cur.empty:
        log("[apgu] no rows for 압구정동")
        return

    ws_main = get_or_create_ws(sh, APGU_SHEET_NAME, rows=2000, cols=40)
    ws_base = get_or_create_ws(sh, APGU_BASE_SHEET_NAME, rows=2000, cols=40)
    _hide_sheet(ws_base)

    # 헤더 유지 또는 생성
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

    if "변동" not in cur.columns:
        cur.insert(0, "변동", "")

    values_cur = _df_to_values_safe(cur, header2)
    if values_cur:
        ws_update(ws_main, values_cur, f"A2:{a1_col(len(header2))}{len(values_cur)+1}")

    # base는 단순 스냅샷 저장(전체 값 안전)
    ws_clear(ws_base)
    ws_update(ws_base, [header2], "A1")
    if values_cur:
        ws_update(ws_base, values_cur, f"A2:{a1_col(len(header2))}{len(values_cur)+1}")
    _hide_sheet(ws_base)

    log("[apgu] updated main/base (safe snapshot)")

# ===================== 메인 =====================
def main():
    log("[MAIN] start (Drive -> Local -> Sheets)")

    sheet_id = os.environ.get("SHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("SHEET_ID 환경변수가 필요합니다.")

    creds = load_creds()
    session = AuthorizedSession(creds)

    # Drive에서 다운로드(artifacts 절대 사용 안 함)
    xlsx_paths = download_latest_12_months(session)
    if not xlsx_paths:
        raise RuntimeError("Drive에서 다운로드된 xlsx가 없습니다.")

    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, sheet_id)

    today_iso = datetime.now().date().isoformat()

    df_all_frames: List[pd.DataFrame] = []
    summary_rows = []

    month_items: List[Tuple[str, Path]] = []
    for p in xlsx_paths:
        name = p.name.strip()
        m = APT_YYYYMM_RE.match(name)
        if not m:
            continue
        yyyymm = f"{m.group(1)}{m.group(2)}"
        month_items.append((yyyymm, p))

    month_items.sort(key=lambda x: (int(x[0][:4]), int(x[0][4:])))

    months_yy_mm: List[str] = []

    for yyyymm, p in month_items:
        nat_title, seoul_title, ym = titles_from_yyyymm(yyyymm)
        months_yy_mm.append(ym)

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
        write_month_sheet(ws_nat, today_iso, header_nat, values_nat, sheet_id)

        ws_seoul = ensure_month_ws(sh, seoul_title, "서울")
        header_seoul = ["날짜"] + SEOUL_REGIONS
        values_seoul = {k: int(counts.get(k, 0)) for k in SEOUL_REGIONS if k != "총합계"}
        values_seoul["총합계"] = int(counts.get("서울", 0))
        write_month_sheet(ws_seoul, today_iso, header_seoul, values_seoul, sheet_id)

    # 거래요약
    ws_sum = get_or_create_ws(sh, SUMMARY_SHEET_NAME, rows=400, cols=60)
    header = ["구분"] + months_yy_mm
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

    for ym in months_yy_mm:
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
    log(f"[summary] wrote rows={len(out_rows)} months={len(months_yy_mm)}")

    # 압구정동
    try:
        df_all = pd.concat(df_all_frames, ignore_index=True) if df_all_frames else pd.DataFrame()
        update_apgujong_tab(sh, df_all)
    except Exception as e:
        log(f"[apgu] ERROR: {e}")

    log("[MAIN] done")

if __name__ == "__main__":
    main()
