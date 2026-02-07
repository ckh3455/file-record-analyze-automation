# -*- coding: utf-8 -*-
from __future__ import annotations

"""
analyze_and_update.py  (Drive -> Local -> Sheets)  [전체본/교체용]

✅ 핵심
- artifacts(깃허브 액션 아티팩트)에서 xlsx 찾는 로직 완전 제거
- Google Drive 지정 폴더(부모폴더 + '아파트' 서브폴더)에서만:
    "아파트 YYYYMM.xlsx" 파일을 찾아 최근 12개월 다운로드 후 처리
- 기존 시트 구조/분류 로직(전국/서울 월탭, 거래요약, 압구정동/압구정동_base)은 유지

필수 시크릿/환경변수
- SHEET_ID              : 기록할 Google Spreadsheet ID
- GDRIVE_SA_JSON         : 서비스계정 JSON 문자열 (또는 SA_JSON)

Drive 폴더 지정 (셋 중 하나면 OK)
1) DRIVE_FOLDER_ID                 : '아파트' 폴더 ID (가장 확실)
2) DRIVE_PARENT_FOLDER_ID          : 부모 폴더 ID  + DRIVE_SUBFOLDER_NAME(기본 '아파트')
3) DRIVE_PARENT_FOLDER_URL         : 부모 폴더 URL (ID 자동 추출) + DRIVE_SUBFOLDER_NAME

권장 옵션(공유드라이브)
- DRIVE_SUPPORTS_ALL_DRIVES=true   : shared drive 포함
- DRIVE_CORPUS=allDrives           : shared drive면 allDrives 권장

파일명 규칙
- "아파트 202510.xlsx" 형식만 대상
"""

import os, re, io, json, time, random
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional, Union

import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

try:
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
except Exception as e:
    build = None
    MediaIoBaseDownload = None
    _GOOGLEAPICLIENT_IMPORT_ERR = e
else:
    _GOOGLEAPICLIENT_IMPORT_ERR = None


# ===================== 설정 =====================
LOG_DIR = Path("analyze_report")
SUMMARY_SHEET_NAME = "거래요약"

MAX_SCAN_ROWS = int(os.environ.get("MAX_SCAN_ROWS", "900"))

APT_NAME_RE = re.compile(r"^아파트\s+(\d{6})\.xlsx$", re.IGNORECASE)
FOLDER_URL_RE = re.compile(r"/folders/([a-zA-Z0-9_-]{10,})")

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

YM_TITLE_RE = re.compile(r"(\d{4})년\s*(\d{1,2})월")


# ===================== 로깅 =====================
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


# ===================== gspread 리트라이 =====================
_LAST = 0.0
def _throttle(sec: float = 0.60):
    global _LAST
    now = time.time()
    if now - _LAST < sec:
        time.sleep(sec - (now - _LAST))
    _LAST = time.time()

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


# ===================== 캐시 =====================
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


# ===================== gspread helpers =====================
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

from urllib.parse import quote
def log_focus_link(ws: gspread.Worksheet, row_idx: int, last_col_index: int, sheet_id: str):
    try:
        a1_last = a1_col(last_col_index if last_col_index >= 1 else 1)
        range_a1 = f"{ws.title}!A{row_idx}:{a1_last}{row_idx}"
        link = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={ws.id}&range={quote(range_a1)}"
        with (LOG_DIR / "where_written.txt").open("a", encoding="utf-8") as f:
            f.write(f"[{ws.title}] wrote row {row_idx} → {range_a1}\n")
            f.write(f"Open here: {link}\n")
    except Exception:
        pass

def values_batch_update(spreadsheet: gspread.Spreadsheet, data: List[Dict]):
    def _clean(v):
        if v is None:
            return ""
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            return ""
        return v

    cleaned = []
    for it in data:
        vals = it.get("values", [])
        out = []
        for row in vals:
            out.append([_clean(x) for x in row])
        cleaned.append({"range": it["range"], "values": out})

    body = {"valueInputOption": "USER_ENTERED", "data": cleaned}
    return _retry(spreadsheet.values_batch_update, body=body)


# ===================== 인증 =====================
def load_creds() -> Credentials:
    # repo에 있는 시크릿명 호환
    sa_json = (os.environ.get("GDRIVE_SA_JSON", "") or os.environ.get("SA_JSON", "")).strip()
    sa_path = os.environ.get("SA_PATH", "").strip()

    if sa_json:
        info = json.loads(sa_json)
    elif sa_path:
        info = json.loads(Path(sa_path).read_text(encoding="utf-8"))
    else:
        raise RuntimeError("GDRIVE_SA_JSON(권장) 또는 SA_JSON 또는 SA_PATH 가 필요합니다.")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return Credentials.from_service_account_info(info, scopes=scopes)


# ===================== Drive: 폴더/파일 탐색 =====================
def _env_bool(name: str, default: bool = False) -> bool:
    v = str(os.environ.get(name, "")).strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")

def _extract_folder_id_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = FOLDER_URL_RE.search(url)
    return m.group(1) if m else None

def _build_drive(creds: Credentials):
    if build is None or MediaIoBaseDownload is None:
        raise RuntimeError(
            f"googleapiclient가 설치되어 있지 않습니다: {_GOOGLEAPICLIENT_IMPORT_ERR}\n"
            "requirements.txt에 google-api-python-client 를 추가하세요."
        )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def _drive_list_files(drive, q: str, fields: str, page_size: int = 1000) -> List[dict]:
    supports_all = _env_bool("DRIVE_SUPPORTS_ALL_DRIVES", True)
    corpus = os.environ.get("DRIVE_CORPUS", "allDrives" if supports_all else "user")

    items: List[dict] = []
    page_token = None
    while True:
        resp = drive.files().list(
            q=q,
            fields=fields,
            pageSize=page_size,
            corpora=corpus,
            includeItemsFromAllDrives=supports_all,
            supportsAllDrives=supports_all,
            pageToken=page_token,
        ).execute()
        items.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return items

def resolve_target_folder_id(drive) -> str:
    """
    우선순위
    1) DRIVE_FOLDER_ID (아파트 폴더 ID 직접)
    2) DRIVE_PARENT_FOLDER_ID / DRIVE_PARENT_FOLDER_URL + DRIVE_SUBFOLDER_NAME(기본 아파트)
    3) DRIVE_QUERY (고급)
    """
    folder_id = os.environ.get("DRIVE_FOLDER_ID", "").strip()
    if folder_id:
        return folder_id

    parent_id = os.environ.get("DRIVE_PARENT_FOLDER_ID", "").strip()
    parent_url = os.environ.get("DRIVE_PARENT_FOLDER_URL", "").strip()
    if not parent_id and parent_url:
        parent_id = _extract_folder_id_from_url(parent_url) or ""

    sub_name = (os.environ.get("DRIVE_SUBFOLDER_NAME", "아파트") or "아파트").strip()

    if parent_id:
        # parent 아래에서 이름이 sub_name 인 폴더 찾기
        q = (
            f"trashed=false and mimeType='application/vnd.google-apps.folder' "
            f"and name='{sub_name}' and '{parent_id}' in parents"
        )
        fields = "nextPageToken, files(id,name,parents,driveId,createdTime,modifiedTime)"
        folders = _drive_list_files(drive, q=q, fields=fields, page_size=200)
        if folders:
            # 보통 1개
            fid = folders[0]["id"]
            log(f"[drive] subfolder found: name='{sub_name}' id={fid}")
            return fid
        raise RuntimeError(
            f"부모폴더({parent_id}) 아래에서 서브폴더 '{sub_name}' 를 찾지 못했습니다.\n"
            "1) 서비스계정이 부모폴더(또는 서브폴더)에 공유되어 있는지\n"
            "2) 공유드라이브면 DRIVE_SUPPORTS_ALL_DRIVES=true 인지 확인하세요."
        )

    drive_query = os.environ.get("DRIVE_QUERY", "").strip()
    if drive_query:
        fields = "nextPageToken, files(id,name,parents,driveId,modifiedTime)"
        items = _drive_list_files(drive, q=drive_query, fields=fields, page_size=200)
        # query가 폴더를 반환하도록 쓰는 방식도 지원
        for it in items:
            if it.get("mimeType") == "application/vnd.google-apps.folder":
                log(f"[drive] folder from DRIVE_QUERY: {it.get('name')} id={it.get('id')}")
                return it["id"]
        if items:
            # 파일 쿼리라면 parent로 폴더 추정 불가 → 실패
            raise RuntimeError("DRIVE_QUERY가 폴더를 반환하지 않습니다. 폴더 쿼리로 지정하세요.")
        raise RuntimeError("DRIVE_QUERY 결과가 없습니다. 쿼리를 확인하세요.")

    raise RuntimeError(
        "Drive 폴더를 지정해야 합니다.\n"
        "권장: DRIVE_PARENT_FOLDER_URL + DRIVE_SUBFOLDER_NAME(아파트)\n"
        "또는: DRIVE_FOLDER_ID(아파트 폴더 ID 직접)\n"
        "또는: DRIVE_PARENT_FOLDER_ID + DRIVE_SUBFOLDER_NAME"
    )

def list_apt_month_files(drive, folder_id: str) -> List[dict]:
    q = f"trashed=false and '{folder_id}' in parents"
    fields = "nextPageToken, files(id,name,size,modifiedTime,createdTime,mimeType)"
    items = _drive_list_files(drive, q=q, fields=fields, page_size=1000)
    out = []
    for it in items:
        name = it.get("name", "")
        if APT_NAME_RE.match(name):
            out.append(it)
    out.sort(key=lambda x: x.get("modifiedTime", ""), reverse=True)
    log(f"[drive] matched files={len(out)} (pattern: 아파트 YYYYMM.xlsx)")
    return out

def download_drive_file(drive, file_id: str, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=_env_bool("DRIVE_SUPPORTS_ALL_DRIVES", True))
    fh = io.FileIO(dest, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()

def pick_latest_12_months(files: List[dict]) -> List[dict]:
    """
    파일명 "아파트 YYYYMM.xlsx" 에서 YYYYMM 기준으로 최신 12개월(중복없게)
    """
    best: Dict[str, dict] = {}
    for it in files:
        m = APT_NAME_RE.match(it["name"])
        if not m:
            continue
        yyyymm = m.group(1)
        prev = best.get(yyyymm)
        if prev is None:
            best[yyyymm] = it
        else:
            # modifiedTime 비교
            if it.get("modifiedTime", "") > prev.get("modifiedTime", ""):
                best[yyyymm] = it

    months = sorted(best.keys(), reverse=True)[:12]
    months = sorted(months)  # 처리/기록은 과거->현재
    return [best[m] for m in months]

def download_latest_12_months_from_drive(creds: Credentials) -> List[Path]:
    drive = _build_drive(creds)
    folder_id = resolve_target_folder_id(drive)
    files = list_apt_month_files(drive, folder_id)
    if not files:
        raise RuntimeError(
            "Drive에서 '아파트 YYYYMM.xlsx' 파일을 찾지 못했습니다.\n"
            "1) 서비스계정이 '아파트' 폴더에 공유되어 있는지\n"
            "2) DRIVE_PARENT_FOLDER_URL/ID 또는 DRIVE_FOLDER_ID가 맞는지\n"
            "3) 공유드라이브면 DRIVE_SUPPORTS_ALL_DRIVES=true 인지 확인하세요."
        )

    picked = pick_latest_12_months(files)
    dl_root = Path("_drive_downloads")
    out_paths: List[Path] = []
    for it in picked:
        dest = dl_root / it["name"]
        log(f"[drive] download: {it['name']} -> {dest}")
        download_drive_file(drive, it["id"], dest)
        out_paths.append(dest)

    log(f"[drive] downloaded months={len(out_paths)}")
    return out_paths


# ===================== 파일 읽기/집계 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    # Drive에서 올라온 아파트 파일은 sheet_name='data'로 가정(기존 유지)
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
        if v is None:
            return ""
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            return ""
        return f"{float(v):.2f}"
    except Exception:
        return ""

def _strip_col(df: pd.DataFrame, col: str):
    if col in df.columns:
        df[col] = df[col].astype(str).map(lambda x: str(x).replace("\u3000", " ").strip())
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


# ===================== 월 시트 생성/업데이트 =====================
def yyyymm_to_titles(yyyymm: str) -> Tuple[str, str, str]:
    y = int(yyyymm[:4])
    m = int(yyyymm[4:6])
    return f"전국 {y}년 {m}월", f"서울 {y}년 {m}월", f"{y%100:02d}/{m:02d}"

def ensure_month_ws(sh: gspread.Spreadsheet, title: str, level: str) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, title)
    if ws is not None:
        return ws

    ws = get_or_create_ws(sh, title, rows=1200, cols=40)
    header = ["날짜"] + (NATION_REGIONS if level == "전국" else SEOUL_REGIONS)
    ws_update(ws, [header], "A1")
    log(f"[ws] created from scratch: {title}")
    return ws

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

def find_or_append_date_row(ws: gspread.Worksheet, date_label: Union[str, date, datetime]) -> int:
    target = parse_any_date(date_label) or parse_any_date(str(date_label))
    if not target:
        return 2

    rng = f"A2:A{MAX_SCAN_ROWS}"
    col = _retry(ws.get, rng) or []  # [[val],[val],...]

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
    return MAX_SCAN_ROWS + 1

def write_month_sheet(ws: gspread.Worksheet, date_iso: str, header: List[str], values_by_colname: Dict[str, int], sheet_id: str):
    # 날짜를 "텍스트"로 강제: 앞에 apostrophe
    date_text = "'" + date_iso

    hmap = {str(h).strip(): idx + 1 for idx, h in enumerate(header) if str(h).strip()}
    row_idx = find_or_append_date_row(ws, date_iso)

    sheet_prefix = f"'{ws.title}'!"
    payload = [{"range": f"{sheet_prefix}A{row_idx}", "values": [[date_text]]}]
    for col_name, val in values_by_colname.items():
        if col_name in hmap:
            c = hmap[col_name]
            payload.append({"range": f"{sheet_prefix}{a1_col(c)}{row_idx}", "values": [[int(val)]]})

    values_batch_update(ws.spreadsheet, payload)
    log(f"[ws] {ws.title} -> {date_iso} row={row_idx} (wrote {len(payload)} cells incl. date)")
    log_focus_link(ws, row_idx, len(header or []), sheet_id)


# ===================== 압구정동 탭(기존 로직 유지) =====================
APGU_SHEET_NAME = "압구정동"
APGU_BASE_SHEET_NAME = "압구정동_base"
APGU_KEY_COLS = [
    "광역","구","법정동","본번","부번","단지명","전용면적(m²)",
    "계약년","계약월","계약일","거래금액(만원)","동","층",
]

def _canon_col(s: str) -> str:
    return str(s or "").strip().replace("\u00a0"," ").replace("\u3000"," ")

def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {_canon_col(c): c for c in df.columns}
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
    cols2 = {norm2(c): c for c in df.columns}
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
    return pd.DataFrame(rows, columns=header[:len(rows[0])] if rows else header)

def _df_to_values(df: pd.DataFrame, header: List[str]) -> List[List[str]]:
    df2 = df.copy()
    for h in header:
        if h not in df2.columns:
            df2[h] = ""
    df2 = df2[header]
    # NaN/Inf 제거
    df2 = df2.replace([np.inf, -np.inf], "").fillna("")
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
            "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red": r, "green": g, "blue": b}}}},
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

    ws_main = get_or_create_ws(sh, APGU_SHEET_NAME, rows=2500, cols=40)
    ws_base = get_or_create_ws(sh, APGU_BASE_SHEET_NAME, rows=2500, cols=40)
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

    base_header = ["__k"] + [c for c in APGU_KEY_COLS if c in cur.columns]
    base_header = list(dict.fromkeys(base_header))

    ws_clear(ws_base)
    ws_update(ws_base, [base_header], "A1")

    base_df2 = _ensure_cols(cur.copy())
    kdf = _make_key_df(base_df2)
    kdf["__k"] = kdf.apply(lambda r: "|".join(r.values.tolist()), axis=1)
    base_df2["__k"] = kdf["__k"].values

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

    creds = load_creds()

    # 1) Drive에서 최근 12개월 다운로드
    xlsx_paths = download_latest_12_months_from_drive(creds)

    # 2) Sheets 연결
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, sheet_id)

    today_iso = datetime.now().date().isoformat()

    df_all_frames: List[pd.DataFrame] = []
    summary_rows = []  # (ym, counts, med, mean)

    for p in xlsx_paths:
        m = APT_NAME_RE.match(p.name)
        if not m:
            continue
        yyyymm = m.group(1)
        nat_title, seoul_title, ym = yyyymm_to_titles(yyyymm)

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

    # ---- 거래요약 탭 ----
    ws_sum = get_or_create_ws(sh, SUMMARY_SHEET_NAME, rows=400, cols=60)

    months = [ym for ym, _, _, _ in summary_rows]
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

    out_rows = [[k] + v for k, v in row_map.items()]
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
