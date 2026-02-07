# -*- coding: utf-8 -*-
from __future__ import annotations

"""
analyze_and_update.py (최종 전체본)

목표:
- Google Drive(공유드라이브 포함)에서 '아파트 YYYYMM.xlsx' 파일(최근 12개월)을 다운로드
- 각 월 파일의 data 시트로부터 통계를 계산해 Google Sheets(월탭/요약탭/압구정동 탭)를 업데이트

핵심 수정:
1) Drive가 1순위: artifacts 의존 제거 (Drive 실패 시 즉시 에러)
2) 공유드라이브에서 list가 0개 뜨는 문제 해결:
   - files.get(folder_id)로 driveId 먼저 획득
   - driveId 있으면 files.list(corpora='drive', driveId=...)로 재조회
3) SA_JSON 혼선 해결:
   - SA_JSON 없으면 GDRIVE_SA_JSON도 허용
   - SA_PATH 없으면 GDRIVE_SA_PATH도 허용
4) NaN/inf가 JSON으로 들어가며 발생하는
   "Out of range float values are not JSON compliant" 방지:
   - 시트에 쓰기 전에 NaN/inf를 빈 문자열로 치환

필수 환경변수:
- SHEET_ID : 기록할 Google Sheets ID
- SA_JSON (또는 GDRIVE_SA_JSON): 서비스계정 JSON 전체 문자열
- DRIVE_FOLDER_ID : '아파트' 폴더 ID (아파트 파일들이 실제로 들어있는 그 폴더)
- DRIVE_SUPPORTS_ALL_DRIVES : 공유드라이브면 true 권장

파일명 규칙:
- 아파트 200601.xlsx  (YYYYMM 6자리)
"""

import os, re, json, time, random
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional, Union

import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# google drive download
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError


# ===================== 설정 =====================
LOG_DIR = Path("analyze_report")
SUMMARY_SHEET_NAME = "거래요약"

MAX_SCAN_ROWS = int(os.environ.get("MAX_SCAN_ROWS", "900"))

# Drive
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "").strip()
DRIVE_SUPPORTS_ALL_DRIVES = os.environ.get("DRIVE_SUPPORTS_ALL_DRIVES", "false").strip()
DRIVE_FILE_REGEX = os.environ.get("DRIVE_FILE_REGEX", r"^아파트\s*(\d{6})\.xlsx$").strip()
APT_RE = re.compile(DRIVE_FILE_REGEX)

DOWNLOAD_DIR = Path(os.environ.get("DRIVE_DOWNLOAD_DIR", "_drive_downloads")).resolve()
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

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


# ===================== helper =====================
def _as_bool(x: str, default: bool = False) -> bool:
    if x is None:
        return default
    s = str(x).strip().lower()
    if s in ("1","true","yes","y","on"):
        return True
    if s in ("0","false","no","n","off"):
        return False
    return default

def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# ===================== gspread “쓰기” 래퍼 =====================
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
    body = {"valueInputOption": "USER_ENTERED", "data": data}
    resp = _retry(ws.spreadsheet.values_batch_update, body=body)
    _invalidate_cache(ws)
    return resp

def fuzzy_ws(sh: gspread.Spreadsheet, wanted: str) -> Optional[gspread.Worksheet]:
    tgt = re.sub(r"\s+", "", wanted.replace("\u3000", "").strip())
    for ws in sh.worksheets():
        t = re.sub(r"\s+", "", ws.title.replace("\u3000", "").strip())
        if t == tgt:
            log(f"[ws] matched: '{ws.title}' (wanted='{wanted}')")
            return ws
    return None

def get_or_create_ws(sh: gspread.Spreadsheet, title: str, rows: int = 100, cols: int = 20) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, title)
    if ws is None:
        ws = _retry(sh.add_worksheet, title=title, rows=rows, cols=cols)
        log(f"[ws] created: {title}")
    return ws


# ===================== 년월 정규화 =====================
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
    """
    Drive 파일명: 아파트 200601.xlsx => yyyymm=200601
    월탭 타이틀: 전국 2020년 6월 / 서울 2020년 6월, ym: 20/06
    """
    s = str(fname or "")
    m = re.search(r"(\d{6})", s)
    if not m:
        return None, None, None
    yyyymm = m.group(1)
    yy = int(yyyymm[:2])
    mm = int(yyyymm[4:6])
    y = 2000 + yy
    if not (1 <= mm <= 12):
        return None, None, None
    return f"전국 {y}년 {mm}월", f"서울 {y}년 {mm}월", f"{yy:02d}/{mm:02d}"


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
    # Drive xlsx는 통상 sheet_name='data'
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
    """df(data 시트)로부터 '전국/광역/서울/구/압구정동' 카운트, 중앙값, 평균"""
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


# ===================== 월 시트(전국/서울) 생성/확보 =====================
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
    """A열을 A2:A{MAX_SCAN_ROWS} 고정범위로 스캔해서 같은 날짜면 그 행, 없으면 첫 빈행."""
    target = parse_any_date(date_label) or parse_any_date(str(date_label))
    if not target:
        return 2

    rng = f"A2:A{MAX_SCAN_ROWS}"
    col = _retry(ws.get, rng) or []   # [[val],[val],...]
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


# ===================== 인증/시트 열기 =====================
def load_creds():
    # ✅ SA_JSON / SA_PATH 이름 혼선 해결
    sa_json = os.environ.get("SA_JSON", "").strip() or os.environ.get("GDRIVE_SA_JSON", "").strip()
    sa_path = os.environ.get("SA_PATH", "").strip() or os.environ.get("GDRIVE_SA_PATH", "").strip()

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


# ===================== Drive: 공유드라이브 안정 list + download =====================
def get_folder_meta(drive, folder_id: str, supports_all_drives: bool) -> dict:
    kwargs = {"fileId": folder_id, "fields": "id,name,mimeType,driveId,parents"}
    if supports_all_drives:
        kwargs["supportsAllDrives"] = True
    try:
        return drive.files().get(**kwargs).execute()
    except HttpError as e:
        raise RuntimeError(f"Drive 폴더 메타 조회 실패(folder_id={folder_id}): {e}")

def list_files_in_folder_any(drive, folder_id: str, supports_all_drives: bool, drive_id: Optional[str]) -> list[dict]:
    q = f"'{folder_id}' in parents and trashed=false"
    files: list[dict] = []
    page_token = None

    while True:
        kwargs = dict(
            q=q,
            fields="nextPageToken, files(id,name,mimeType,modifiedTime,size)",
            pageSize=1000,
        )

        if supports_all_drives:
            kwargs.update(
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )

        # ✅ 공유드라이브면 driveId 기반 corpora='drive'가 가장 안정적
        if drive_id:
            kwargs["corpora"] = "drive"
            kwargs["driveId"] = drive_id

        try:
            resp = drive.files().list(pageToken=page_token, **kwargs).execute()
        except HttpError as e:
            raise RuntimeError(f"Drive list 실패(folder_id={folder_id}, driveId={drive_id}): {e}")

        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return files

def pick_latest_12_months_from_drive(drive, folder_id: str, supports_all_drives: bool) -> list[tuple[str, dict]]:
    folder_meta = get_folder_meta(drive, folder_id, supports_all_drives)
    drive_id = folder_meta.get("driveId")  # shared drive면 존재

    log(f"[drive] folder meta name='{folder_meta.get('name')}', driveId={'YES' if drive_id else 'NO'}")

    all_files = list_files_in_folder_any(drive, folder_id, supports_all_drives, drive_id)
    log(f"[drive] listed total files={len(all_files)}")

    matched: list[tuple[str, dict]] = []
    for f in all_files:
        name = f.get("name", "")
        m = APT_RE.match(name)
        if not m:
            continue
        yyyymm = m.group(1)
        if len(yyyymm) == 6 and yyyymm.isdigit():
            matched.append((yyyymm, f))

    log(f"[drive] matched_apt={len(matched)} regex='{DRIVE_FILE_REGEX}'")

    if not matched:
        sample = [x.get("name","") for x in all_files[:30]]
        raise RuntimeError(
            "Drive 폴더에서 '아파트 YYYYMM.xlsx' 파일을 찾지 못했습니다.\n"
            f"- folder='{folder_meta.get('name')}' driveId={'있음' if drive_id else '없음'} total_files={len(all_files)}\n"
            f"- 예시 파일(상위 30개): {sample}\n"
            "확인사항:\n"
            "1) DRIVE_FOLDER_ID가 '아파트 폴더 자체'인지\n"
            "2) 서비스계정이 해당 폴더(공유드라이브)에 공유됐는지\n"
            "3) 파일명이 정확히 '아파트 200601.xlsx' 형태인지(공백 포함)\n"
        )

    best: dict[str, dict] = {}
    for yyyymm, meta in matched:
        if yyyymm not in best or meta.get("modifiedTime","") > best[yyyymm].get("modifiedTime",""):
            best[yyyymm] = meta

    yms = sorted(best.keys(), reverse=True)[:12]
    yms = sorted(yms)  # 과거 -> 최신
    return [(yyyymm, best[yyyymm]) for yyyymm in yms]

def download_file(drive, file_id: str, dest: Path, supports_all_drives: bool):
    dest.parent.mkdir(parents=True, exist_ok=True)
    req_kwargs = {"fileId": file_id}
    if supports_all_drives:
        req_kwargs["supportsAllDrives"] = True
    req = drive.files().get_media(**req_kwargs)

    with dest.open("wb") as fh:
        downloader = MediaIoBaseDownload(fh, req, chunksize=1024 * 1024 * 4)
        done = False
        while not done:
            _, done = downloader.next_chunk()

def download_latest_12_months_from_drive(creds: Credentials) -> List[Path]:
    if not DRIVE_FOLDER_ID:
        raise RuntimeError("DRIVE_FOLDER_ID(아파트 폴더 ID)를 설정하세요.")

    supports_all_drives = _as_bool(DRIVE_SUPPORTS_ALL_DRIVES, default=False)

    drive = build("drive", "v3", credentials=creds, cache_discovery=False)

    picked = pick_latest_12_months_from_drive(drive, DRIVE_FOLDER_ID, supports_all_drives)
    paths: List[Path] = []

    for yyyymm, meta in picked:
        fid = meta["id"]
        name = meta.get("name") or f"아파트 {yyyymm}.xlsx"
        out = DOWNLOAD_DIR / name
        log(f"[drive] download {name} -> {out}")
        download_file(drive, fid, out, supports_all_drives)
        paths.append(out)

    log(f"[drive] downloaded files={len(paths)} to {DOWNLOAD_DIR}")
    return paths


# ===================== 압구정동 탭(원본 스크립트 유지, 단 NaN/inf 방지) =====================
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
    df["동"] = df.get("동","").astype(str).str.strip().replace({"nan":""})
    df["층"] = df.get("층","").astype(str).str.strip().replace({"nan":""})

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

    # ✅ JSON 에러 방지: NaN/inf 제거
    df2 = df2.replace([np.inf, -np.inf], "")
    df2 = df2.fillna("")

    return df2.astype(str).values.tolist()

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

    header2 = ["변동"] + header if "변동" not in header else header

    ws_clear(ws_main)
    ws_update(ws_main, [header2], "A1")

    cur_out = cur.copy()
    if "변동" not in cur_out.columns:
        cur_out.insert(0, "변동", "")
    values_cur = _df_to_values(cur_out, header2)
    if values_cur:
        ws_update(ws_main, values_cur, f"A2:{a1_col(len(header2))}{len(values_cur)+1}")

    # base 저장
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

    log("[apgu] updated main/base")


# ===================== 메인 =====================
def main():
    log("[MAIN] start (Drive -> Local -> Sheets)")

    sheet_id = os.environ.get("SHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("SHEET_ID 환경변수가 필요합니다.")

    creds = load_creds()

    # 1) Drive에서 최근 12개월 다운로드 (artifacts 의존 제거)
    xlsx_paths = download_latest_12_months_from_drive(creds)

    # 2) Sheets 연결
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, sheet_id)

    today_iso = datetime.now().date().isoformat()

    df_all_frames: List[pd.DataFrame] = []
    summary_rows = []  # (ym, counts, med, mean)

    # ym 정렬(파일명에서 추출)
    by_ym: Dict[str, Path] = {}
    for p in xlsx_paths:
        nat_title, seoul_title, ym = ym_from_filename(p.name)
        if ym:
            by_ym[ym] = p

    def ym_key(ym: str):
        yy, mm = ym.split("/")
        return (2000 + int(yy), int(mm))

    months = sorted(by_ym.keys(), key=ym_key)

    log(f"[input] months_to_process={months}")

    for ym in months:
        p = by_ym[ym]
        nat_title, seoul_title, _ = ym_from_filename(p.name)
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

    # ---- 거래요약 탭 ----
    ws_sum = get_or_create_ws(sh, SUMMARY_SHEET_NAME, rows=400, cols=60)

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
