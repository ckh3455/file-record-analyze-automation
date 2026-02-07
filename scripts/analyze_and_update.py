# -*- coding: utf-8 -*-
from __future__ import annotations

import os, re, json, time, random
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
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload


"""
analyze_and_update.py (최종본)

목표:
- Google Drive(공유드라이브 포함) 안의 폴더에서 "아파트 YYYYMM.xlsx" 파일만 찾는다
- 최신 12개월치를 다운로드하여 기존 로직대로 집계/시트 업데이트한다
- GitHub Actions artifacts는 절대 쓰지 않는다
- 레포에 새 파일을 만들지 않는다(로컬 캐시는 런타임 폴더에만)

필수 환경변수:
- SHEET_ID
- SA_JSON (또는 GDRIVE_SA_JSON / SA_PATH)

Drive 관련:
- DRIVE_FOLDER_ID : (부모 폴더 또는 아파트 폴더)
- DRIVE_SUBFOLDER_NAME : 기본 '아파트' (부모 폴더일 경우 하위폴더 자동 탐색)
- DRIVE_FILENAME_REGEX : 기본 '^아파트\\s+20\\d{4}(0[1-9]|1[0-2])\\.xlsx$'
- DRIVE_SUPPORTS_ALL_DRIVES : 'true'/'false' (공유드라이브면 true)
- MONTHS_TO_PROCESS : 기본 12
"""

# ===================== 설정 =====================
LOG_DIR = Path("analyze_report")
CACHE_DIR = Path("_drive_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

SUMMARY_SHEET_NAME = "거래요약"
MAX_SCAN_ROWS = int(os.environ.get("MAX_SCAN_ROWS", "900"))

MONTHS_TO_PROCESS = int(os.environ.get("MONTHS_TO_PROCESS", "12"))

DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "").strip()
DRIVE_SUBFOLDER_NAME = os.environ.get("DRIVE_SUBFOLDER_NAME", "아파트").strip()
DRIVE_FILENAME_REGEX = os.environ.get(
    "DRIVE_FILENAME_REGEX", r"^아파트\s+20\d{4}(0[1-9]|1[0-2])\.xlsx$"
).strip()

DRIVE_SUPPORTS_ALL_DRIVES = os.environ.get("DRIVE_SUPPORTS_ALL_DRIVES", "false").strip().lower() in ("1","true","yes","y")

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

_LAST = 0.0
def _throttle(sec: float = 0.6):
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

# ===================== gspread 캐시/쓰기 래퍼 =====================
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
    # ✅ 날짜가 "2026. 2. 7"로 자동 변형되는 걸 막기 위해 RAW 사용
    body = {"valueInputOption": "RAW", "data": data}
    resp = _retry(ws.spreadsheet.values_batch_update, body=body)
    _invalidate_cache(ws)
    return resp

def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

import re as _re
def _norm(s: str) -> str:
    return _re.sub(r"\s+", "", str(s or "").replace("\u3000", "").strip())

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

# ===================== 인증 =====================
def load_creds() -> Credentials:
    # ✅ SA_JSON / GDRIVE_SA_JSON 둘 다 지원
    sa_json = (os.environ.get("SA_JSON", "") or os.environ.get("GDRIVE_SA_JSON", "")).strip()
    sa_path = os.environ.get("SA_PATH", "").strip()

    if sa_json:
        info = json.loads(sa_json)
    elif sa_path:
        info = json.loads(Path(sa_path).read_text(encoding="utf-8"))
    else:
        raise RuntimeError("SA_JSON 또는 GDRIVE_SA_JSON 또는 SA_PATH 환경변수가 필요합니다.")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return Credentials.from_service_account_info(info, scopes=scopes)

# ===================== Drive: 폴더/파일 검색 =====================
def _drive_files_list(service, q: str, fields: str):
    params = {
        "q": q,
        "fields": fields,
        "pageSize": 1000,
        "supportsAllDrives": DRIVE_SUPPORTS_ALL_DRIVES,
        "includeItemsFromAllDrives": DRIVE_SUPPORTS_ALL_DRIVES,
    }
    return service.files().list(**params).execute()

def _find_child_folder_id(service, parent_id: str, child_name: str) -> Optional[str]:
    # parent 안에서 이름이 child_name 인 폴더 검색
    q = (
        f"'{parent_id}' in parents and "
        f"mimeType='application/vnd.google-apps.folder' and "
        f"name='{child_name}' and trashed=false"
    )
    res = _drive_files_list(service, q, "files(id,name)")
    files = res.get("files", [])
    return files[0]["id"] if files else None

def _list_xlsx_in_folder(service, folder_id: str) -> List[Dict]:
    q = (
        f"'{folder_id}' in parents and "
        f"mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and "
        f"trashed=false"
    )
    res = _drive_files_list(service, q, "files(id,name,modifiedTime,size)")
    return res.get("files", [])

def _download_file(service, file_id: str, out_path: Path):
    request = service.files().get_media(fileId=file_id, supportsAllDrives=DRIVE_SUPPORTS_ALL_DRIVES)
    fh = out_path.open("wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()

def pick_apt_month_files_from_drive(creds: Credentials) -> Dict[str, Dict]:
    if not DRIVE_FOLDER_ID:
        raise RuntimeError("DRIVE_FOLDER_ID(부모폴더 또는 아파트폴더)를 시크릿/환경변수로 넣어야 합니다.")

    service = build("drive", "v3", credentials=creds, cache_discovery=False)

    # 1) DRIVE_FOLDER_ID를 “아파트 폴더”로 가정하고 직접 xlsx를 검색
    files = _list_xlsx_in_folder(service, DRIVE_FOLDER_ID)

    # 2) 없으면 DRIVE_FOLDER_ID를 “부모 폴더”로 보고, 그 안의 '아파트' 하위폴더를 찾는다
    if not files:
        child_id = _find_child_folder_id(service, DRIVE_FOLDER_ID, DRIVE_SUBFOLDER_NAME)
        if child_id:
            log(f"[drive] subfolder matched: {DRIVE_SUBFOLDER_NAME} (id={child_id})")
            files = _list_xlsx_in_folder(service, child_id)
        else:
            raise RuntimeError(
                f"Drive 폴더에서 '{DRIVE_SUBFOLDER_NAME}' 하위폴더를 찾지 못했습니다. "
                f"DRIVE_FOLDER_ID가 맞는지/서비스계정이 공유되어 있는지 확인하세요."
            )

    # 파일명 필터: "아파트 YYYYMM.xlsx"
    pat = re.compile(DRIVE_FILENAME_REGEX)
    matched = [f for f in files if pat.match(f.get("name",""))]
    log(f"[drive] matched files={len(matched)} (regex: {DRIVE_FILENAME_REGEX})")

    # ym=YYYYMM 추출
    best: Dict[str, Dict] = {}
    for f in matched:
        m = re.search(r"(20\d{4})(0[1-9]|1[0-2])", f["name"])
        if not m:
            continue
        ym = m.group(1) + m.group(2)
        prev = best.get(ym)
        # modifiedTime 최신 우선
        if (prev is None) or (f.get("modifiedTime","") > prev.get("modifiedTime","")):
            best[ym] = f

    if not best:
        raise RuntimeError(
            "Drive에서 '아파트 YYYYMM.xlsx' 파일을 찾지 못했습니다. "
            "파일명이 정확히 '아파트 202510.xlsx' 형태인지 확인하세요."
        )
    return best

def download_latest_12_months_from_drive(creds: Credentials) -> List[Path]:
    best = pick_apt_month_files_from_drive(creds)

    # 최신 N개월만 선택
    yms = sorted(best.keys(), reverse=True)[:MONTHS_TO_PROCESS]
    yms = sorted(yms)  # 과거->현재 처리

    service = build("drive", "v3", credentials=creds, cache_discovery=False)

    out_paths: List[Path] = []
    for ym in yms:
        f = best[ym]
        out = CACHE_DIR / f["name"]
        log(f"[drive] download {f['name']} -> {out}")
        _download_file(service, f["id"], out)
        out_paths.append(out)

    return out_paths

# ===================== 집계/유틸 =====================
YM_RE = re.compile(r"(20\d{2})년\s*(\d{1,2})월")

def ym_from_filename(fname: str):
    # "아파트 202510.xlsx" -> 전국/서울 시트명은 기존 규칙대로 맞춰줌
    m = re.search(r"(20\d{2})(0[1-9]|1[0-2])", fname)
    if not m:
        return None, None, None
    y, mm = int(m.group(1)), int(m.group(2))
    yy = y % 100
    return f"전국 {y}년 {mm}월", f"서울 {y}년 {mm}월", f"{yy:02d}/{mm:02d}"

def read_month_df(path: Path) -> pd.DataFrame:
    # 너가 올린 아파트 파일도 data 시트로 들어온다고 가정(기존 로직 유지)
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
    log(f"[ws] {ws.title} -> {date_iso} row={row_idx}")

# ===================== 압구정동 탭 로직(기존 유지 + NaN/inf 방지) =====================
APGU_SHEET_NAME = "압구정동"
APGU_BASE_SHEET_NAME = "압구정동_base"

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

    for c in ["광역","구","법정동","본번","부번","단지명","동","층","계약년","계약월","계약일","거래금액(만원)"]:
        if c not in df.columns:
            df[c] = ""

    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    for c in ["본번","부번","동","층","광역","구","법정동","단지명","전용면적(m²)"]:
        df[c] = df[c].astype(str).str.strip().replace({"nan":""})

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

def _df_to_values(df: pd.DataFrame, header: List[str]) -> List[List[str]]:
    # ✅ JSON 에러 방지: NaN/inf 제거 후 문자열화
    df2 = df.copy()
    df2 = df2.replace([np.inf, -np.inf], np.nan).fillna("")
    for h in header:
        if h not in df2.columns:
            df2[h] = ""
    df2 = df2[header].replace({np.nan:""})
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

    prev_vals = _get_all_values_cached(ws_base)
    prev_set = set()
    if prev_vals and len(prev_vals) >= 2:
        header_prev = prev_vals[0]
        if "__k" in header_prev:
            k_idx = header_prev.index("__k")
            for r in prev_vals[1:]:
                if len(r) > k_idx and r[k_idx]:
                    prev_set.add(r[k_idx])

    added_keys = sorted(list(cur_set - prev_set))
    removed_keys = sorted(list(prev_set - cur_set))
    log(f"[apgu] snapshot rows={len(cur)} added={len(added_keys)} removed={len(removed_keys)}")

    # 메인 시트 재작성(기존 정책 유지)
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

    # base 저장(키만)
    ws_clear(ws_base)
    ws_update(ws_base, [["__k"]], "A1")
    base_rows = [[k] for k in sorted(list(cur_set))]
    if base_rows:
        ws_update(ws_base, base_rows, f"A2:A{len(base_rows)+1}")

# ===================== main =====================
def main():
    log("[MAIN] start (Drive -> Local -> Sheets)")

    sheet_id = os.environ.get("SHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("SHEET_ID 환경변수가 필요합니다.")

    creds = load_creds()

    # 1) Drive에서 월별 파일 다운로드
    xlsx_paths = download_latest_12_months_from_drive(creds)
    log(f"[input] drive_xlsx_files={len(xlsx_paths)}")

    # 2) 시트 오픈
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, sheet_id)

    today_iso = datetime.now().date().isoformat()

    df_all_frames: List[pd.DataFrame] = []
    summary_rows = []  # (ym, counts, med, mean)

    # 월별 처리(과거->현재 순)
    months = []
    for p in xlsx_paths:
        nat_title, seoul_title, ym = ym_from_filename(p.name)
        if not ym:
            continue
        months.append((ym, p, nat_title, seoul_title))
    months.sort(key=lambda x: x[0])

    ym_sorted = [x[0] for x in months]
    log(f"[input] months_to_process={ym_sorted}")

    for ym, p, nat_title, seoul_title in months:
        log(f"[file] {p.name}")
        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")
        df_all_frames.append(df)

        counts, med, mean = agg_all_stats(df)
        summary_rows.append((ym, counts, med, mean))

        # 전국 월탭
        ws_nat = ensure_month_ws(sh, nat_title, "전국")
        header_nat = ["날짜"] + NATION_REGIONS
        values_nat = {k: int(counts.get(k, 0)) for k in NATION_REGIONS if k != "총합계"}
        values_nat["총합계"] = int(counts.get("전국", 0))
        write_month_sheet(ws_nat, today_iso, header_nat, values_nat)

        # 서울 월탭
        ws_seoul = ensure_month_ws(sh, seoul_title, "서울")
        header_seoul = ["날짜"] + SEOUL_REGIONS
        values_seoul = {k: int(counts.get(k, 0)) for k in SEOUL_REGIONS if k != "총합계"}
        values_seoul["총합계"] = int(counts.get("서울", 0))
        write_month_sheet(ws_seoul, today_iso, header_seoul, values_seoul)

    # 거래요약 탭
    ws_sum = get_or_create_ws(sh, SUMMARY_SHEET_NAME, rows=400, cols=60)
    header = ["구분"] + ym_sorted
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

    for ym in ym_sorted:
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
    log(f"[summary] wrote rows={len(out_rows)} months={len(ym_sorted)}")

    # 압구정동 탭
    try:
        df_all = pd.concat(df_all_frames, ignore_index=True) if df_all_frames else pd.DataFrame()
        update_apgujong_tab(sh, df_all)
    except Exception as e:
        log(f"[apgu] ERROR: {e}")

    log("[MAIN] done")

if __name__ == "__main__":
    main()
