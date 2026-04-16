# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import json
import time
import random
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# ===================== 설정 =====================
LOG_DIR = Path("analyze_report")
RUN_LOG = LOG_DIR / "latest.log"
DOWNLOAD_DIR = Path(os.environ.get("DOWNLOAD_DIR", "_drive_downloads"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

DRIVE_FILE_REGEX = os.environ.get("DRIVE_FILE_REGEX", r"^아파트\s*(\d{6})\.xlsx$")
APT_FILE_RE = re.compile(DRIVE_FILE_REGEX)
DRIVE_SCAN_MAX_FILES = int(os.environ.get("DRIVE_SCAN_MAX_FILES", "1000"))

MONTH_WS_MIN_ROWS = int(os.environ.get("MONTH_WS_MIN_ROWS", "400"))
MONTH_WS_MIN_EXTRA = int(os.environ.get("MONTH_WS_MIN_EXTRA", "30"))
MONTH_WS_MIN_COLS = int(os.environ.get("MONTH_WS_MIN_COLS", "40"))

SUMMARY_SHEET_NAME = "거래요약"

SEOUL_REGIONS = [
    "강남구", "강동구", "강북구", "강서구", "관악구", "광진구", "구로구", "금천구", "노원구", "도봉구",
    "동대문구", "동작구", "마포구", "서대문구", "서초구", "성동구", "성북구", "송파구", "양천구", "영등포구",
    "용산구", "은평구", "종로구", "중구", "중랑구", "총합계"
]

NATION_REGIONS = [
    "강원도", "경기도", "경상남도", "경상북도", "광주광역시", "대구광역시", "대전광역시", "부산광역시",
    "서울특별시", "세종특별자치시", "울산광역시", "인천광역시", "전라남도", "전북특별자치도", "제주특별자치도",
    "충청남도", "충청북도", "총합계"
]


# ===================== 공통 =====================
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


def _bool_env(name: str, default: bool = False) -> bool:
    v = str(os.environ.get(name, str(default))).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _extract_id(x: str) -> str:
    if not x:
        return ""
    x = x.strip()
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", x)
    if m:
        return m.group(1)
    m = re.search(r"id=([a-zA-Z0-9_-]+)", x)
    if m:
        return m.group(1)
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


# ===================== Drive =====================
def drive_list_files(
    drive,
    q: str,
    supports_all_drives: bool,
    corpora: str = "allDrives",
    drive_id: Optional[str] = None,
    page_size: int = 1000,
    fields: str = "nextPageToken, files(id,name,mimeType,driveId,parents,modifiedTime,createdTime,size)",
):
    page_size = max(1, min(int(page_size), 1000))

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
        supportsAllDrives=supports_all_drives,
    ).execute()


def pick_latest_5_months_from_folder(drive, folder_id: str, supports_all_drives: bool) -> List[dict]:
    meta = get_folder_meta(drive, folder_id, supports_all_drives)
    drive_id = meta.get("driveId")
    corpora = "drive" if (supports_all_drives and drive_id) else "allDrives"

    q = (
        f"'{folder_id}' in parents and trashed=false "
        f"and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    )

    items = drive_list_files(
        drive,
        q,
        supports_all_drives,
        corpora=corpora,
        drive_id=drive_id,
        page_size=min(DRIVE_SCAN_MAX_FILES, 1000),
    )

    matched = []
    for it in items:
        name = it.get("name", "")
        m = APT_FILE_RE.match(name)
        if m:
            matched.append((m.group(1), it))

    log(f"[drive] listed_xlsx={len(items)} matched_apt_xlsx={len(matched)} folder={folder_id}")

    if not matched:
        raise RuntimeError("Drive 폴더에서 '아파트 YYYYMM.xlsx' 파일을 찾지 못했습니다.")

    def ts(it):
        s = it.get("modifiedTime") or it.get("createdTime") or ""
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0

    best_by_ym: Dict[str, dict] = {}
    for ym, it in matched:
        cur = best_by_ym.get(ym)
        if not cur or ts(it) > ts(cur):
            best_by_ym[ym] = it

    yms = sorted(best_by_ym.keys(), reverse=True)[:5]
    log(f"[drive] months_to_process={yms}")
    return [best_by_ym[ym] for ym in yms]


def download_file_from_drive(drive, file_id: str, out_path: Path, supports_all_drives: bool):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=supports_all_drives)
    with out_path.open("wb") as f:
        downloader = MediaIoBaseDownload(f, request, chunksize=1024 * 1024)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return out_path


def download_latest_5_months_from_drive(creds) -> List[Path]:
    supports_all_drives = _bool_env("DRIVE_SUPPORTS_ALL_DRIVES", True)

    folder_env = os.environ.get("DRIVE_FOLDER_ID", "").strip()
    if not folder_env:
        raise RuntimeError("DRIVE_FOLDER_ID 환경변수가 필요합니다.")

    folder_id = _extract_id(folder_env)
    if not folder_id:
        raise RuntimeError("DRIVE_FOLDER_ID에서 폴더 ID를 추출하지 못했습니다.")

    drive = build_drive(creds)
    picked = pick_latest_5_months_from_folder(drive, folder_id, supports_all_drives)

    paths: List[Path] = []
    for it in picked:
        name = it.get("name", "")
        fid = it.get("id", "")
        if not fid:
            continue
        out = DOWNLOAD_DIR / name
        log(f"[drive] downloading: {name}")
        download_file_from_drive(drive, fid, out, supports_all_drives)
        paths.append(out)

    log(f"[drive] downloaded files={len(paths)} -> {DOWNLOAD_DIR}")
    return paths


# ===================== 시트 기본 =====================
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


def fuzzy_ws(sh: gspread.Spreadsheet, wanted: str) -> Optional[gspread.Worksheet]:
    tgt = re.sub(r"\s+", "", wanted.strip())
    for ws in sh.worksheets():
        if re.sub(r"\s+", "", ws.title.strip()) == tgt:
            log(f"[ws] matched: '{ws.title}' (wanted='{wanted}')")
            return ws
    return None


def ensure_ws_size(ws: gspread.Worksheet, min_rows: int, min_cols: int = 40):
    new_rows = max(ws.row_count, min_rows)
    new_cols = max(ws.col_count, min_cols)
    if new_rows != ws.row_count or new_cols != ws.col_count:
        _retry(ws.resize, rows=new_rows, cols=new_cols)
        _invalidate_cache(ws)
        log(f"[ws] resized: {ws.title} rows={new_rows} cols={new_cols}")


# ===================== 날짜/탭명 =====================
def parse_any_date(x) -> Optional[date]:
    if x is None:
        return None

    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x

    if isinstance(x, (int, float)):
        try:
            base = date(1899, 12, 30)
            days = int(float(x))
            if 1 <= days <= 60000:
                return base + timedelta(days=days)
        except Exception:
            pass

    s = str(x).strip()
    if not s:
        return None

    if re.fullmatch(r"\d+(?:\.\d+)?", s):
        try:
            base = date(1899, 12, 30)
            days = int(float(s))
            if 1 <= days <= 60000:
                return base + timedelta(days=days)
        except Exception:
            pass

    patterns = [
        r"(\d{4})-(\d{1,2})-(\d{1,2})",
        r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})",
        r"(\d{4})/(\d{1,2})/(\d{1,2})",
        r"(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일",
        r"(\d{1,2})월\s*(\d{1,2})일",
    ]

    for pat in patterns:
        m = re.search(pat, s)
        if m:
            try:
                if len(m.groups()) == 3:
                    return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                else:
                    # 연도가 없는 "4월 16일" 형식은 비교용으로는 오늘 연도 사용
                    today = datetime.now().date()
                    return date(today.year, int(m.group(1)), int(m.group(2)))
            except Exception:
                return None

    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        return None


def yymm_from_filename(fname: str) -> Tuple[int, int]:
    m = re.search(r"(20\d{2})(\d{2})", fname)
    if not m:
        raise ValueError(f"파일명에서 YYYYMM 추출 실패: {fname}")
    return int(m.group(1)), int(m.group(2))


def month_sheet_titles(year: int, month: int) -> Dict[str, List[str]]:
    yy = year % 100
    return {
        "전국": [
            f"전국 {yy}년 {month}월",
            f"전국 {month}월",
            f"전국 {year}년 {month}월",
        ],
        "서울": [
            f"서울 {yy}년 {month}월",
            f"서울 {month}월",
            f"서울 {year}년 {month}월",
        ],
    }


def preferred_sheet_title(level: str, year: int, month: int) -> str:
    yy = year % 100
    return f"{level} {yy}년 {month}월"


def date_label_for_sheet(d: date, style: str = "korean") -> str:
    if style == "iso":
        return d.isoformat()
    return f"{d.month}월 {d.day}일"


def detect_date_style(ws: gspread.Worksheet) -> str:
    vals = _get_all_values_cached(ws)
    if len(vals) >= 2 and len(vals[1]) >= 1:
        sample = vals[1][0]
        if parse_any_date(sample):
            s = str(sample).strip()
            if re.match(r"^\d{4}-\d{1,2}-\d{1,2}$", s):
                return "iso"
            if "월" in s and "일" in s:
                return "korean"
    return "korean"


# ===================== 월별 탭 찾기/생성 =====================
def find_month_ws(sh: gspread.Spreadsheet, level: str, year: int, month: int) -> Optional[gspread.Worksheet]:
    for cand in month_sheet_titles(year, month)[level]:
        ws = fuzzy_ws(sh, cand)
        if ws is not None:
            return ws
    return None


def ensure_month_ws(sh: gspread.Spreadsheet, level: str, year: int, month: int) -> gspread.Worksheet:
    expected_header = ["날짜"] + (NATION_REGIONS if level == "전국" else SEOUL_REGIONS)
    min_cols = max(MONTH_WS_MIN_COLS, len(expected_header) + 5)

    ws = find_month_ws(sh, level, year, month)
    if ws is None:
        title = preferred_sheet_title(level, year, month)
        ws = _retry(sh.add_worksheet, title=title, rows=MONTH_WS_MIN_ROWS, cols=min_cols)
        ws_update(ws, [expected_header], f"A1:{a1_col(len(expected_header))}1")
        log(f"[ws] created: {title}")
        return ws

    ensure_ws_size(ws, min_rows=MONTH_WS_MIN_ROWS, min_cols=min_cols)

    vals = _get_all_values_cached(ws)
    if not vals:
        ws_update(ws, [expected_header], f"A1:{a1_col(len(expected_header))}1")
        log(f"[ws] header initialized: {ws.title}")
        return ws

    header = [str(x).strip() for x in vals[0]]
    # A1이 비어 있고 B1부터 헤더가 있던 시트도 지원
    if header and header[0] == "":
        header[0] = "날짜"

    if header[:len(expected_header)] != expected_header:
        ws_update(ws, [expected_header], f"A1:{a1_col(len(expected_header))}1")
        log(f"[ws] header repaired: {ws.title}")
    else:
        log(f"[ws] exists: {ws.title}")

    return ws


# ===================== 날짜행 탐색 =====================
def find_or_append_date_row(ws: gspread.Worksheet, target_label: str) -> int:
    """
    A열 기준:
    1) 같은 날짜가 있으면 그 행
    2) 없으면 첫 빈 행
    3) 빈 행도 없으면 마지막 사용 행 다음 줄
    """
    target = parse_any_date(target_label)
    if not target:
        return 2

    # A열 전체 값을 직접 읽는다. 중간 빈 칸을 놓치지 않기 위해 A2:A2000 범위 사용.
    # 현재 구조상 충분히 넉넉하게 본다.
    ensure_ws_size(ws, min_rows=MONTH_WS_MIN_ROWS, min_cols=MONTH_WS_MIN_COLS)
    col = _retry(ws.get, f"A2:A{ws.row_count}") or []

    first_empty = None
    last_used = 1  # 헤더 행

    for idx, row in enumerate(col, start=2):
        v = row[0] if row else ""
        s = str(v).strip() if v is not None else ""

        if s == "":
            if first_empty is None:
                first_empty = idx
            continue

        last_used = idx
        d = parse_any_date(s)
        if d and d == target:
            return idx

    if first_empty is not None:
        return first_empty

    return last_used + 1


# ===================== 기록 =====================
def write_month_sheet(ws: gspread.Worksheet, target_day: date, header: List[str], values_by_colname: Dict[str, int]):
    style = detect_date_style(ws)
    date_label = date_label_for_sheet(target_day, style=style)
    row_idx = find_or_append_date_row(ws, date_label)

    ensure_ws_size(
        ws,
        min_rows=max(MONTH_WS_MIN_ROWS, row_idx + MONTH_WS_MIN_EXTRA),
        min_cols=max(MONTH_WS_MIN_COLS, len(header) + 5),
    )

    hmap = {str(h).strip(): idx + 1 for idx, h in enumerate(header) if str(h).strip()}
    sheet_prefix = f"'{ws.title}'!"
    payload = [{"range": f"{sheet_prefix}A{row_idx}", "values": [[date_label]]}]

    for col_name, val in values_by_colname.items():
        if col_name in hmap:
            c = hmap[col_name]
            payload.append({"range": f"{sheet_prefix}{a1_col(c)}{row_idx}", "values": [[int(val)]]})

    values_batch_update(ws, payload)
    log(f"[ws] {ws.title} -> {date_label} row={row_idx} wrote_cells={len(payload)}")


# ===================== 월 파일 읽기/집계 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    forced = os.environ.get("EXCEL_SHEET_NAME", "").strip()

    try:
        xls = pd.ExcelFile(path)
        sheet_names = list(xls.sheet_names or [])
    except Exception as e:
        raise RuntimeError(f"엑셀 파일을 열 수 없습니다: {path} ({e})")

    if not sheet_names:
        raise RuntimeError(f"엑셀 시트가 없습니다: {path.name}")

    if forced:
        picked = forced
    else:
        if "data" in sheet_names:
            picked = "data"
        elif "Sheet1" in sheet_names:
            picked = "Sheet1"
        else:
            picked = sheet_names[0]

    log(f"[excel] {path.name} -> picked '{picked}' candidates={sheet_names}")

    df = pd.read_excel(path, sheet_name=picked, dtype=str).fillna("")
    for c in ["계약년", "계약월", "계약일", "거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _strip_col(df: pd.DataFrame, col: str):
    if col in df.columns:
        df[col] = df[col].astype(str).map(lambda x: str(x).replace("\u3000", " ").strip())
    return df


def agg_counts(df: pd.DataFrame):
    counts = {}

    if df is None or df.empty:
        return counts

    df = df.copy()
    _strip_col(df, "광역")
    _strip_col(df, "구")
    _strip_col(df, "법정동")

    counts["전국"] = int(len(df))

    if "광역" in df.columns:
        for prov, sub in df.groupby("광역"):
            prov = str(prov).strip()
            counts[prov] = counts.get(prov, 0) + int(len(sub))

    seoul = df[df.get("광역", "") == "서울특별시"].copy()
    counts["서울"] = int(len(seoul))

    if "구" in seoul.columns:
        for gu, sub in seoul.groupby("구"):
            gu = str(gu).strip()
            counts[gu] = counts.get(gu, 0) + int(len(sub))

    return counts


# ===================== 거래요약(옵션) =====================
def update_summary_if_exists(sh: gspread.Spreadsheet, summary_rows: List[Tuple[str, Dict[str, int]]]):
    ws = fuzzy_ws(sh, SUMMARY_SHEET_NAME)
    if ws is None:
        log("[summary] skip: 거래요약 시트 없음")
        return

    months = [x[0] for x in summary_rows]  # 최신월 -> 과거월
    header = ["구분"] + months
    ensure_ws_size(ws, min_rows=50, min_cols=max(20, len(header) + 2))
    ws_update(ws, [header], f"A1:{a1_col(len(header))}1")

    lookup = {ym: counts for ym, counts in summary_rows}
    row_map = {
        "전국 거래건수": [],
        "서울 거래건수": [],
    }

    for ym in months:
        c = lookup[ym]
        row_map["전국 거래건수"].append(int(c.get("전국", 0)))
        row_map["서울 거래건수"].append(int(c.get("서울", 0)))

    out_rows = [[k] + v for k, v in row_map.items()]
    ws_update(ws, out_rows, f"A2:{a1_col(len(header))}{len(out_rows)+1}")
    log(f"[summary] wrote rows={len(out_rows)} months={len(months)}")


# ===================== 메인 =====================
def main():
    log("[MAIN] start (Drive -> Local -> Sheets)")

    sheet_id = os.environ.get("SHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("SHEET_ID 환경변수가 필요합니다.")

    creds = load_creds()
    xlsx_paths = download_latest_5_months_from_drive(creds)
    if not xlsx_paths:
        log("[drive] no files downloaded. stop.")
        return

    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, sheet_id)
    today = datetime.now().date()

    def ym_key_from_file(p: Path):
        y, m = yymm_from_filename(p.name)
        return y, m

    xlsx_paths = sorted(xlsx_paths, key=ym_key_from_file, reverse=True)

    summary_rows: List[Tuple[str, Dict[str, int]]] = []

    for p in xlsx_paths:
        year, month = yymm_from_filename(p.name)
        yy = year % 100
        ym_label = f"{yy:02d}/{month:02d}"

        log(f"[file] {p.name}")
        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        counts = agg_counts(df)
        summary_rows.append((ym_label, counts))

        ws_nat = ensure_month_ws(sh, "전국", year, month)
        header_nat = ["날짜"] + NATION_REGIONS
        values_nat = {k: int(counts.get(k, 0)) for k in NATION_REGIONS if k != "총합계"}
        values_nat["총합계"] = int(counts.get("전국", 0))
        write_month_sheet(ws_nat, today, header_nat, values_nat)

        ws_seoul = ensure_month_ws(sh, "서울", year, month)
        header_seoul = ["날짜"] + SEOUL_REGIONS
        values_seoul = {k: int(counts.get(k, 0)) for k in SEOUL_REGIONS if k != "총합계"}
        values_seoul["총합계"] = int(counts.get("서울", 0))
        write_month_sheet(ws_seoul, today, header_seoul, values_seoul)

    update_summary_if_exists(sh, summary_rows)
    log("[MAIN] done")


if __name__ == "__main__":
    main()
