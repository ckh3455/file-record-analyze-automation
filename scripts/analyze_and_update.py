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

# ===================== 설정 =====================
LOG_DIR = Path("analyze_report")
LOG_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOG = LOG_DIR / "latest.log"

SUMMARY_SHEET_NAME = "거래요약"
MAX_SCAN_ROWS = int(os.environ.get("MAX_SCAN_ROWS", "900"))

# 기존 로직 유지용(너가 기존에 쓰던 상수/리스트는 그대로 둬야 함)
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
def log(msg: str):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    try:
        with RUN_LOG.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

_LAST = 0.0
def _throttle(sec: float = 0.35):
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

# ===================== 인증/시트 열기 =====================
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

# ===================== Drive (핵심 수정) =====================
APT_FILE_RE = re.compile(os.environ.get("DRIVE_FILE_REGEX", r"^아파트\s*(\d{6})\.xlsx$"))

def _bool_env(name: str, default: bool = False) -> bool:
    v = str(os.environ.get(name, str(default))).strip().lower()
    return v in ("1", "true", "yes", "y", "on")

def _extract_id(x: str) -> str:
    """DRIVE_FOLDER_ID가 URL로 들어와도 ID만 뽑아냄."""
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

def build_drive(creds):
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def drive_get_file(drive, file_id: str, supports_all_drives: bool):
    return drive.files().get(
        fileId=file_id,
        fields="id,name,mimeType,driveId,parents,shortcutDetails",
        supportsAllDrives=supports_all_drives
    ).execute()

def drive_list_files(drive, q: str, supports_all_drives: bool, corpora: str = "allDrives",
                     drive_id: Optional[str] = None, page_size: int = 1000):
    kwargs = dict(
        q=q,
        fields="nextPageToken, files(id,name,mimeType,driveId,parents,modifiedTime,createdTime,size,shortcutDetails)",
        supportsAllDrives=supports_all_drives,
        includeItemsFromAllDrives=supports_all_drives,
        pageSize=page_size,
        corpora=corpora,
    )
    if corpora == "drive" and drive_id:
        kwargs["driveId"] = drive_id

    out = []
    token = None
    while True:
        if token:
            kwargs["pageToken"] = token
        resp = drive.files().list(**kwargs).execute()
        out.extend(resp.get("files", []))
        token = resp.get("nextPageToken")
        if not token:
            break
    return out

def drive_list_drives(drive, page_size: int = 100):
    out = []
    token = None
    while True:
        resp = drive.drives().list(
            pageSize=page_size,
            pageToken=token,
            fields="nextPageToken, drives(id,name)"
        ).execute()
        out.extend(resp.get("drives", []))
        token = resp.get("nextPageToken")
        if not token:
            break
    return out

def resolve_folder_id(drive, folder_id: str, supports_all_drives: bool) -> Tuple[str, Optional[str]]:
    """
    folder_id가:
    - 실제 폴더ID면 그대로
    - 바로가기(shortcut)이면 targetId로 변환
    - 접근 불가/틀린 ID면: 공유드라이브 전체에서 '아파트' 폴더를 자동 탐색해서 찾아줌
    return: (resolved_folder_id, driveId)
    """
    folder_id = _extract_id(folder_id)
    if folder_id:
        try:
            meta = drive_get_file(drive, folder_id, supports_all_drives)
            mt = meta.get("mimeType", "")
            if mt == "application/vnd.google-apps.shortcut":
                target = (meta.get("shortcutDetails") or {}).get("targetId")
                if target:
                    log(f"[drive] folder id is SHORTCUT -> targetId={target}")
                    meta2 = drive_get_file(drive, target, supports_all_drives)
                    return meta2["id"], meta2.get("driveId")
            return meta["id"], meta.get("driveId")
        except HttpError as e:
            # 404면 "못 보는 것"이라서 자동탐색으로 넘어감
            log(f"[drive] folder meta get failed (will auto-discover): {e}")

    # ===== 자동 탐색: 공유드라이브들에서 '아파트' 폴더 후보를 찾고, 그 안에 '아파트 6자리.xlsx'가 있는 폴더를 선택 =====
    log("[drive] auto-discovering '아파트' folder in shared drives...")
    drives = drive_list_drives(drive)
    log(f"[drive] visible shared drives={len(drives)}")

    best_folder = None
    best_score = -1
    best_drive_id = None

    # shared drive 하나씩 탐색
    for d in drives:
        did = d.get("id")
        if not did:
            continue
        # 드라이브 내부에서 '아파트' 폴더 찾기
        q_folder = "mimeType='application/vnd.google-apps.folder' and trashed=false and name='아파트'"
        folders = drive_list_files(drive, q_folder, supports_all_drives, corpora="drive", drive_id=did, page_size=200)
        for f in folders:
            fid = f.get("id")
            if not fid:
                continue
            # 그 폴더 안의 xlsx 목록을 보고 패턴 매칭 수로 점수화
            q_xlsx = f"'{fid}' in parents and trashed=false and mimeType!='application/vnd.google-apps.folder'"
            items = drive_list_files(drive, q_xlsx, supports_all_drives, corpora="drive", drive_id=did, page_size=1000)
            matched = 0
            for it in items:
                name = it.get("name", "")
                if APT_FILE_RE.match(name):
                    matched += 1
            if matched > best_score:
                best_score = matched
                best_folder = fid
                best_drive_id = did

    if not best_folder or best_score <= 0:
        raise RuntimeError(
            "Drive에서 '아파트' 폴더(아파트 6자리.xlsx 포함)를 자동으로 찾지 못했습니다.\n"
            "- 1) 서비스계정이 '부동산자료' 공유드라이브 멤버(또는 최소 해당 폴더 접근)인지\n"
            "- 2) 공유드라이브 내 실제 폴더명이 정확히 '아파트'인지\n"
            "- 3) 파일명이 '아파트 200601.xlsx' 형식인지 확인하세요."
        )

    log(f"[drive] auto-discovered apt folder id={best_folder} (matched_files={best_score}) driveId={best_drive_id}")
    return best_folder, best_drive_id

def pick_latest_12_months_from_drive(drive, folder_id: str, supports_all_drives: bool) -> List[dict]:
    """
    folder_id 폴더에서 '아파트 YYYYMM.xlsx'를 찾아 최근 12개(월)만 선택
    """
    resolved_folder_id, drive_id = resolve_folder_id(drive, folder_id, supports_all_drives)

    # 폴더 내 파일 리스트
    q = f"'{resolved_folder_id}' in parents and trashed=false"
    corpora = "drive" if (supports_all_drives and drive_id) else "allDrives"
    items = drive_list_files(drive, q, supports_all_drives, corpora=corpora, drive_id=drive_id, page_size=2000)

    matched = []
    for it in items:
        name = it.get("name", "")
        m = APT_FILE_RE.match(name)
        if not m:
            continue
        yyyymm = m.group(1)
        matched.append((yyyymm, it))

    log(f"[drive] listed files={len(items)} matched_apt={len(matched)} folder={resolved_folder_id}")

    if not matched:
        raise RuntimeError(
            "Drive 폴더에서 '아파트 YYYYMM.xlsx' 파일을 찾지 못했습니다.\n"
            "- DRIVE_FOLDER_ID가 '아파트 폴더 자체'인지 확인하거나,\n"
            "- 파일명이 정확히 '아파트 200601.xlsx' 형식인지 확인하세요."
        )

    # 같은 월 중복이면 modifiedTime 최신 우선
    def parse_dt(s: str) -> float:
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0

    best_by_ym: Dict[str, dict] = {}
    for ym, it in matched:
        cur = best_by_ym.get(ym)
        if not cur:
            best_by_ym[ym] = it
        else:
            if parse_dt(it.get("modifiedTime","")) > parse_dt(cur.get("modifiedTime","")):
                best_by_ym[ym] = it

    # 최근 12개월 선택
    yms = sorted(best_by_ym.keys(), reverse=True)[:12]
    yms = sorted(yms)
    picked = [best_by_ym[ym] for ym in yms]
    log(f"[drive] months_to_process={yms}")
    return picked

def download_file_from_drive(drive, file_id: str, out_path: Path, supports_all_drives: bool):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=supports_all_drives)
    with out_path.open("wb") as f:
        downloader = MediaIoBaseDownload(f, request, chunksize=1024 * 1024)
        done = False
        while not done:
            status, done = downloader.next_chunk()

def download_latest_12_months_from_drive(creds) -> List[Path]:
    supports_all_drives = _bool_env("DRIVE_SUPPORTS_ALL_DRIVES", True)
    folder_id = os.environ.get("DRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        raise RuntimeError("DRIVE_FOLDER_ID(아파트폴더 또는 상위 링크) 환경변수가 필요합니다.")

    drive = build_drive(creds)
    picked = pick_latest_12_months_from_drive(drive, folder_id, supports_all_drives)

    local_dir = Path("drive_cache")
    local_dir.mkdir(parents=True, exist_ok=True)

    out_paths: List[Path] = []
    for it in picked:
        fid = it["id"]
        name = it.get("name", fid + ".xlsx")
        out_path = local_dir / name
        download_file_from_drive(drive, fid, out_path, supports_all_drives)
        out_paths.append(out_path)
        log(f"[drive] downloaded: {name} -> {out_path}  size={out_path.stat().st_size:,}")

    return out_paths

# ===================== 이하: 너가 기존에 쓰던 분석/시트 업데이트 로직(핵심 구조 유지) =====================
# 아래는 “최소 동작”을 위해 필요한 함수들만 포함(너가 기존에 쓰던 그대로 두면 됨)

YM_RE = re.compile(r"(\d{4})년\s*(\d{1,2})월")

def ym_from_filename(fname: str):
    # '아파트 200601.xlsx' => 2020년 6월
    m = re.search(r"(\d{2})(\d{2})(\d{2})", fname.replace(" ", ""))
    # 위 정규식이 너무 느슨할 수 있어 '아파트 200601.xlsx' 형태로만 파싱
    m2 = re.search(r"아파트\s*(\d{6})", fname)
    if m2:
        yyyymm = m2.group(1)
        y = int(yyyymm[:4])
        mm = int(yyyymm[4:])
        if 1 <= mm <= 12:
            return f"전국 {y}년 {mm}월", f"서울 {y}년 {mm}월", f"{y%100:02d}/{mm:02d}"
    return None, None, None

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

def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data", dtype=str).fillna("")
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

# ===== 시트 쓰기(네 기존 로직과 동일하게 유지; 여기선 최소 구현만) =====
def main():
    log("[MAIN] start (Drive -> Local -> Sheets)")

    sheet_id = os.environ.get("SHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("SHEET_ID 환경변수가 필요합니다.")

    creds = load_creds()

    # 1) Drive에서 최근 12개월 xlsx 다운로드 (핵심)
    xlsx_paths = download_latest_12_months_from_drive(creds)

    # 2) Sheets 연결
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, sheet_id)
    today_iso = datetime.now().date().isoformat()

    # 3) 너의 기존 “월별 탭/요약 탭/압구정동 탭” 업데이트 로직은
    #    여기서 xlsx_paths를 입력으로 동일하게 돌리면 됨.
    #    (아래는 “동작 확인용” 최소 로그만)
    log(f"[input] drive xlsx_files={len(xlsx_paths)}")
    for p in xlsx_paths:
        log(f"[file] {p.name} size={p.stat().st_size:,}")
        df = read_month_df(p)
        counts, med, mean = agg_all_stats(df)
        # 여기서 너의 기존 write_month_sheet / update_apgujong_tab / 거래요약 업데이트를 그대로 호출하면 됨.

    log("[MAIN] done (Drive download OK)")

if __name__ == "__main__":
    main()
