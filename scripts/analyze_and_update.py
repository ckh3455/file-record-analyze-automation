# -*- coding: utf-8 -*-
"""
Artifacts로 받은 MOLIT 전처리 엑셀을 읽어
- '전국 YY년 MM월' 탭: 날짜별 광역단위 건수 집계, 정책에 따라 upsert
- '서울 YY년 MM월' 탭: 날짜별 구단위 건수 집계, 정책에 따라 upsert
를 수행한다.

중요:
- 기존 시트를 '찾아서' 갱신만 한다(새 시트 생성 금지).
- analyze_report/ 에 상세 로그와 where_written.txt 남김.
"""

from __future__ import annotations
import os, re, sys, json, shutil, zipfile
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Optional

import pandas as pd
import numpy as np

# -------------- 로그 --------------
LOG_DIR = Path("analyze_report")
RUN_TS = datetime.now()
RUN_LOG = LOG_DIR / f"run-{RUN_TS:%Y%m%d-%H%M%S}.log"
LATEST  = LOG_DIR / "latest.log"
WHERE   = LOG_DIR / "where_written.txt"

def _ensure_logdir():
    # 폴더가 파일로 존재하면 삭제하고 폴더로 재생성
    if LOG_DIR.exists() and not LOG_DIR.is_dir():
        try:
            LOG_DIR.unlink()
        except Exception:
            shutil.rmtree(LOG_DIR, ignore_errors=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

def log(line: str):
    _ensure_logdir()
    msg = f"[{RUN_TS:%H:%M:%S}] {line.rstrip()}"
    print(msg, flush=True)
    try:
        with RUN_LOG.open("a", encoding="utf-8") as f:
            f.write(msg + "\n")
        with LATEST.open("a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass

def log_block(title: str):
    log(f"[{title.upper()}]")

def note_where(s: str):
    _ensure_logdir()
    try:
        with WHERE.open("a", encoding="utf-8") as f:
            f.write(s.rstrip() + "\n")
    except Exception:
        pass

# -------------- 유틸 --------------
MONTH_TITLE_NAT = re.compile(r"^전국\s+(\d{2})년\s+(\d{2})월$")
MONTH_TITLE_SEO = re.compile(r"^서울\s+(\d{2})년\s+(\d{2})월$")

# 기존 정책: 최초 기록일로부터 3개월까지는 매일 기록,
# 이후에는 '마지막행과 값이 동일하면' 스킵, 다르면 append.
MAX_DAILY_WINDOW_DAYS = 92

SIDO_LIST = [
    "강원특별자치도","경기도","경상남도","경상북도","광주광역시","대구광역시","대전광역시",
    "부산광역시","서울특별시","세종특별자치시","울산광역시","인천광역시",
    "전라남도","전북특별자치도","제주특별자치도","충청남도","충청북도"
]
SEOUL_GU = [
    "강남구","강동구","강북구","강서구","관악구","광진구","구로구","금천구","노원구","도봉구",
    "동대문구","동작구","마포구","서대문구","서초구","성동구","성북구","송파구","양천구",
    "영등포구","용산구","은평구","종로구","중구","중랑구"
]

def yymm_from_fname(name: str) -> Tuple[int,int]:
    # "전국 2411_250926.xlsx" -> (24, 11)
    m = re.search(r"전국\s+(\d{2})(\d{2})_", name)
    if not m:
        # "서울시 250926.xlsx" (서울은 파일명에 월 정보 없음 → 스킵)
        return (0,0)
    return (int(m.group(1)), int(m.group(2)))

def guess_write_date_from_fname(name: str) -> date:
    # "..._YYMMDD.xlsx" -> 날짜
    m = re.search(r"_(\d{2})(\d{2})(\d{2})", name)
    if m:
        y, mth, d = map(int, m.groups())
        # 20xx 로 가정
        return date(2000 + y, mth, d)
    return date.today()

def parse_nat_title(y: int, m: int) -> str:
    return f"전국 {y:02d}년 {m:02d}월"

def parse_seoul_title(y: int, m: int) -> str:
    return f"서울 {y:02d}년 {m:02d}월"

def month_number(y2: int, m2: int) -> int:
    # 24년 10월 -> 24*12+10
    return y2*12 + m2

# -------------- 엑셀 로드 --------------
def read_xlsx(path: Path) -> pd.DataFrame:
    """
    전처리된 'data' 시트가 있으면 그걸 사용.
    없으면 첫 시트를 읽은 뒤, 상단 헤더 감지 실패 시 1행을 헤더로 간주.
    """
    log(f"[read] loading xlsx: {path.as_posix()}")
    try:
        df = pd.read_excel(path, sheet_name="data", dtype=str, engine="openpyxl")
        log(f"[read] sheet='data' rows={len(df)} cols={len(df.columns)}")
    except Exception:
        df = pd.read_excel(path, dtype=str, engine="openpyxl")
        log(f"[read] sheet='(first)' rows={len(df)} cols={len(df.columns)}")

    df = df.fillna("")
    # 헤더 이름 정규화
    df.columns = [str(c).strip() for c in df.columns]

    # 예전 파일 대비: 중복 컬럼 제거(첫 번째만 유지)
    dup = [c for c in df.columns if df.columns.tolist().count(c) > 1]
    if dup:
        log(f"[read] duplicated columns dropped (keep=first): {dup}")
        seen=set(); keep=[]
        for c in df.columns:
            if c in seen: continue
            seen.add(c); keep.append(c)
        df = df[keep]

    # 전처리 스크립트 형식 기준의 컬럼이 있으면 곧바로 return
    must = {"광역","구","계약년","계약월","계약일"}
    if must.issubset(set(df.columns)):
        return df

    # 혹시 '시','시군구' 계열만 있고 '광역'이 없으면 매핑 시도
    if "시군구" in df.columns and "광역" not in df.columns:
        # "서울특별시 강남구 역삼동" 형태 분리
        parts = df["시군구"].astype(str).str.split(expand=True, n=2)
        df["광역"] = parts[0]
        df["구"] = parts[1] if parts.shape[1] > 1 else ""
        if "계약년월" in df.columns and "계약년" not in df.columns:
            s = df["계약년월"].str.replace(r"\D","",regex=True)
            df["계약년"] = s.str[:4]
            df["계약월"] = s.str[4:6]
        return df

    # 그래도 없으면, 1행을 헤더로 보고 다시 구성
    if len(df)>0:
        header = [str(x).strip() for x in df.iloc[0].tolist()]
        df2 = df.iloc[1:].copy()
        df2.columns = header
        df2 = df2.fillna("")
        log("[read] header fallback -> used first row as header")
        return df2

    return df

# -------------- 집계 --------------
def aggregate_national(df: pd.DataFrame) -> pd.Series:
    if "광역" not in df.columns:
        log("[agg] missing column '광역' -> empty series")
        return pd.Series(dtype=int)
    s = df.groupby("광역")["광역"].count()
    # 시도 목록 기준으로 재정렬/없으면 0
    out = pd.Series({k:int(s.get(k,0)) for k in SIDO_LIST})
    return out

def aggregate_seoul(df: pd.DataFrame) -> pd.Series:
    if "광역" not in df.columns or "구" not in df.columns:
        log("[agg] missing column '광역/구' -> empty series")
        return pd.Series(dtype=int)
    sdf = df[df["광역"]=="서울특별시"]
    s = sdf.groupby("구")["구"].count()
    out = pd.Series({k:int(s.get(k,0)) for k in SEOUL_GU})
    return out

# -------------- Google Sheets --------------
import gspread
from google.oauth2.service_account import Credentials

def open_sheet(sa_path: Path, sheet_id: str):
    log("[gspread] auth with sa.json")
    info = json.loads(sa_path.read_text(encoding="utf-8"))
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    log("[gspread] spreadsheet opened")
    return sh

def find_ws(sh, title: str):
    try:
        return sh.worksheet(title)
    except Exception:
        return None

def get_header(ws) -> List[str]:
    vals = ws.row_values(1)
    return [v.strip() for v in vals]

def find_or_create_row_index_for_date(ws, when: date) -> Tuple[int, bool]:
    """
    기존 날짜가 있으면 그 행 index 반환, 없으면 append 위치 반환.
    반환: (row_index, exists_bool)
    """
    colA = ws.col_values(1)
    for i, v in enumerate(colA, start=1):
        if i == 1:  # header
            continue
        if str(v).strip() == when.isoformat():
            return i, True
    # append 위치
    return len(colA)+1, False

def policy_should_write(ws, when: date, values: List) -> bool:
    """
    정책 판단:
    - 첫 기록일 ~ +92일: 무조건 기록(업데이트)
    - 이후: 마지막행 값과 동일하면 스킵, 다르면 기록
    """
    # 첫 기록일
    colA = ws.col_values(1)
    dates = []
    for i,v in enumerate(colA, start=1):
        if i == 1: continue
        try:
            dates.append(date.fromisoformat(str(v).strip()))
        except Exception:
            pass
    if dates:
        first = min(dates)
        if (when - first).days <= MAX_DAILY_WINDOW_DAYS:
            return True
        # 마지막행 비교
        last_row_idx = len(colA)
        last_vals = ws.row_values(last_row_idx)
        # 날짜열 포함 헤더 길이에 맞춰 비교
        cur = [when.isoformat()] + values[1:]
        return cur != last_vals[:len(cur)]
    return True

def write_row(ws, when: date, header: List[str], series: pd.Series, label_total: str="총합계") -> str:
    # row 구성
    vals = [when.isoformat()]
    total = 0
    for key in header[1:]:
        if key == label_total:
            vals.append(total)
        else:
            v = int(series.get(key, 0))
            total += v
            vals.append(v)

    # upsert 위치 찾기
    row_idx, exists = find_or_create_row_index_for_date(ws, when)
    # 정책 체크
    if not policy_should_write(ws, when, vals):
        return "skip(=last)"

    rng = f"A{row_idx}:{gspread.utils.rowcol_to_a1(1, len(header))[:-1]}{row_idx}"
    ws.update([vals], range_name=rng)
    return "append" if not exists else "update"

# -------------- 메인 --------------
def collect_artifacts(art_dir: Path) -> List[Path]:
    log_block("collect")
    log(f"artifacts_dir={art_dir}")
    paths: List[Path] = []

    # zip → 풀기
    zips = list(art_dir.rglob("*.zip"))
    log(f"zip files found: {len(zips)}")
    for z in zips:
        dest = Path("extracted") / z.parent.name
        dest.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(z, "r") as zp:
            zp.extractall(dest)

    # xlsx 모으기
    for root in ["extracted", str(art_dir)]:
        for p in Path(root).rglob("*.xlsx"):
            paths.append(p.resolve())
    log(f"total xlsx under work_dir: {len(paths)}")
    return paths

def main():
    _ensure_logdir()
    log_block("main")

    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts-dir", required=True)
    ap.add_argument("--sa", required=True)
    ap.add_argument("--sheet-id", required=True)
    args = ap.parse_args()

    art_dir = Path(args.artifacts_dir)
    sa_path = Path(args.sa)
    sheet_id = args.sheet_id

    files = collect_artifacts(art_dir)

    # 전국 파일만(파일명에 '전국 YYMM_')
    nat_files = [p for p in files if "전국 " in p.name]
    log(f"national files count={len(nat_files)}")

    sh = open_sheet(sa_path, sheet_id)

    for f in sorted(nat_files, key=lambda p: p.name):
        y2, m2 = yymm_from_fname(f.name)
        if y2 == 0:
            continue
        write_day = guess_write_date_from_fname(f.name)
        nat_title = parse_nat_title(y2, m2)
        se_title  = parse_seoul_title(y2, m2)

        log(f"[file] {f.name} -> nat='{nat_title}' seoul='{se_title}' date={write_day}")

        df = read_xlsx(f)

        # 집계
        nat_series = aggregate_national(df)
        se_series  = aggregate_seoul(df)

        # --- 전국 ---
        ws_nat = find_ws(sh, nat_title)
        if ws_nat is None:
            log(f"[전국] {f.name} -> sheet not found: '{nat_title}' (skip)")
        else:
            header = get_header(ws_nat)
            # 헤더 정리: '날짜' + SIDO_LIST + (총합계 있으면 유지)
            wants = ["날짜"] + SIDO_LIST
            if any("총합" in h for h in header):
                wants.append("총합계")
            # 헤더 길이가 다르면 수정하지 않고, 교집합 기준으로만 씀
            header_use = [h for h in header if h in wants]
            if header_use and header_use[0] != "날짜":
                header_use = ["날짜"] + [c for c in header_use if c!="날짜"]
            op = write_row(ws_nat, write_day, header_use, nat_series, label_total="총합계")
            note_where(f"[전국] {nat_title} @ {write_day}: {op}, sum={int(nat_series.sum()) if len(nat_series)>0 else 0}")

        # --- 서울 ---
        ws_se = find_ws(sh, se_title)
        if ws_se is None:
            log(f"[서울] {f.name} -> sheet not found: '{se_title}' (skip)")
        else:
            header = get_header(ws_se)
            wants = ["날짜"] + SEOUL_GU
            if any("총합" in h for h in header):
                wants.append("총합계")
            header_use = [h for h in header if h in wants]
            if header_use and header_use[0] != "날짜":
                header_use = ["날짜"] + [c for c in header_use if c!="날짜"]
            op = write_row(ws_se, write_day, header_use, se_series, label_total="총합계")
            note_where(f"[서울] {se_title} @ {write_day}: {op}, sum={int(se_series.sum()) if len(se_series)>0 else 0}")

    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {e!r}")
        raise
