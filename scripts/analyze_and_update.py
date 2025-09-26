# -*- coding: utf-8 -*-
"""
Artifacts로 받은 MOLIT 전처리 엑셀을 읽어
- '전국 YY년 MM월' 탭: 날짜별 광역단위 건수 집계, 정책에 따라 upsert
- '서울 YY년 MM월' 탭: 날짜별 구단위 건수 집계, 정책에 따라 upsert
를 수행한다.

중요:
- 기존 시트를 '찾아서' 갱신만 한다(새 시트 생성 금지).
- 탭 이름은 퍼지 매칭(공백/불필요 문자 무시, 괄호/접미사 허용)으로 찾는다.
- analyze_report/ 에 상세 로그와 where_written.txt 남김.
"""

from __future__ import annotations
import os, re, sys, json, shutil, zipfile
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional

import pandas as pd
import numpy as np

# ---------------- 로그 ----------------
LOG_DIR = Path("analyze_report")
RUN_TS = datetime.now()
RUN_LOG = LOG_DIR / f"run-{RUN_TS:%Y%m%d-%H%M%S}.log"
LATEST  = LOG_DIR / "latest.log"
WHERE   = LOG_DIR / "where_written.txt"

def _ensure_logdir():
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

# ---------------- 상수/유틸 ----------------
MAX_DAILY_WINDOW_DAYS = 92  # 최초 기록일 + 3개월

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
        return (0,0)
    return (int(m.group(1)), int(m.group(2)))

def guess_write_date_from_fname(name: str) -> date:
    # "_YYMMDD.xlsx"
    m = re.search(r"_(\d{2})(\d{2})(\d{2})", name)
    if m:
        y, mth, d = map(int, m.groups())
        return date(2000 + y, mth, d)
    return date.today()

def expect_title(kind: str, y2: int, m2: int) -> str:
    # kind: '전국' or '서울'
    return f"{kind} {y2:02d}년 {m2:02d}월"

# ---------------- 엑셀 로드 ----------------
def read_xlsx(path: Path) -> pd.DataFrame:
    log(f"[read] loading xlsx: {path.as_posix()}")
    # 우선 전처리 저장된 'data' 시트 시도
    for sn in ("data", 0):
        try:
            df = pd.read_excel(path, sheet_name=sn, dtype=str, engine="openpyxl")
            log(f"[read] sheet='{sn}' rows={len(df)} cols={len(df.columns)}")
            break
        except Exception:
            if sn == 0:
                raise
    df = df.fillna("")
    df.columns = [str(c).strip() for c in df.columns]

    dup = [c for c in df.columns if df.columns.tolist().count(c) > 1]
    if dup:
        log(f"[read] duplicated columns dropped (keep=first): {dup}")
        seen=set(); keep=[]
        for c in df.columns:
            if c in seen: continue
            seen.add(c); keep.append(c)
        df = df[keep]

    # 이미 전처리된 스키마라면 바로 사용
    must = {"광역","구","계약년","계약월","계약일"}
    if must.issubset(set(df.columns)):
        return df

    # '시군구'만 있는 경우 분리
    if "시군구" in df.columns and "광역" not in df.columns:
        parts = df["시군구"].astype(str).str.split(expand=True, n=2)
        df["광역"] = parts[0]
        df["구"] = parts[1] if parts.shape[1] > 1 else ""
        if "계약년월" in df.columns and "계약년" not in df.columns:
            s = df["계약년월"].str.replace(r"\D","",regex=True)
            df["계약년"] = s.str[:4]
            df["계약월"] = s.str[4:6]
        if "계약일" not in df.columns and "일" in df.columns:
            df["계약일"] = df["일"].astype(str).str.replace(r"\D","",regex=True)
        return df

    # 첫 행을 헤더로 재구성
    if len(df)>0:
        header = [str(x).strip() for x in df.iloc[0].tolist()]
        df2 = df.iloc[1:].copy()
        df2.columns = header
        df2 = df2.fillna("")
        log("[read] header fallback -> used first row as header")
        return df2
    return df

# ---------------- 집계 ----------------
def aggregate_national(df: pd.DataFrame) -> pd.Series:
    if "광역" not in df.columns:
        log("[agg] missing column '광역' -> empty series")
        return pd.Series(dtype=int)
    s = df.groupby("광역")["광역"].count()
    return pd.Series({k:int(s.get(k,0)) for k in SIDO_LIST})

def aggregate_seoul(df: pd.DataFrame) -> pd.Series:
    if "광역" not in df.columns or "구" not in df.columns:
        log("[agg] missing column '광역/구' -> empty series")
        return pd.Series(dtype=int)
    sdf = df[df["광역"]=="서울특별시"]
    s = sdf.groupby("구")["구"].count()
    return pd.Series({k:int(s.get(k,0)) for k in SEOUL_GU})

# ---------------- Sheets ----------------
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

def _norm(s: str) -> str:
    # 공백/괄호/하이픈/밑줄 같은 군더더기 제거, 전각 공백 제거
    s = re.sub(r"[\s\u3000\-\_()（）]+", "", s)
    return s

def _title_variants(kind: str, y2: int, m2: int) -> List[str]:
    # ‘전국 24년 10월’, ‘전국24년10월’, ‘전국(24년10월)’, ‘전국 24년 10월_자동’ 등 폭넓게 매칭
    base = expect_title(kind, y2, m2)
    compact = _norm(base)
    pat = [
        base,
        compact,
        f"{kind}{y2:02d}년{m2:02d}월",
        f"{kind} {y2:02d}년{m2:02d}월",
        f"{kind}{y2:02d}년 {m2:02d}월",
        f"{kind}({y2:02d}년 {m2:02d}월)",
        f"{kind}({y2:02d}년{m2:02d}월)"
    ]
    return list(dict.fromkeys(pat))  # 중복 제거, 순서 유지

def find_ws_fuzzy(sh, kind: str, y2: int, m2: int):
    """
    탭 이름 퍼지 매칭:
    - 공백/괄호/하이픈/밑줄 무시한 정규화로 비교
    - 접미사 붙은 변형(예: '..._자동', '...copy')도 허용
    - 최종적으로 가장 먼저 매칭된 탭을 반환
    """
    wants = _title_variants(kind, y2, m2)
    wants_norm = [_norm(w) for w in wants]
    titles = [ws.title for ws in sh.worksheets()]
    titles_norm = [_norm(t) for t in titles]

    # 1) 완전 일치(정규화 후)
    for t, tn in zip(titles, titles_norm):
        if tn in wants_norm:
            log(f"[ws] fuzzy matched (exact norm): '{t}'")
            return sh.worksheet(t)

    # 2) 접두 일치(정규화 후) — 원래 제목 + 접미사
    for t, tn in zip(titles, titles_norm):
        if any(tn.startswith(wn) for wn in wants_norm):
            log(f"[ws] fuzzy matched (prefix norm): '{t}'")
            return sh.worksheet(t)

    # 3) 숫자 토큰 검색 — '전국'/'서울' + YY + MM 패턴 포함
    yy = f"{y2:02d}"
    mm = f"{m2:02d}"
    for t in titles:
        if ("전국" if kind=="전국" else "서울") in t and yy in t and mm in t and "월" in t:
            log(f"[ws] fuzzy matched (tokens): '{t}'")
            return sh.worksheet(t)

    log(f"[ws] no fuzzy match for kind='{kind}' y2={y2} m2={m2}")
    return None

def get_header(ws) -> List[str]:
    vals = ws.row_values(1)
    return [v.strip() for v in vals]

def find_or_create_row_index_for_date(ws, when: date) -> Tuple[int, bool]:
    colA = ws.col_values(1)
    for i, v in enumerate(colA, start=1):
        if i == 1:  # header
            continue
        if str(v).strip() == when.isoformat():
            return i, True
    return len(colA)+1, False

def policy_should_write(ws, when: date, values: List) -> bool:
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
        # 마지막행 값과 동일하면 스킵
        last_row_idx = len(colA)
        last_vals = ws.row_values(last_row_idx)
        cur = [when.isoformat()] + values[1:]
        return cur != last_vals[:len(cur)]
    return True

def write_row(ws, when: date, header: List[str], series: pd.Series, label_total: str="총합계") -> str:
    vals = [when.isoformat()]
    total = 0
    for key in header[1:]:
        if key == label_total:
            vals.append(total)
        else:
            v = int(series.get(key, 0))
            total += v
            vals.append(v)
    row_idx, exists = find_or_create_row_index_for_date(ws, when)
    if not policy_should_write(ws, when, vals):
        return "skip(=last)"

    # range 계산 (A{row} : {lastcol}{row})
    last_a1 = gspread.utils.rowcol_to_a1(1, len(header))
    last_col = re.sub(r"\d+", "", last_a1)  # "A1" -> "A"
    rng = f"A{row_idx}:{last_col}{row_idx}"
    ws.update([vals], range_name=rng)
    return "append" if not exists else "update"

# ---------------- 수집/메인 ----------------
def collect_artifacts(art_dir: Path) -> List[Path]:
    log_block("collect")
    log(f"artifacts_dir={art_dir}")
    paths: List[Path] = []

    zips = list(art_dir.rglob("*.zip"))
    log(f"zip files found: {len(zips)}")
    for z in zips:
        dest = Path("extracted") / z.parent.name
        dest.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(z, "r") as zp:
            zp.extractall(dest)

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
    nat_files = [p for p in files if "전국 " in p.name]
    log(f"national files count={len(nat_files)}")

    sh = open_sheet(sa_path, sheet_id)

    for f in sorted(nat_files, key=lambda p: p.name):
        y2, m2 = yymm_from_fname(f.name)
        if y2 == 0:
            continue
        write_day = guess_write_date_from_fname(f.name)

        # 엑셀 읽기 & 집계
        df = read_xlsx(f)
        nat_series = aggregate_national(df)
        se_series  = aggregate_seoul(df)

        # 탭 이름 퍼지 매칭
        ws_nat = find_ws_fuzzy(sh, "전국", y2, m2)
        ws_se  = find_ws_fuzzy(sh, "서울", y2, m2)

        if ws_nat is None:
            log(f"[전국] {f.name} -> sheet NOT FOUND for {expect_title('전국', y2, m2)} (skip)")
        else:
            header = get_header(ws_nat)
            wants = ["날짜"] + SIDO_LIST
            if any("총합" in h for h in header):
                wants.append("총합계")
            header_use = [h for h in header if h in wants]
            if header_use and header_use[0] != "날짜":
                header_use = ["날짜"] + [c for c in header_use if c!="날짜"]
            op = write_row(ws_nat, write_day, header_use, nat_series, label_total="총합계")
            note_where(f"[전국] {ws_nat.title} @ {write_day}: {op}, sum={int(nat_series.sum()) if len(nat_series)>0 else 0}")

        if ws_se is None:
            log(f"[서울] {f.name} -> sheet NOT FOUND for {expect_title('서울', y2, m2)} (skip)")
        else:
            header = get_header(ws_se)
            wants = ["날짜"] + SEOUL_GU
            if any("총합" in h for h in header):
                wants.append("총합계")
            header_use = [h for h in header if h in wants]
            if header_use and header_use[0] != "날짜":
                header_use = ["날짜"] + [c for c in header_use if c!="날짜"]
            op = write_row(ws_se, write_day, header_use, se_series, label_total="총합계")
            note_where(f"[서울] {ws_se.title} @ {write_day}: {op}, sum={int(se_series.sum()) if len(se_series)>0 else 0}")

    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {e!r}")
        raise
