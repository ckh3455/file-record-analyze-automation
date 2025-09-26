# -*- coding: utf-8 -*-
"""
Artifacts 엑셀을 읽어 기존 Google Sheet의 '기존 탭'에만 갱신한다.
- 전국: 날짜별 광역(시·도) 건수
- 서울: 날짜별 구 건수
정책:
- 최초 기록일+3개월 이내: 매일 기록
- 3개월 이후: 마지막 행과 값이 같으면 스킵, 다르면 기록
주의:
- 새 탭 생성/헤더 수정/열 재배치 금지
- 탭명 퍼지 매칭(공백/괄호/0패딩 차이 허용)
- 전국 구·신 명칭 정규화(강원도→강원특별자치도, 전라북도→전북특별자치도)
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

# 시트에 쓰길 기대하는 '표준' 시도 목록
SIDO_STD = [
    "강원특별자치도","경기도","경상남도","경상북도","광주광역시","대구광역시","대전광역시",
    "부산광역시","서울특별시","세종특별자치시","울산광역시","인천광역시",
    "전라남도","전북특별자치도","제주특별자치도","충청남도","충청북도"
]

# 구/신 명칭 alias (엑셀에 과거 명칭으로 들어오는 달 대응)
SIDO_ALIAS_TO_STD = {
    "강원도": "강원특별자치도",
    "전라북도": "전북특별자치도",
    # 필요시 추가
}

SEOUL_GU = [
    "강남구","강동구","강북구","강서구","관악구","광진구","구로구","금천구","노원구","도봉구",
    "동대문구","동작구","마포구","서대문구","서초구","성동구","성북구","송파구","양천구",
    "영등포구","용산구","은평구","종로구","중구","중랑구"
]

TOTAL_LABEL_CANDIDATES = ["총합계", "전체 개수", "합계", "총계", "전체"]

def yymm_from_fname(name: str) -> Tuple[int,int]:
    # "전국 2411_250926.xlsx" -> (24, 11)
    m = re.search(r"전국\s+(\d{2})(\d{2})_", name)
    return (int(m.group(1)), int(m.group(2))) if m else (0,0)

def guess_write_date_from_fname(name: str) -> date:
    # "_YYMMDD.xlsx"
    m = re.search(r"_(\d{2})(\d{2})(\d{2})", name)
    return date(2000 + int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else date.today()

def expect_title(kind: str, y2: int, m2: int) -> str:
    return f"{kind} {y2:02d}년 {m2:02d}월"

# ---------------- 엑셀 로드 ----------------
def read_xlsx(path: Path) -> pd.DataFrame:
    log(f"[read] loading xlsx: {path.as_posix()}")
    # 전처리 저장된 'data' 시트 우선
    last_err = None
    for sn in ("data", 0):
        try:
            df = pd.read_excel(path, sheet_name=sn, dtype=str, engine="openpyxl")
            log(f"[read] sheet='{sn}' rows={len(df)} cols={len(df.columns)}")
            break
        except Exception as e:
            last_err = e
            if sn == 0:
                raise
    df = df.fillna("")
    df.columns = [str(c).strip() for c in df.columns]

    # 중복열 방지(첫번째만 유지)
    dup = [c for c in df.columns if df.columns.tolist().count(c) > 1]
    if dup:
        log(f"[read] duplicated columns dropped (keep=first): {dup}")
        seen=set(); keep=[]
        for c in df.columns:
            if c in seen: continue
            seen.add(c); keep.append(c)
        df = df[keep]

    must = {"계약년","계약월"}
    # 전처리된 형태(광역/구/계약년/계약월/계약일 포함)면 그대로 사용
    if {"광역","구","계약년","계약월"}.issubset(set(df.columns)):
        return df

    # '시군구'만 있는 경우 분리(광역/구 추출), 계약년월 분리
    if "시군구" in df.columns:
        parts = df["시군구"].astype(str).str.split(expand=True, n=2)
        df["광역"] = parts[0]
        df["구"] = parts[1] if parts.shape[1] > 1 else ""
        if "계약년월" in df.columns:
            s = df["계약년월"].astype(str).str.replace(r"\D","",regex=True)
            df["계약년"] = s.str[:4]
            df["계약월"] = s.str[4:6]
        if "계약일" not in df.columns:
            # 계약일이 따로 없으면 말일 취급(집계에는 영향 없음)
            df["계약일"] = "01"
        return df

    # 최후: 1행을 헤더로 간주
    if len(df) > 0:
        header = [str(x).strip() for x in df.iloc[0].tolist()]
        df2 = df.iloc[1:].copy()
        df2.columns = header
        df2 = df2.fillna("")
        log("[read] header fallback -> used first row as header")
        return df2
    return df

# ---------------- 집계 ----------------
def _canon_sido(name: str) -> str:
    name = str(name).strip()
    return SIDO_ALIAS_TO_STD.get(name, name)

def aggregate_national(df: pd.DataFrame) -> pd.Series:
    # 광역 명칭 정규화 후 집계
    if "광역" not in df.columns:
        log("[agg] missing column '광역' -> empty series")
        return pd.Series(dtype=int)
    tmp = df.copy()
    tmp["광역"] = tmp["광역"].map(_canon_sido)
    s = tmp.groupby("광역")["광역"].count()
    # 표준 키로 정렬하여 반환
    return pd.Series({k:int(s.get(k,0)) for k in SIDO_STD})

def aggregate_seoul(df: pd.DataFrame) -> pd.Series:
    if "광역" not in df.columns or "구" not in df.columns:
        log("[agg] missing column '광역/구' -> empty series")
        return pd.Series(dtype=int)
    sdf = df[df["광역"].map(_canon_sido)=="서울특별시"]
    s = sdf.groupby("구")["구"].count()
    return pd.Series({k:int(s.get(k,0)) for k in SEOUL_GU})

# ---------------- Sheets ----------------
import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

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
    return re.sub(r"[\s\u3000\-\_()（）]+", "", str(s))

def _title_variants(kind: str, y2: int, m2: int) -> List[str]:
    mm2 = f"{m2:02d}"
    m1  = f"{m2}"
    yy  = f"{y2:02d}"
    bases = [
        f"{kind} {yy}년 {mm2}월", f"{kind} {yy}년 {m1}월",
        f"{kind}{yy}년{mm2}월",   f"{kind}{yy}년{m1}월",
        f"{kind} {yy}년{mm2}월",  f"{kind} {yy}년{m1}월",
        f"{kind}{yy}년 {mm2}월",  f"{kind}{yy}년 {m1}월",
        f"{kind}({yy}년 {mm2}월)",f"{kind}({yy}년 {m1}월)",
        f"{kind}({yy}년{mm2}월)", f"{kind}({yy}년{m1}월)",
    ]
    more = [_norm(b) for b in bases]
    return list(dict.fromkeys(bases + more))

def find_ws_fuzzy(sh, kind: str, y2: int, m2: int):
    wants = _title_variants(kind, y2, m2)
    wants_norm = [_norm(w) for w in wants]
    titles = [ws.title for ws in sh.worksheets()]
    titles_norm = [_norm(t) for t in titles]

    # 정확 정규화 일치
    for t, tn in zip(titles, titles_norm):
        if tn in wants_norm:
            log(f"[ws] fuzzy matched (exact norm): '{t}'")
            return sh.worksheet(t)
    # 접두 정규화 일치
    for t, tn in zip(titles, titles_norm):
        if any(tn.startswith(wn) for wn in wants_norm):
            log(f"[ws] fuzzy matched (prefix norm): '{t}'")
            return sh.worksheet(t)
    # 토큰 포함(0패딩/무패딩 허용)
    yy = f"{y2:02d}"; mm2 = f"{m2:02d}"; m1 = f"{m2}"
    for t in titles:
        if (("전국" if kind=="전국" else "서울") in t) and (yy in t) and ("월" in t) and (mm2 in t or m1 in t):
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

def _first_record_date(ws) -> Optional[date]:
    colA = ws.col_values(1)
    dates = []
    for i,v in enumerate(colA, start=1):
        if i == 1: continue
        try:
            dates.append(date.fromisoformat(str(v).strip()))
        except Exception:
            pass
    return min(dates) if dates else None

def _choose_total_label(header: List[str]) -> Optional[str]:
    for cand in TOTAL_LABEL_CANDIDATES:
        if cand in header:
            return cand
    return None

def _row_same_as_payload(ws, row_idx: int, payload: Dict[int, object]) -> bool:
    cur = ws.row_values(row_idx)
    for cidx, val in payload.items():
        curv = cur[cidx-1] if cidx-1 < len(cur) else ""
        if str(curv) != str(val):
            return False
    return True

def write_row_mapped(ws, when: date, header_full: List[str],
                     series: pd.Series, kind: str) -> str:
    """
    header_full의 '제목'을 기준으로 해당 열만 갱신.
    kind: 'national' or 'seoul'
    """
    if not header_full or header_full[0] != "날짜":
        log(f"[ws] header missing '날짜' -> skip write ({ws.title})")
        return "skip(header)"

    # 제목→열인덱스 맵
    col_map: Dict[str,int] = {}
    for j, t in enumerate(header_full, start=1):
        tt = t.strip()
        if tt:
            col_map[tt] = j

    # 합계 라벨 결정
    total_label = _choose_total_label(header_full)

    # payload 생성(열 인덱스→값)
    payload: Dict[int, object] = {}
    if "날짜" in col_map:
        payload[col_map["날짜"]] = when.isoformat()

    if kind == "national":
        # 표준 키 기준으로 합계 계산
        nat_total = int(series.reindex(SIDO_STD).fillna(0).sum()) if len(series)>0 else 0
        # 각 시도 값: 시트에 구명칭 열(예: '강원도')가 있으면 canonical로부터 값 가져와 채움
        for header_title, cidx in col_map.items():
            if header_title in ("날짜",) + tuple(TOTAL_LABEL_CANDIDATES):
                continue
            # 표준/구명칭 모두 수용
            std_key = _canon_sido(header_title)
            val = int(series.get(std_key, 0)) if std_key in series.index else 0
            if header_title in SIDO_STD or header_title in SIDO_ALIAS_TO_STD.keys():
                payload[cidx] = val
        if total_label:
            payload[col_map[total_label]] = nat_total

    else:  # seoul
        se_total = int(series.sum()) if len(series)>0 else 0
        for header_title, cidx in col_map.items():
            if header_title in ("날짜",) + tuple(TOTAL_LABEL_CANDIDATES):
                continue
            if header_title in SEOUL_GU:
                payload[cidx] = int(series.get(header_title, 0))
        if total_label:
            payload[col_map[total_label]] = se_total

    # 행 인덱스 및 정책 판단
    row_idx, exists = find_or_create_row_index_for_date(ws, when)
    first = _first_record_date(ws)
    if first is not None and (when - first).days > MAX_DAILY_WINDOW_DAYS:
        # 3개월 이후: 마지막 행과 동일하면 스킵
        if _row_same_as_payload(ws, row_idx, payload):
            return "skip(=last)"

    # 개별 셀 업데이트(제목 위치에 정확히 씀)
    for cidx, val in sorted(payload.items()):
        a1 = rowcol_to_a1(row_idx, cidx)
        # gspread 6.x: values first, range second (Deprecation 대응)
        ws.update([[val]], a1)
    return "append" if not exists else "update"

# ---------------- 수집/메인 ----------------
def collect_artifacts(art_dir: Path) -> List[Path]:
    log_block("collect")
    log(f"artifacts_dir={art_dir}")
    paths: List[Path] = []

    zips = list(Path(art_dir).rglob("*.zip"))
    log(f"zip files found: {len(zips)}")
    for z in zips:
        dest = Path("extracted") / z.parent.name
        dest.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(z, "r") as zp:
            zp.extractall(dest)

    # zip 해제된 곳 + 원본 경로 모두 스캔
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
            log(f"[file] skip (no yymm in name): {f.name}")
            continue
        write_day = guess_write_date_from_fname(f.name)
        log(f"[file] {f.name} -> nat='{expect_title('전국', y2, m2)}' seoul='{expect_title('서울', y2, m2)}' date={write_day}")

        # 엑셀 읽기 & 집계
        df = read_xlsx(f)
        nat_series = aggregate_national(df)
        se_series  = aggregate_seoul(df)

        # 탭 퍼지 매칭
        ws_nat = find_ws_fuzzy(sh, "전국", y2, m2)
        ws_se  = find_ws_fuzzy(sh, "서울", y2, m2)

        # 전국
        if ws_nat is None:
            log(f"[전국] {f.name} -> sheet NOT FOUND (skip)")
        else:
            header_nat = get_header(ws_nat)
            op = write_row_mapped(ws_nat, write_day, header_nat, nat_series, kind="national")
            note_where(f"[전국] {ws_nat.title} @ {write_day}: {op}, sum={int(nat_series.sum()) if len(nat_series)>0 else 0}")

        # 서울
        if ws_se is None:
            log(f"[서울] {f.name} -> sheet NOT FOUND (skip)")
        else:
            header_se = get_header(ws_se)
            op = write_row_mapped(ws_se, write_day, header_se, se_series, kind="seoul")
            note_where(f"[서울] {ws_se.title} @ {write_day}: {op}, sum={int(se_series.sum()) if len(se_series)>0 else 0}")

    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {e!r}")
        raise
