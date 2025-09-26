# -*- coding: utf-8 -*-
"""
분석 & 기록 파이프라인 (STRICT: 기존 탭만 사용 / 헤더 불변 / 행만 upsert)

- 입력: artifacts/ 아래의 ZIP 또는 xlsx (전국/서울 전처리 산출물)
- 집계:
  · 전국 탭: '광역' 기준 건수 집계
  · 서울 탭 : '광역'=='서울특별시' → '구' 기준 건수 집계
- 기록:
  · "전국 YY년 MM월", "서울 YY년 MM월" **기존 탭만 사용(없으면 SKIP)**
  · 날짜(A열, ISO yyyy-mm-dd)가 이미 있으면 해당 행만 업데이트
  · 없으면 마지막 행 아래로 append
  · 시트 헤더/열 순서는 절대 변경하지 않음(존재하는 열만 반영)
- 로깅:
  · analyze_report/latest.log (마지막 실행 로그)
  · analyze_report/run-YYYYMMDD-HHMMSS.log (개별 실행 로그)
  · analyze_report/sheet_id.txt (대상 스프레드시트 id)
"""

from __future__ import annotations
import os, sys, json, zipfile, shutil, traceback
from pathlib import Path
from datetime import datetime, date
from typing import Tuple, Optional

import pandas as pd
from openpyxl.utils import get_column_letter

# =========================
# 로거(파일/폴더 충돌 안전)
# =========================
LOG_DIR = Path("analyze_report")
_run_stamp = datetime.now().strftime("%Y%m%d-%H%M%S")

def _safe_prepare_logdir():
    # analyze_report가 "파일"로 존재해도 안전하게 처리
    if LOG_DIR.exists():
        if LOG_DIR.is_file():
            backup = Path(f"{LOG_DIR}.bak-{_run_stamp}")
            try:
                LOG_DIR.rename(backup)
            except Exception:
                LOG_DIR.unlink(missing_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

_safe_prepare_logdir()
_run_log = LOG_DIR / f"run-{_run_stamp}.log"
_latest_log = LOG_DIR / "latest.log"

def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    for p in (_run_log, _latest_log):
        with p.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()

def write_text(path: Path, text: str):
    path.write_text(text, encoding="utf-8")

# =========================
# 아티팩트 수집
# =========================
def collect_artifacts(artifacts_dir: Path, work_dir: Path) -> None:
    log(f"[collect] artifacts_dir={artifacts_dir}")
    zips = list(artifacts_dir.rglob("*.zip"))
    xlsx = list(artifacts_dir.rglob("*.xlsx"))
    log(f"[collect] zip files found: {len(zips)}")
    log(f"[collect] direct xlsx found: {len(xlsx)}")

    for z in zips:
        with zipfile.ZipFile(z, "r") as zf:
            zf.extractall(work_dir)

    for x in xlsx:
        dst = work_dir / x.relative_to(artifacts_dir)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(x, dst)
        log(f"[collect] copy: {x} -> {dst}")

    total = len(list(work_dir.rglob("*.xlsx")))
    log(f"[collect] total xlsx under work_dir: {total}")

# =========================
# 엑셀 로드 (전처리본 가정, 시트/헤더 견고)
# =========================
EXPECTED_HEADERS = {
    "광역","구","법정동","리","전용면적(㎡)","계약년","계약월","계약일","거래금액(만원)","단지명"
}

def _try_read(path: Path, sheet_name: Optional[str]) -> pd.DataFrame:
    info = f"sheet='{sheet_name}'" if sheet_name is not None else "sheet=0"
    log(f"[read] loading xlsx: {path} ({info})")
    return pd.read_excel(path, engine="openpyxl", dtype=str, sheet_name=sheet_name)

def _detect_header_row(df: pd.DataFrame, max_scan: int = 50) -> int:
    # 0..max_scan-1 범위에서 기대 헤더와 교집합 개수가 가장 큰 행 선택 (최소 3개 이상일 때 채택)
    best_idx, best_score = 0, -1
    n = min(max_scan, len(df))
    for i in range(n):
        row = [str(x).strip() for x in df.iloc[i].tolist()]
        score = sum(1 for x in row if x in EXPECTED_HEADERS)
        if score > best_score:
            best_idx, best_score = i, score
    log(f"[read] header_try: chosen_row={best_idx}, match_count={best_score}")
    # 3 미만이면 그래도 best_idx 사용하되 이후 유효성 검증에서 걸러짐
    return best_idx

def _finalize_after_header(df: pd.DataFrame, hdr_row: int) -> pd.DataFrame:
    # 헤더 지정 + 데이터 영역
    df.columns = df.iloc[hdr_row].astype(str).str.strip()
    df = df.iloc[hdr_row + 1 :].reset_index(drop=True)

    # 중복 컬럼 제거(첫 발생만 유지)
    dup_mask = df.columns.duplicated(keep="first")
    if dup_mask.any():
        dups = [c for c, m in zip(df.columns, dup_mask) if m]
        log(f"[read] duplicated columns dropped (keep=first): {dups}")
        df = df.loc[:, ~dup_mask]

    # 문자열 컬럼 정리
    obj_cols = df.select_dtypes(include=["object"]).columns.tolist()
    for c in obj_cols:
        df[c] = df[c].astype(str).str.strip()

    log(f"[read] shape={df.shape}, cols(sample)={list(df.columns)[:12]}")
    return df

def read_table(path: Path) -> pd.DataFrame:
    # 1차: sheet='data' 시도
    try:
        df = _try_read(path, sheet_name="data")
    except Exception as e:
        log(f"[read] sheet='data' failed: {e}")
        df = _try_read(path, sheet_name=0)  # 폴백: 첫 시트

    # 헤더 탐지
    hdr_row = _detect_header_row(df, max_scan=50)
    df2 = _finalize_after_header(df, hdr_row)

    # 유효성 검사: 최소 핵심 컬럼 존재 여부
    needed = {"계약년","계약월"}
    if not (("광역" in df2.columns) and needed.issubset(df2.columns)):
        log("[read] header validation failed -> fallback to first row as header")
        try:
            # 폴백: 0행을 헤더로 가정 후 다시 유효성 검증
            df_fb = _finalize_after_header(df, 0)
            if ("광역" in df_fb.columns) and needed.issubset(df_fb.columns):
                log("[read] fallback header accepted (row=0)")
                return df_fb
        except Exception as e:
            log(f"[read] fallback failed: {e}")

    return df2

# =========================
# 집계 정의
# =========================
PROVINCES = [
    "강원특별자치도","경기도","경상남도","경상북도","광주광역시","대구광역시","대전광역시",
    "부산광역시","서울특별시","세종특별자치시","울산광역시","인천광역시",
    "전라남도","전북특별자치도","제주특별자치도","충청남도","충청북도",
]
SEOUL_GU = [
    "강남구","강동구","강북구","강서구","관악구","광진구","구로구","금천구","노원구","도봉구",
    "동대문구","동작구","마포구","서대문구","서초구","성동구","성북구","송파구","양천구","영등포구",
    "용산구","은평구","종로구","중구","중랑구"
]

def aggregate(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    if "광역" not in df.columns:
        log("[agg] missing column '광역' -> empty series")
        return pd.Series(dtype="int64"), pd.Series(dtype="int64")

    # 전국(광역) 카운트
    s_nat = df.groupby("광역")["광역"].count()
    s_nat.index.name = None
    s_nat = s_nat.reindex(PROVINCES).fillna(0).astype(int)

    # 서울(구) 카운트
    if "구" in df.columns:
        seoul = df[df["광역"] == "서울특별시"]
        s_seoul = seoul.groupby("구")["구"].count()
        s_seoul.index.name = None
        s_seoul = s_seoul.reindex(SEOUL_GU).fillna(0).astype(int)
    else:
        log("[agg] missing column '구' -> empty seoul series")
        s_seoul = pd.Series(dtype="int64")

    log(f"[agg] nat len={s_nat.shape[0]} seoul len={s_seoul.shape[0]}")
    return s_nat, s_seoul

# =========================
# 파일명 → 탭명/날짜
# =========================
def parse_titles_and_date(fname: str) -> Tuple[str, str, date]:
    # 예: "전국 2410_250926.xlsx" → 전국 24년 10월 / 서울 24년 10월 / 2025-09-26
    stem = Path(fname).stem
    tail = stem.split("_")[-1]            # YYMMDD
    y = 2000 + int(tail[:2])
    m = int(tail[2:4])
    d = int(tail[4:6])
    when = date(y, m, d)

    head = stem.split("_")[0]             # "전국 2410"
    ym = head.split()[-1]                 # "2410"
    yy = int(ym[:2]); mm = int(ym[2:4])

    nat_title = f"전국 {yy:02d}년 {mm:02d}월"
    seoul_title = f"서울 {yy:02d}년 {mm:02d}월"
    log(f"[file] {Path(fname).name} -> nat='{nat_title}' seoul='{seoul_title}' date={when.isoformat()}")
    return nat_title, seoul_title, when

# =========================
# gspread
# =========================
def open_sheet(sa_json_path: Path, sheet_id: str):
    log("[gspread] auth with sa.json")
    from google.oauth2.service_account import Credentials
    import gspread
    creds = Credentials.from_service_account_file(
        sa_json_path.as_posix(),
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    log("[gspread] spreadsheet opened")
    write_text(LOG_DIR / "sheet_id.txt", sheet_id)
    return sh

def get_ws_existing(sh, title: str):
    """기존 탭만 열고, 없으면 None (생성/헤더 수정 금지)"""
    try:
        return sh.worksheet(title)
    except Exception:
        log(f"[ws] SKIP: sheet not found (no create): '{title}'")
        return None

# =========================
# 날짜 행만 upsert (헤더/열 불변)
# =========================
def write_row_strict(ws, when: date, counts: pd.Series) -> str:
    """
    시트 구조 변경 없이 날짜 행만 업데이트/append.
    counts: {헤더명: 값} (시트에 없는 열은 무시)
    """
    rows = ws.get_all_values() or []
    if not rows:
        log(f"[ws] empty sheet -> skip: '{ws.title}'")
        return "skip(empty sheet)"

    header = rows[0]
    date_col_idx = 0  # A열(날짜)

    # 시트에 존재하는 열만 사용
    row_vals = ["" for _ in header]
    row_vals[date_col_idx] = when.isoformat()
    for j, col_name in enumerate(header):
        if j == date_col_idx:
            continue
        try:
            v = int(counts.get(col_name, 0))
        except Exception:
            v = 0
        row_vals[j] = v

    # 기존 날짜 행 찾기
    target_idx = None
    for i, r in enumerate(rows[1:], start=2):
        if r and r[date_col_idx].strip() == when.isoformat():
            target_idx = i
            break

    end_col = get_column_letter(len(header))

    if target_idx:
        ws.update([row_vals], range_name=f"A{target_idx}:{end_col}{target_idx}")
        log(f"[verify] updated row {target_idx} in '{ws.title}'")
        return "update"
    else:
        ws.append_row(row_vals, value_input_option="RAW")
        log(f"[verify] appended 1 row in '{ws.title}'")
        return "append"

# =========================
# main
# =========================
def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts-dir", required=True)
    ap.add_argument("--sa", required=True, help="service account json path")
    ap.add_argument("--sheet-id", required=True)
    args = ap.parse_args()

    log(f"[main] start artifacts={Path(args.artifacts_dir).resolve()}, sa={args.sa}, sheet_id=***")

    # 깨끗한 작업 디렉터리
    work_dir = Path("extracted")
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    # 수집
    collect_artifacts(Path(args.artifacts_dir), work_dir)

    # 파일 목록 필터(전국만 - 서울은 전국 파일에서 같이 계산)
    files = sorted(work_dir.rglob("*.xlsx"))
    nat_files = [p for p in files if p.name.startswith("전국 ")]
    log(f"[main] national files count={len(nat_files)}")

    # 시트 오픈
    sh = open_sheet(Path(args.sa), args.sheet_id)

    # 처리 루프
    for p in nat_files:
        try:
            nat_title, seoul_title, write_day = parse_titles_and_date(p.name)
            df = read_table(p)
            nat_series, seoul_series = aggregate(df)

            # 전국 탭
            if not nat_series.empty:
                ws_nat = get_ws_existing(sh, nat_title)
                if ws_nat:
                    op = write_row_strict(ws_nat, write_day, nat_series)
                    log(f"[전국] {p.name} -> {nat_title} @ {write_day}: {op}, sum={int(nat_series.sum())}")
                else:
                    log(f"[전국] {p.name} -> SKIP(no sheet): {nat_title}")
            else:
                log(f"[전국] {p.name} -> no national rows")

            # 서울 탭
            if not seoul_series.empty:
                ws_se = get_ws_existing(sh, seoul_title)
                if ws_se:
                    op2 = write_row_strict(ws_se, write_day, seoul_series)
                    log(f"[서울] {p.name} -> {seoul_title} @ {write_day}: {op2}, sum={int(seoul_series.sum())}")
                else:
                    log(f"[서울] {p.name} -> SKIP(no sheet): {seoul_title}")
            else:
                log(f"[서울] {p.name} -> no seoul rows")

        except Exception as e:
            log(f"[ERROR] {p.name}: {e}")
            tb = traceback.format_exc()
            for ln in tb.rstrip().splitlines():
                log(f"[TB] {ln}")

    log("[main] done")

if __name__ == "__main__":
    main()
