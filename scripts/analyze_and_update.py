# -*- coding: utf-8 -*-
"""
Artifacts의 전처리된 엑셀(전국 월별, 서울 1년)을 받아
구글 시트의 기존 탭에 날짜별 집계(전국=광역, 서울=구)를 기록한다.

- 새 탭 생성 금지(없으면 스킵)
- 오래된 파일도 견고하게 읽도록 헤더 자동 감지 + 포지션 폴백
- 3개월 규칙:
  * 해당 탭의 '날짜' 첫 기록일로부터 3개월 이내: 매일 기록/업데이트
  * 3개월 이후: 마지막 기록값과 비교해 동일하면 스킵, 달라지면 기록

작성: 2025-09
"""

from __future__ import annotations
import os, re, sys, json, shutil, zipfile, time
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, Tuple, Optional, List

import numpy as np
import pandas as pd

# ---------------- logging ----------------
RUN_TS = datetime.now()
LOG_DIR = Path("analyze_report")
RUN_LOG = LOG_DIR / f"run-{RUN_TS:%Y%m%d-%H%M%S}.log"
LATEST = LOG_DIR / "latest.log"
WHERE = LOG_DIR / "where_written.txt"

def log(msg: str):
    msg = msg.rstrip()
    line = f"[{RUN_TS:%H:%M:%S}] {msg}"
    print(line, flush=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with RUN_LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
    with LATEST.open("w", encoding="utf-8") as f:
        f.write(line + "\n")  # 마지막 한 줄이라도 보이게
    with LATEST.open("a", encoding="utf-8") as f:
        pass  # tail 추가를 위해 handle 유지 X

def log_block(title: str):
    log(f"[{title}]".upper())

# ---------------- args ----------------
import argparse
ap = argparse.ArgumentParser()
ap.add_argument("--artifacts-dir", required=True)
ap.add_argument("--sa", required=True, help="service account json path")
ap.add_argument("--sheet-id", required=True)
ap.add_argument("--work-dir", default="extracted")
args = ap.parse_args()

# ---------------- collect artifacts ----------------
def collect_artifacts(artifacts_dir: Path, work_dir: Path) -> List[Path]:
    log_block("collect")
    log(f"artifacts_dir={artifacts_dir}")
    work_dir.mkdir(parents=True, exist_ok=True)

    zips = list(Path(artifacts_dir).rglob("*.zip"))
    xlsx_direct = list(Path(artifacts_dir).rglob("*.xlsx"))
    log(f"zip files found: {len(zips)}")
    log(f"direct xlsx found: {len(xlsx_direct)}")

    for z in zips:
        with zipfile.ZipFile(z, "r") as zf:
            zf.extractall(work_dir)

    copied = []
    for x in xlsx_direct:
        dst = work_dir / x.relative_to(artifacts_dir)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(x, dst)
        copied.append(dst)
        log(f"copy: {x} -> {dst}")

    all_xlsx = list(work_dir.rglob("*.xlsx"))
    log(f"total xlsx under work_dir: {len(all_xlsx)}")
    return all_xlsx

# ---------------- robust excel reader ----------------
EXPECTED_HEADERS = [
    "광역","구","법정동","리","번지","본번","부번","단지명",
    "전용면적(㎡)","계약년","계약월","계약일","거래금액(만원)"
]
FALLBACK_BY_POS = [
    "광역","구","법정동","리","번지","본번","부번","단지명",
    "전용면적(㎡)","계약년","계약월","계약일","거래금액(만원)"
]

def _score_header_row(row_vals):
    s = 0
    for v in row_vals:
        t = str(v).strip()
        if t in EXPECTED_HEADERS:
            s += 2
        if any(k in t for k in ["계약", "거래금액", "전용면적", "법정동", "광역"]):
            s += 1
    return s

def _promote_header(df_raw, max_scan_rows=20):
    best_idx, best_score = None, -1
    scan = min(max_scan_rows, len(df_raw))
    for r in range(scan):
        row_vals = df_raw.iloc[r].tolist()
        score = _score_header_row(row_vals)
        if score > best_score:
            best_idx, best_score = r, score
    if best_idx is not None and best_score >= 2:
        cols = df_raw.iloc[best_idx].astype(str).str.strip().tolist()
        df2 = df_raw.iloc[best_idx+1:].copy()
        df2.columns = cols
        return df2
    return None

def _fallback_by_position(df_raw):
    df2 = df_raw.copy()
    n = min(len(FALLBACK_BY_POS), df2.shape[1])
    new_cols = FALLBACK_BY_POS[:n] + [f"col{i+1}" for i in range(df2.shape[1]-n)]
    df2.columns = new_cols
    return df2

def _cleanup_columns(df):
    if df.columns.duplicated().any():
        keep = ~df.columns.duplicated()
        df = df.loc[:, keep]
    empty_cols = [c for c in df.columns if df[c].isna().all()]
    if empty_cols:
        df = df.drop(columns=empty_cols)
    return df

def read_xlsx_robust(xlsx_path: Path, sheet_name: str = "data") -> pd.DataFrame:
    log(f"[read] loading xlsx: {xlsx_path} (sheet='{sheet_name}')")
    try:
        df_raw = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None, engine="openpyxl")
    except Exception:
        df_raw = pd.read_excel(xlsx_path, header=None, engine="openpyxl")
    df_raw = df_raw.replace({np.nan: ""})

    df = _promote_header(df_raw, max_scan_rows=20)
    if df is None:
        df = _fallback_by_position(df_raw)
    df = _cleanup_columns(df)

    # '광역' 유추(첫 두 컬럼) — 필요시 이름 부여
    cols = list(df.columns)
    if "광역" not in cols and len(cols) >= 2:
        sample = df.iloc[:10, :2].astype(str)
        def looks_prov(s):
            return any(k in s for k in ["특별자치도","광역시","서울특별시","세종특별자치시","제주특별자치도","경기도","충청","전라","경상","강원","울산","부산","대구","대전","인천","광주"])
        p_ratio = (sample.iloc[:,0].apply(looks_prov)).mean()
        if p_ratio > 0.5:
            new_cols = cols[:]
            new_cols[0] = "광역"
            new_cols[1] = "구"
            df.columns = new_cols
            cols = new_cols

    # 숫자/날짜 정리
    for c in ["계약년","계약월","계약일"]:
        if c in df.columns:
            df[c] = (df[c].astype(str).str.replace(r"[^0-9]", "", regex=True)).replace({"": np.nan})
    for c in ["거래금액(만원)","전용면적(㎡)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(r"[^0-9.\-]", "", regex=True), errors="coerce")

    log(f"[read] shape=({df.shape[0]},{df.shape[1]}), cols(sample)={list(df.columns)[:12]}")
    return df

# ---------------- Google Sheets ----------------
import gspread
from google.oauth2.service_account import Credentials

def open_sheet(sa_path: Path, sheet_id: str):
    log_block("gspread")
    log("auth with sa.json")
    creds = Credentials.from_service_account_file(
        sa_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    log("spreadsheet opened")
    return sh

def yymm_to_title(prefix: str, yy: str, mm: str) -> str:
    # 예: ("전국","24","10") -> "전국 24년 10월"
    return f"{prefix} {int(yy):02d}년 {int(mm):02d}월"

def parse_nat_filename(p: Path) -> Optional[Tuple[str, str, date]]:
    # "전국 2410_250926.xlsx" -> ("24","10"), write_date=2025-09-26
    m = re.search(r"전국\s*(\d{2})(\d{2})_(\d{6})", p.stem)
    if not m: 
        return None
    yy, mm, d6 = m.group(1), m.group(2), m.group(3)
    write_date = date(2000+int(d6[:2]), int(d6[2:4]), int(d6[4:6]))
    return yy, mm, write_date

def ensure_existing_ws(sh, title: str):
    """없으면 None 반환(생성 금지)."""
    try:
        ws = sh.worksheet(title)
        return ws
    except Exception:
        log(f"[ws] SKIP (no such sheet): '{title}'")
        return None

def row_index_by_date(ws, when: date) -> Optional[int]:
    """A열에서 YYYY-MM-DD 매칭되는 행 찾기."""
    values = ws.col_values(1)
    target = when.isoformat()
    for idx, v in enumerate(values, start=1):
        if str(v).strip() == target:
            return idx
    return None

def get_first_record_date(ws) -> Optional[date]:
    vals = ws.col_values(1)
    for v in vals[1:]:  # header 제외
        s = str(v).strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            y,m,d = int(s[:4]), int(s[5:7]), int(s[8:10])
            return date(y,m,d)
    return None

def last_row_values(ws) -> Optional[List[str]]:
    vals = ws.get_all_values()
    if len(vals) <= 1:
        return None
    # 마지막 실제 데이터 행 찾기(뒤에서부터)
    for r in range(len(vals)-1, 0, -1):
        if any(str(x).strip() for x in vals[r]):
            return vals[r]
    return None

def write_row(ws, when: date, keys: List[str], series: pd.Series) -> str:
    """
    keys: 헤더(두 번째 칼럼부터의 이름들). 날짜 열은 A.
    series: keys 인덱스를 가진 시리즈(부재 키는 0 취급)
    """
    # 헤더
    header = ws.row_values(1)
    # 날짜 + keys 순서
    row_vals = [when.isoformat()]
    for k in keys:
        v = series.get(k, 0)
        try:
            v = int(v)
        except Exception:
            try: v = float(v)
            except Exception: v = 0
        row_vals.append(v)

    # 3개월 정책
    first_dt = get_first_record_date(ws)
    mode_note = ""
    if first_dt:
        if when <= first_dt + timedelta(days=92):
            mode_note = "<=3mo"
        else:
            # 마지막행과 비교
            last = last_row_values(ws)
            if last:
                # 마지막행의 같은 길이로 비교(없는 칼럼 0)
                same = True
                # A열(날짜)은 제외
                for i, k in enumerate(keys, start=2):
                    prev = last[i-1] if i-1 < len(last) and str(last[i-1]).strip() else "0"
                    try:
                        prev_v = int(float(prev))
                    except:
                        prev_v = 0
                    cur_v = int(series.get(k, 0) or 0)
                    if prev_v != cur_v:
                        same = False
                        break
                if same:
                    return f"skip(same-last)"
            mode_note = ">3mo"

    # 이미 그 날짜가 있으면 update, 없으면 append
    idx = row_index_by_date(ws, when)
    if idx is None:
        # append
        ws.append_row(row_vals, value_input_option="USER_ENTERED")
        return f"append({mode_note})"
    else:
        # update range A{idx}:?{idx}
        col_end = chr(ord('A') + len(row_vals) - 1)
        ws.update(range_name=f"A{idx}:{col_end}{idx}", values=[row_vals])
        return f"update({mode_note})"

# ---------------- main ----------------
def main():
    log_block("main")
    log(f"start artifacts={args.artifacts_dir}, sa={args.sa}, sheet_id=***")

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    (LOG_DIR / "sheet_id").write_text(args.sheet_id, encoding="utf-8")
    WHERE.write_text("", encoding="utf-8")

    artifacts = collect_artifacts(Path(args.artifacts_dir), Path(args.work_dir))

    # 전국 월 파일만 사용
    nat_files = [p for p in artifacts if re.search(r"전국\s*\d{4}_\d{6}\.xlsx$", p.name)]
    log(f"national files count={len(nat_files)}")

    sh = open_sheet(Path(args.sa), args.sheet_id)

    written_lines = []

    for p in sorted(nat_files):
        meta = parse_nat_filename(p)
        if not meta:
            continue
        yy, mm, write_day = meta
        nat_title = yymm_to_title("전국", yy, mm)
        seoul_title = yymm_to_title("서울", yy, mm)
        log(f"[file] {p.name} -> nat='{nat_title}' seoul='{seoul_title}' date={write_day}")

        df = read_xlsx_robust(p, sheet_name="data")

        # --------- 전국 집계 (광역별 건수) ----------
        if "광역" not in df.columns:
            log("[agg] missing column '광역' -> empty series")
            nat_series = pd.Series(dtype=int)
        else:
            nat_series = df.groupby("광역").size().astype(int).sort_index()
        # --------- 서울 집계 (서울특별시 필터 후 구별 건수) ----------
        if {"광역","구"}.issubset(df.columns):
            se_df = df[df["광역"].astype(str).str.contains("서울특별시", na=False)]
            if len(se_df)==0:
                log("[서울] no rows for 서울특별시")
                seoul_series = pd.Series(dtype=int)
            else:
                seoul_series = se_df.groupby("구").size().astype(int).sort_index()
        else:
            log("[서울] missing columns -> empty series")
            seoul_series = pd.Series(dtype=int)

        # --- 전국 탭에 기록 ---
        ws_nat = ensure_existing_ws(sh, nat_title)
        if ws_nat is not None and len(nat_series)>0:
            # header(1행) 기준 키 목록(날짜 제외)
            hdr = ws_nat.row_values(1)
            keys_nat = [c for c in hdr[1:] if c]  # B~ 끝
            if not keys_nat:
                # 키가 없으면 series 인덱스로
                keys_nat = nat_series.index.tolist()
            op = write_row(ws_nat, write_day, keys_nat, nat_series)
            log(f"[전국] {p.name} -> {nat_title} @ {write_day}: {op}, sum={int(nat_series.sum())}")
            written_lines.append(f"{nat_title}\t{write_day}\t{op}")
        else:
            log(f"[전국] {p.name} -> no national rows or sheet missing")

        # --- 서울 탭에 기록 ---
        ws_se = ensure_existing_ws(sh, seoul_title)
        if ws_se is not None and len(seoul_series)>0:
            hdr = ws_se.row_values(1)
            keys_se = [c for c in hdr[1:] if c]
            if not keys_se:
                keys_se = seoul_series.index.tolist()
            op2 = write_row(ws_se, write_day, keys_se, seoul_series)
            log(f"[서울] {p.name} -> {seoul_title} @ {write_day}: {op2}, sum={int(seoul_series.sum())}")
            written_lines.append(f"{seoul_title}\t{write_day}\t{op2}")
        else:
            log(f"[서울] {p.name} -> no seoul rows or sheet missing")

    # where_written
    WHERE.write_text("\n".join(written_lines), encoding="utf-8")
    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {e!r}")
        raise
