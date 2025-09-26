# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import zipfile
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Tuple, Optional

import pandas as pd


# ===================== 로거 =====================
class RunLogger:
    def __init__(self):
        self.lines: List[str] = []
        self.t0 = datetime.now()

    def log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        self.lines.append(line)
        print(line, flush=True)

    def dump(self, report_dir: Path, sheet_id: str):
        """report_dir가 파일로 존재해도 안전하게 폴더 보장 후 로그 저장"""
        # 폴더 보장(동명 파일 충돌 회피)
        if report_dir.exists() and not report_dir.is_dir():
            try:
                report_dir.unlink()  # 파일이면 제거
            except Exception:
                # 이름 바꿔 피함
                bak = report_dir.with_name(report_dir.name + ".bak")
                try:
                    report_dir.rename(bak)
                except Exception:
                    pass
        report_dir.mkdir(parents=True, exist_ok=True)

        # 파일 저장
        run_id = self.t0.strftime("run-%Y%m%d-%H%M%S")
        (report_dir / "sheet_id.txt").write_text(sheet_id, encoding="utf-8")
        (report_dir / "latest.log").write_text("\n".join(self.lines) or "(no logs)", encoding="utf-8")
        (report_dir / f"{run_id}.log").write_text("\n".join(self.lines) or "(no logs)", encoding="utf-8")


logger = RunLogger()
log = logger.log


# ===================== 파일명 파서 =====================
def parse_national_fname(fname: str) -> Optional[Tuple[int, int, date]]:
    """
    '전국 2410_250926.xlsx' -> (year=2024, month=10, write_date=2025-09-26)
    """
    m = re.match(r"전국\s+(\d{4})_(\d{6})\.xlsx$", fname)
    if not m:
        return None
    yyMM, yymmdd = m.group(1), m.group(2)
    year = 2000 + int(yyMM[:2])
    month = int(yyMM[2:])
    wyear = 2000 + int(yymmdd[:2])
    wmonth = int(yymmdd[2:4])
    wday = int(yymmdd[4:6])
    return year, month, date(wyear, wmonth, wday)


# ===================== 아티팩트 수집 =====================
def collect_artifacts(artifacts_dir: Path, work_dir: Path) -> List[Path]:
    """
    artifacts_dir 아래:
      - *.zip 발견 시 work_dir로 해제
      - 이미 풀린 *.xlsx는 work_dir로 복사(구조 보존)
    최종적으로 work_dir 아래의 모든 xlsx 경로 리스트 반환
    """
    log(f"[collect] artifacts_dir={artifacts_dir}")
    if work_dir.exists() and work_dir.is_file():
        log(f"[collect] WARN: work_dir exists as file, removing: {work_dir}")
        work_dir.unlink()
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    # 1) zip 해제
    zips = list(artifacts_dir.rglob("*.zip"))
    log(f"[collect] zip files found: {len(zips)}")
    for zp in zips:
        try:
            with zipfile.ZipFile(zp, "r") as zf:
                target = work_dir / zp.stem
                log(f"[collect] extracting zip: {zp} -> {target}")
                zf.extractall(target)
        except zipfile.BadZipFile:
            log(f"[collect] WARN: bad zip ignored: {zp}")

    # 2) xlsx 복사(미러링)
    xlsx_src = list(artifacts_dir.rglob("*.xlsx"))
    log(f"[collect] direct xlsx found: {len(xlsx_src)}")
    for p in xlsx_src:
        try:
            rel = p.relative_to(artifacts_dir)
        except ValueError:
            rel = Path(p.name)
        dst = (work_dir / rel).resolve()
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not dst.exists():
            try:
                shutil.copy2(p.as_posix(), dst.as_posix())
                log(f"[collect] copy: {p} -> {dst}")
            except Exception as e:
                log(f"[collect] WARN: copy failed: {p} -> {dst}: {e}")

    # 3) 최종 수집
    xlsxs = list(work_dir.rglob("*.xlsx"))
    log(f"[collect] total xlsx under work_dir: {len(xlsxs)}")
    return xlsxs


# ===================== 데이터 로드/집계 =====================
def read_xlsx(path: Path) -> pd.DataFrame:
    log(f"[read] loading xlsx: {path}")
    df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    df.columns = df.columns.str.strip()
    log(f"[read] shape={df.shape}, cols={list(df.columns)}")
    return df


def aggregate_from_national_file(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """
    전국 파일 1개로부터:
      - 전국 탭: '광역'별 건수
      - 서울 탭: '광역' == '서울특별시'만 필터 후 '구'별 건수
    """
    nat_series = pd.Series(dtype=int)
    seoul_series = pd.Series(dtype=int)

    if "광역" in df.columns:
        nat_series = df["광역"].value_counts().sort_index()
    else:
        log("[agg] WARN: '광역' 컬럼 없음")

    if {"광역", "구"}.issubset(df.columns):
        seoul_df = df[df["광역"] == "서울특별시"].copy()
        if not seoul_df.empty:
            seoul_series = seoul_df["구"].value_counts().sort_index()
        else:
            log("[agg] INFO: 서울특별시 행 없음")
    else:
        log("[agg] WARN: '광역' 또는 '구' 컬럼 없음")

    log(f"[agg] nat len={len(nat_series)}, seoul len={len(seoul_series)}")
    return nat_series, seoul_series


# ===================== gspread 유틸 =====================
def connect_gspread(sa_json_path: Path):
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    log(f"[gspread] auth with {sa_json_path}")
    creds = Credentials.from_service_account_file(sa_json_path.as_posix(), scopes=scopes)
    gc = gspread.authorize(creds)
    return gc


def ensure_ws(sh, title: str, header: List[str]):
    """
    시트가 없으면 생성, 있으면 헤더를 '날짜 + 항목 유니온'으로 동기화.
    gspread 최신 시그니처( values 먼저, range_name 둘째 )로 update 호출.
    '모든 비고정 행 삭제 불가' 오류를 피하기 위해 최소 행 수를 보장.
    """
    log(f"[ws] ensure '{title}' with header keys={header[1:]}")
    try:
        ws = sh.worksheet(title)
        log("[ws] found existing sheet")
    except Exception:
        log("[ws] not found, creating sheet")
        ws = sh.add_worksheet(title=title, rows=5000, cols=max(26, len(header)))
        ws.update([header], range_name="A1")
        return ws

    rows = ws.get_all_values() or []
    cur_header = rows[0] if rows else []
    desired = ["날짜"] + sorted(set(cur_header[1:] + header[1:]))
    frozen = ws._properties.get("gridProperties", {}).get("frozenRowCount", 0) or 0
    min_rows = max(2, frozen + 1)
    cur_rows = max(ws.row_count, len(rows) or 1)
    target_rows = max(cur_rows, min_rows)
    target_cols = max(ws.col_count, len(desired))

    log(f"[ws] frozen={frozen} cur_rows={ws.row_count} cur_cols={ws.col_count} "
        f"-> resize rows={target_rows} cols={target_cols}")

    if ws.row_count != target_rows or ws.col_count != target_cols:
        ws.resize(rows=target_rows, cols=target_cols)

    if cur_header != desired:
        log(f"[ws] header change: {cur_header} -> {desired}")
        ws.update([desired], range_name="A1")
    else:
        log("[ws] header OK (no change)")

    return ws


def parse_date_str(s: str) -> Optional[date]:
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d", "%y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    try:
        sp = re.sub(r"[^\d]", "-", s)
        return datetime.strptime(sp, "%Y-%m-%d").date()
    except Exception:
        return None


def find_date_row_index(ws, d: date) -> Optional[int]:
    rows = ws.get_all_values()
    if len(rows) <= 1:
        return None
    for i, r in enumerate(rows[1:], start=2):
        if r and parse_date_str(r[0]) == d:
            return i
    return None


def first_record_date(ws) -> Optional[date]:
    rows = ws.get_all_values()
    if len(rows) <= 1:
        return None
    dates = [parse_date_str(r[0]) for r in rows[1:] if r and r[0]]
    dates = [d for d in dates if d]
    return min(dates) if dates else None


def last_row_values(ws) -> Optional[Tuple[List[str], List[int]]]:
    rows = ws.get_all_values()
    if len(rows) <= 1:
        return None
    header = rows[0]
    for r in reversed(rows[1:]):
        if any(c.strip() for c in r):
            vals = []
            for c in header[1:]:
                idx = header.index(c)
                try:
                    v = int(float(r[idx])) if len(r) > idx and r[idx] != "" else 0
                except Exception:
                    v = 0
                vals.append(v)
            return header, vals
    return None


def write_row(ws, when: date, header_entities: List[str], counts: pd.Series, mode: str) -> str:
    """
    mode:
      - 'force': 날짜 존재하든 말든 해당 날짜에 덮어쓰기/추가 보장
      - 'smart': 3개월 이후엔 마지막 행과 동일하면 skip
    """
    rows = ws.get_all_values() or []
    header = rows[0] if rows else ["날짜"]
    desired = ["날짜"] + sorted(set(header[1:] + header_entities))

    frozen = ws._properties.get("gridProperties", {}).get("frozenRowCount", 0) or 0
    min_rows = max(2, frozen + 1)
    cur_rows = max(ws.row_count, len(rows) or 1)
    target_rows = max(cur_rows, min_rows)
    target_cols = max(ws.col_count, len(desired))
    if ws.row_count != target_rows or ws.col_count != target_cols:
        log(f"[ws] write_row resize rows={target_rows} cols={target_cols}")
        ws.resize(rows=target_rows, cols=target_cols)

    if header != desired:
        log(f"[ws] write_row header fix: {header} -> {desired}")
        ws.update([desired], range_name="A1")
        header = desired

    row_vals = [when.isoformat()] + [int(counts.get(k, 0)) for k in header[1:]]
    log(f"[ws] row values for {when}: {row_vals[:6]}... len={len(row_vals)}")

    def _upsert_row():
        idx = find_date_row_index(ws, when)
        if idx:
            col_end = chr(ord("A") + len(row_vals) - 1)
            log(f"[ws] update existing row idx={idx}")
            ws.update([row_vals], range_name=f"A{idx}:{col_end}{idx}")
            return "update"
        else:
            log("[ws] append new row")
            ws.append_row(row_vals, value_input_option="RAW")
            return "append"

    if mode == "force":
        res = _upsert_row()
        log(f"[policy] mode=force -> {res}")
        return res

    # smart 모드
    frst = first_record_date(ws)
    log(f"[policy] first_record_date={frst}, when={when}")
    if frst and when <= frst + timedelta(days=92):
        res = _upsert_row()
        log(f"[policy] <=3mo -> {res}")
        return res + "(<=3mo)"
    else:
        last = last_row_values(ws)
        if last:
            hdr, last_vals = last
            cur_vals = [int(counts.get(k, 0)) for k in hdr[1:]]
            log(f"[policy] compare last vs current: last={sum(last_vals)} sum, equal={cur_vals==last_vals}")
            if cur_vals == last_vals:
                log("[policy] skip: same as last")
                return "skip(same as last)"
        res = _upsert_row()
        log(f"[policy] >3mo -> {res}")
        return res + "(>3mo)"


# ===================== 메인 =====================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts-dir", required=True)
    ap.add_argument("--sa", required=True)
    ap.add_argument("--sheet-id", required=True)
    args = ap.parse_args()

    artifacts = Path(args.artifacts_dir).resolve()
    work = Path("extracted").resolve()

    log(f"[main] start artifacts={artifacts}, sa={args.sa}, sheet_id={args.sheet_id}")

    try:
        # 1) 아티팩트 수집(zip/직접 xlsx 모두 지원)
        all_xlsx = collect_artifacts(artifacts, work)
        nat_files = [p for p in all_xlsx if p.name.startswith("전국 ")]
        nat_files.sort(key=lambda x: x.name)
        log(f"[main] national files count={len(nat_files)}")

        if not nat_files:
            log("[main] EXIT: no national files")
            return

        # 2) 구글 시트 연결
        gc = connect_gspread(Path(args.sa))
        sh = gc.open_by_key(args.sheet_id)
        log("[gspread] spreadsheet opened")

        # 3) 각 파일 처리
        for p in nat_files:
            meta = parse_national_fname(p.name)
            if not meta:
                log(f"[skip] filename pattern not matched: {p.name}")
                continue

            year, month, write_day = meta
            yy = year % 100
            nat_title = f"전국 {yy:02d}년 {month:02d}월"
            seoul_title = f"서울 {yy:02d}년 {month:02d}월"
            log(f"[file] {p.name} -> nat='{nat_title}' seoul='{seoul_title}' date={write_day}")

            df = read_xlsx(p)
            nat_series, seoul_series = aggregate_from_national_file(df)

            # 전국 탭
            if not nat_series.empty:
                nat_header = ["날짜"] + sorted(nat_series.index.tolist())
                ws_nat = ensure_ws(sh, nat_title, nat_header)
                op = write_row(ws_nat, write_day, nat_series.index.tolist(), nat_series, mode="smart")
                log(f"[전국] {p.name} -> {nat_title} @ {write_day}: {op}, sum={int(nat_series.sum())}")
            else:
                log(f"[전국] {p.name} -> no data")

            # 서울 탭
            if not seoul_series.empty:
                seoul_header = ["날짜"] + sorted(seoul_series.index.tolist())
                ws_se = ensure_ws(sh, seoul_title, seoul_header)
                op2 = write_row(ws_se, write_day, seoul_series.index.tolist(), seoul_series, mode="smart")
                log(f"[서울] {p.name} -> {seoul_title} @ {write_day}: {op2}, sum={int(seoul_series.sum())}")
            else:
                log(f"[서울] {p.name} -> no seoul rows")

    except Exception as e:
        # 예외도 상세 출력
        log(f"[ERROR] {type(e).__name__}: {e}")
        raise
    finally:
        # 어떤 경우에도 로그 파일 남김
        logger.dump(Path("analyze_report"), args.sheet_id)
        log("[main] logs written to analyze_report/")


if __name__ == "__main__":
    main()
