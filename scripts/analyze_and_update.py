# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import re
import shutil
import zipfile
from pathlib import Path
from datetime import datetime, date, timedelta
import pandas as pd


# ===================== 파일명 파서 =====================
def parse_national_fname(fname: str):
    """
    '전국 2410_250926.xlsx' -> (year=2024, month=10, write_date=date(2025,9,26))
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
def collect_artifacts(artifacts_dir: Path, work_dir: Path) -> list[Path]:
    """
    artifacts_dir 아래:
      - *.zip 발견 시 work_dir로 해제
      - 이미 풀린 *.xlsx는 work_dir로 복사(구조 보존)
    최종적으로 work_dir 아래의 모든 xlsx 경로 리스트 반환
    """
    work_dir.mkdir(parents=True, exist_ok=True)

    # 1) zip 해제
    zips = list(artifacts_dir.rglob("*.zip"))
    for zp in zips:
        try:
            with zipfile.ZipFile(zp, "r") as zf:
                zf.extractall(work_dir / zp.stem)
        except zipfile.BadZipFile:
            # 이미 액션이 해제한 경우가 있어 무시
            pass

    # 2) xlsx 복사(미러링)
    for p in artifacts_dir.rglob("*.xlsx"):
        try:
            rel = p.relative_to(artifacts_dir)
        except ValueError:
            rel = p.name
        dst = (work_dir / rel).resolve()
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not dst.exists():
            try:
                shutil.copy2(p.as_posix(), dst.as_posix())
            except Exception:
                pass

    # 3) 최종 수집
    return list(work_dir.rglob("*.xlsx"))


# ===================== 데이터 로드/집계 =====================
def read_xlsx(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    df.columns = df.columns.str.strip()
    return df


def aggregate_from_national_file(df: pd.DataFrame):
    """
    전국 파일 1개로부터:
      - 전국 탭: '광역'별 건수
      - 서울 탭: '광역' == '서울특별시'만 필터 후 '구'별 건수
    """
    nat_series = df["광역"].value_counts().sort_index() if "광역" in df.columns else pd.Series(dtype=int)

    seoul_series = pd.Series(dtype=int)
    if {"광역", "구"}.issubset(df.columns):
        seoul_df = df[df["광역"] == "서울특별시"].copy()
        if not seoul_df.empty:
            seoul_series = seoul_df["구"].value_counts().sort_index()

    return nat_series, seoul_series


# ===================== gspread 유틸 =====================
def connect_gspread(sa_json_path: Path):
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(sa_json_path.as_posix(), scopes=scopes)
    return gspread.authorize(creds)


def ensure_ws(sh, title: str, header: list[str]):
    """
    시트가 없으면 생성, 있으면 헤더를 '날짜 + 항목 유니온'으로 동기화.
    gspread 최신 시그니처( values 먼저, range_name 둘째 )로 update 호출.
    '모든 비고정 행 삭제 불가' 오류를 피하기 위해 최소 행 수를 보장.
    """
    import gspread

    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=5000, cols=max(26, len(header)))
        ws.update([header], range_name="A1")
        return ws

    rows = ws.get_all_values() or []
    cur_header = rows[0] if rows else []

    # 원하는 헤더 = '날짜' + (기존헤더/새헤더 합집합 정렬)
    desired = ["날짜"] + sorted(set(cur_header[1:] + header[1:]))

    # 안전 리사이즈(행 최소 2행 또는 frozenRowCount+1 이상 유지)
    frozen = ws._properties.get("gridProperties", {}).get("frozenRowCount", 0) or 0
    min_rows = max(2, frozen + 1)
    cur_rows = max(ws.row_count, len(rows) or 1)
    target_rows = max(cur_rows, min_rows)
    target_cols = max(ws.col_count, len(desired))

    if ws.row_count != target_rows or ws.col_count != target_cols:
        ws.resize(rows=target_rows, cols=target_cols)

    if cur_header != desired:
        ws.update([desired], range_name="A1")

    return ws


def parse_date_str(s: str) -> date | None:
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


def find_date_row_index(ws, d: date) -> int | None:
    rows = ws.get_all_values()
    if len(rows) <= 1:
        return None
    for i, r in enumerate(rows[1:], start=2):
        if r and parse_date_str(r[0]) == d:
            return i
    return None


def first_record_date(ws) -> date | None:
    rows = ws.get_all_values()
    if len(rows) <= 1:
        return None
    dates = [parse_date_str(r[0]) for r in rows[1:] if r and r[0]]
    dates = [d for d in dates if d]
    return min(dates) if dates else None


def last_row_values(ws) -> tuple[list[str], list[int]] | None:
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


def write_row(ws, when: date, header_entities: list[str], counts: pd.Series, mode: str):
    """
    mode:
      - 'force': 날짜 존재하든 말든 해당 날짜에 덮어쓰기/추가 보장
      - 'smart': 3개월 이후엔 마지막 행과 동일하면 skip
    """
    rows = ws.get_all_values() or []
    header = rows[0] if rows else ["날짜"]
    desired = ["날짜"] + sorted(set(header[1:] + header_entities))

    # 안전 리사이즈(행 최소 2행 또는 frozen+1)
    frozen = ws._properties.get("gridProperties", {}).get("frozenRowCount", 0) or 0
    min_rows = max(2, frozen + 1)
    cur_rows = max(ws.row_count, len(rows) or 1)
    target_rows = max(cur_rows, min_rows)
    target_cols = max(ws.col_count, len(desired))
    if ws.row_count != target_rows or ws.col_count != target_cols:
        ws.resize(rows=target_rows, cols=target_cols)

    if header != desired:
        ws.update([desired], range_name="A1")
        header = desired

    # 행 데이터 구성
    row_vals = [when.isoformat()] + [int(counts.get(k, 0)) for k in header[1:]]

    def _upsert_row():
        idx = find_date_row_index(ws, when)
        if idx:
            col_end = chr(ord("A") + len(row_vals) - 1)
            ws.update([row_vals], range_name=f"A{idx}:{col_end}{idx}")
            return "update"
        else:
            ws.append_row(row_vals, value_input_option="RAW")
            return "append"

    if mode == "force":
        return _upsert_row()

    # smart 모드
    frst = first_record_date(ws)
    if frst and when <= frst + timedelta(days=92):  # 최초 기록일 + 약 3개월
        res = _upsert_row()
        return res + "(<=3mo)"
    else:
        last = last_row_values(ws)
        if last:
            hdr, last_vals = last
            cur_vals = [int(counts.get(k, 0)) for k in hdr[1:]]
            if cur_vals == last_vals:
                return "skip(same as last)"
        res = _upsert_row()
        return res + "(>3mo)"


# ===================== 리포트 파일 =====================
def write_reports(report_dir: Path, sheet_id: str, logs: list[str]):
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "sheet_id.txt").write_text(sheet_id, encoding="utf-8")
    (report_dir / "log.txt").write_text("\n".join(logs) if logs else "(no logs)", encoding="utf-8")


# ===================== 메인 =====================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts-dir", required=True)  # artifacts/outputs-<runid>/output/*.xlsx
    ap.add_argument("--sa", required=True)             # sa.json
    ap.add_argument("--sheet-id", required=True)
    args = ap.parse_args()

    artifacts = Path(args.artifacts_dir)
    work = Path("extracted")
    if work.exists():
        shutil.rmtree(work)

    logs: list[str] = []

    try:
        # 1) 아티팩트 수집(zip/직접 xlsx 모두 지원)
        all_xlsx = collect_artifacts(artifacts, work)
        logs.append(f"[collect] total xlsx found: {len(all_xlsx)}")

        # 2) '전국 ' 파일만 대상
        nat_files = [p for p in all_xlsx if p.name.startswith("전국 ")]
        nat_files.sort(key=lambda x: x.name)
        logs.append(f"[collect] national files: {len(nat_files)}")

        if not nat_files:
            print("\n".join(logs))
            write_reports(Path("analyze_report"), args.sheet_id, logs + ["[exit] no national files"])
            return

        # 3) 구글 시트 연결
        gc = connect_gspread(Path(args.sa))
        sh = gc.open_by_key(args.sheet_id)

        # 4) 각 파일 처리
        for p in nat_files:
            meta = parse_national_fname(p.name)
            if not meta:
                logs.append(f"[skip] filename pattern not matched: {p.name}")
                continue

            year, month, write_day = meta
            yy = year % 100
            nat_title = f"전국 {yy:02d}년 {month:02d}월"
            seoul_title = f"서울 {yy:02d}년 {month:02d}월"

            df = read_xlsx(p)
            nat_series, seoul_series = aggregate_from_national_file(df)

            # 전국 탭
            if not nat_series.empty:
                nat_header = ["날짜"] + sorted(nat_series.index.tolist())
                ws_nat = ensure_ws(sh, nat_title, nat_header)
                op = write_row(ws_nat, write_day, nat_series.index.tolist(), nat_series, mode="smart")
                logs.append(f"[전국] {p.name} → {nat_title} @ {write_day}: {op}, sum={int(nat_series.sum())}")
            else:
                logs.append(f"[전국] {p.name} → no data")

            # 서울 탭
            if not seoul_series.empty:
                seoul_header = ["날짜"] + sorted(seoul_series.index.tolist())
                ws_se = ensure_ws(sh, seoul_title, seoul_header)
                op2 = write_row(ws_se, write_day, seoul_series.index.tolist(), seoul_series, mode="smart")
                logs.append(f"[서울] {p.name} → {seoul_title} @ {write_day}: {op2}, sum={int(seoul_series.sum())}")
            else:
                logs.append(f"[서울] {p.name} → no seoul rows")

    except Exception as e:
        logs.append(f"[ERROR] {type(e).__name__}: {e}")
        raise
    finally:
        write_reports(Path("analyze_report"), args.sheet_id, logs)
        print("\n".join(logs))


if __name__ == "__main__":
    main()
