
# analyze_and_update.py
# 실행: python scripts/analyze_and_update.py --artifacts-dir artifacts --sa sa.json --sheet-id 16If...

from __future__ import annotations
import argparse, zipfile, shutil, re, os
from pathlib import Path
from datetime import datetime
import pandas as pd

# ================== 파일명 파서 ==================
def yyyymm_from_filename(fname: str) -> str | None:
    # '전국 2508_20250926.xlsx' -> '2025-08'
    m = re.search(r"전국\s+(\d{4})_\d{8}", fname)
    if not m:
        return None
    yyMM = m.group(1)  # '2508'
    yy, MM = yyMM[:2], yyMM[2:]
    year = 2000 + int(yy)
    return f"{year}-{MM}"

def yyyymmdd_from_filename(fname: str) -> str | None:
    # '서울시 20250926.xlsx' -> '2025-09-26'
    m = re.search(r"서울시\s+(\d{8})", fname)
    if not m:
        return None
    ds = m.group(1)
    return f"{ds[:4]}-{ds[4:6]}-{ds[6:8]}"

# ================== 로딩 유틸 ==================
def safe_read_xlsx(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    df.columns = df.columns.str.strip()
    return df

def collect_artifacts(artifacts_dir: Path, work_dir: Path) -> list[Path]:
    """
    artifacts_dir 아래에서
      - *.zip 이 있으면 모두 'work_dir'로 해제
      - (zip이 없거나, 이미 액션이 압축해제한) *.xlsx 는 그대로 복사
    최종적으로 work_dir 아래의 모든 xlsx 경로 리스트를 반환
    """
    work_dir.mkdir(parents=True, exist_ok=True)

    zips = list(artifacts_dir.rglob("*.zip"))
    if zips:
        for zp in zips:
            try:
                with zipfile.ZipFile(zp, "r") as zf:
                    zf.extractall(work_dir / zp.stem)
            except zipfile.BadZipFile:
                # 액션이 이미 자동 해제한 경우도 있으므로 무시
                pass

    # 이미 풀린 구조에서도 *.xlsx 수집
    xlsx_in_src = list(artifacts_dir.rglob("*.xlsx"))
    for p in xlsx_in_src:
        # work_dir 안으로 미러링 복사(출처 폴더명을 최대한 보존)
        rel = p.relative_to(artifacts_dir)
        dst = (work_dir / rel).resolve()
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not dst.exists():
            try:
                shutil.copy2(p.as_posix(), dst.as_posix())
            except Exception:
                pass

    # 최종 수집
    xlsxs = list(work_dir.rglob("*.xlsx"))
    return xlsxs

# ================== 집계 로직 ==================
def aggregate_national_monthly(xlsx_paths: list[Path]) -> dict[str, pd.Series]:
    out: dict[str, pd.Series] = {}
    for p in xlsx_paths:
        ym = yyyymm_from_filename(p.name)
        if not ym:
            continue
        df = safe_read_xlsx(p)
        if "광역" not in df.columns:
            continue
        s = df["광역"].value_counts().sort_index()
        out[ym] = s
    return out

def aggregate_seoul_by_gu(xlsx_paths: list[Path]) -> dict[str, pd.Series]:
    out: dict[str, pd.Series] = {}
    for p in xlsx_paths:
        if not p.name.startswith("서울시 "):
            continue
        df = safe_read_xlsx(p)
        if "구" not in df.columns:
            continue
        if {"계약년","계약월"}.issubset(df.columns):
            df["월"] = df["계약년"].astype(str).str.strip() + "-" + df["계약월"].astype(str).str.zfill(2)
        else:
            # 파일명 날짜 기반 fallback
            ds = yyyymmdd_from_filename(p.name) or "최근"
            df["월"] = ds[:7]
        for month, sub in df.groupby("월"):
            s = sub["구"].value_counts().sort_index()
            out[month] = out.get(month, pd.Series(dtype=int)).add(s, fill_value=0)
    return out

# ================== Sheets 유틸 ==================
def connect_gspread(sa_json_path: Path):
    import gspread
    from google.oauth2.service_account import Credentials
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(sa_json_path.as_posix(), scopes=scopes)
    return gspread.authorize(creds)

def ensure_worksheet(sh, title: str, rows=100, cols=30):
    try:
        return sh.worksheet(title)
    except Exception:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)

def overwrite_or_append_row(ws, date_key: str, header_entities: list[str], counts: pd.Series):
    # 헤더
    cur = ws.get_all_values() or []
    need = ["날짜"] + header_entities
    if not cur or cur[0] != need:
        ws.resize(1, len(need))
        ws.update("A1", [need])
        cur = [need]

    # 날짜 행 찾기
    date_col = [r[0] for r in cur[1:]] if len(cur) > 1 else []
    try:
        idx = date_col.index(date_key) + 2
        is_update = True
    except ValueError:
        idx = len(cur) + 1
        is_update = False

    row_vals = [date_key] + [int(counts.get(k, 0)) for k in header_entities]
    col_end = chr(ord('A') + len(row_vals) - 1)
    ws.update(f"A{idx}:{col_end}{idx}", [row_vals])
    return "update" if is_update else "append"

def write_reports(report_dir: Path, sheet_id: str, logs: list[str]):
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "sheet_id.txt").write_text(sheet_id, encoding="utf-8")
    (report_dir / "log.txt").write_text("\n".join(logs) if logs else "(no logs)", encoding="utf-8")

# ================== main ==================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts-dir", required=True)
    ap.add_argument("--sa", required=True, help="service account json path")
    ap.add_argument("--sheet-id", required=True)
    args = ap.parse_args()

    artifacts = Path(args.artifacts_dir)
    work = Path("extracted")
    if work.exists():
        shutil.rmtree(work)

    logs: list[str] = []
    try:
        # 1) 아티팩트 수집 (zip/직접 xlsx 모두 지원)
        xlsxs = collect_artifacts(artifacts, work)
        logs.append(f"[collect] found xlsx: {len(xlsxs)}")
        if not xlsxs:
            logs.append("[collect] no xlsx found under artifacts")
            write_reports(Path("analyze_report"), args.sheet_id, logs)
            print("\n".join(logs))
            return

        # 2) 파일 분류
        national_files = [p for p in xlsxs if p.name.startswith("전국 ")]
        seoul_files    = [p for p in xlsxs if p.name.startswith("서울시 ")]
        logs.append(f"[collect] national: {len(national_files)}, seoul: {len(seoul_files)}")

        # 3) 집계
        nat = aggregate_national_monthly(national_files)
        sgg = aggregate_seoul_by_gu(seoul_files)

        # 4) Sheets 업데이트
        gc = connect_gspread(Path(args.sa))
        sh = gc.open_by_key(args.sheet_id)

        # 전국 월별 요약
        ws_nat = ensure_worksheet(sh, "전국 월별 요약", rows=300, cols=80)
        all_regions = sorted(set().union(*[set(s.index) for s in nat.values()]) if nat else [])
        for ym, series in sorted(nat.items()):
            op = overwrite_or_append_row(ws_nat, ym, all_regions, series)
            logs.append(f"[전국] {ym} {op}: {int(series.sum())}건")

        # 서울 월별 구합계
        ws_seoul = ensure_worksheet(sh, "서울 월별 구합계", rows=300, cols=80)
        all_gus = sorted(set().union(*[set(s.index) for s in sgg.values()]) if sgg else [])
        for ym, series in sorted(sgg.items()):
            op = overwrite_or_append_row(ws_seoul, ym, all_gus, series)
            logs.append(f"[서울] {ym} {op}: {int(series.sum())}건")

    except Exception as e:
        logs.append(f"[ERROR] {type(e).__name__}: {e}")
        raise
    finally:
        write_reports(Path("analyze_report"), args.sheet_id, logs)
        print("\n".join(logs))

if __name__ == "__main__":
    main()
