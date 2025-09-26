from __future__ import annotations
import argparse, re, shutil
from pathlib import Path
from datetime import datetime, date, timedelta
import pandas as pd

# ========== 파일명 → (기준연월, 기록일) 파서 ==========
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

# ========== 엑셀 로드 ==========
def read_xlsx(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    df.columns = df.columns.str.strip()
    return df

# ========== gspread 유틸 ==========
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
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=5000, cols=max(26, len(header)))
        ws.update("A1", [header])
        return ws

    # 헤더 동기화(필요 시 확장/정렬)
    cur = ws.get_all_values()
    if not cur:
        ws.update("A1", [header])
        return ws

    cur_header = cur[0]
    # 헤더를 '날짜 + (기존/새 항목의 유니온)'으로 재구성
    desired = ["날짜"] + sorted(set(cur_header[1:] + header[1:]))
    if cur_header != desired:
        ws.resize(1, len(desired))
        ws.update("A1", [desired])
    return ws

def parse_date_str(s: str) -> date | None:
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d", "%y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    # 'YYYY.MM.DD' 등도 대비
    try:
        sp = re.sub(r"[^\d]", "-", s)
        return datetime.strptime(sp, "%Y-%m-%d").date()
    except Exception:
        return None

def find_date_row_index(ws, d: date) -> int | None:
    rows = ws.get_all_values()
    if len(rows) <= 1:  # header only
        return None
    want = d.isoformat()
    for i, r in enumerate(rows[1:], start=2):
        if (r and parse_date_str(r[0]) == d):
            return i
        if r and r[0] == want:
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
    # 마지막 실제 데이터 행(공백 제외) 찾기
    for r in reversed(rows[1:]):
        if any(c.strip() for c in r):
            # 숫자만 뽑아서 비교 쉽게
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
    # 헤더 동기화
    header = ws.get_all_values()[0]
    desired = ["날짜"] + sorted(set(header[1:] + header_entities))
    if header != desired:
        ws.resize(1, len(desired))
        ws.update("A1", [desired])
        header = desired

    # 행 데이터 구성
    row_vals = [when.isoformat()]
    for key in header[1:]:
        row_vals.append(int(counts.get(key, 0)))

    if mode == "force":
        # 있으면 업데이트, 없으면 append
        idx = find_date_row_index(ws, when)
        if idx:
            col_end = chr(ord('A') + len(row_vals) - 1)
            ws.update(f"A{idx}:{col_end}{idx}", [row_vals])
            return "update"
        else:
            ws.append_row(row_vals, value_input_option="RAW")
            return "append"

    # smart 모드
    frst = first_record_date(ws)
    if frst and when <= frst + timedelta(days=92):  # 최초 기록일+약 3개월
        idx = find_date_row_index(ws, when)
        if idx:
            col_end = chr(ord('A') + len(row_vals) - 1)
            ws.update(f"A{idx}:{col_end}{idx}", [row_vals])
            return "update(<=3mo)"
        else:
            ws.append_row(row_vals, value_input_option="RAW")
            return "append(<=3mo)"
    else:
        # 3개월 이후: 마지막 행과 동일하면 skip
        last = last_row_values(ws)
        if last:
            hdr, last_vals = last
            # 현재 헤더 순서대로 현재값 벡터 만들기
            cur_vals = []
            for key in hdr[1:]:
                cur_vals.append(int(counts.get(key, 0)))
            if cur_vals == last_vals:
                return "skip(same as last)"
        # 다르면 기록 (존재 시 업데이트, 없으면 append)
        idx = find_date_row_index(ws, when)
        if idx:
            col_end = chr(ord('A') + len(row_vals) - 1)
            ws.update(f"A{idx}:{col_end}{idx}", [row_vals])
            return "update(>3mo)"
        else:
            ws.append_row(row_vals, value_input_option="RAW")
            return "append(>3mo)"

# ========== 집계(전국파일 1개 → 전국/서울 두 탭) ==========
def aggregate_from_national_file(df: pd.DataFrame):
    # 전국: 광역별 건수
    nat_series = df["광역"].value_counts().sort_index() if "광역" in df.columns else pd.Series(dtype=int)

    # 서울: 광역=서울특별시만 필터 → 구별 건수
    seoul_series = pd.Series(dtype=int)
    if {"광역","구"}.issubset(df.columns):
        seoul_df = df[df["광역"] == "서울특별시"].copy()
        if not seoul_df.empty:
            seoul_series = seoul_df["구"].value_counts().sort_index()
    return nat_series, seoul_series

# ========== 메인 ==========
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts-dir", required=True)  # artifacts/outputs-<runid>/output/*.xlsx
    ap.add_argument("--sa", required=True)             # sa.json
    ap.add_argument("--sheet-id", required=True)
    args = ap.parse_args()

    artifacts = Path(args.artifacts_dir)
    xlsxs = [p for p in artifacts.rglob("*.xlsx") if p.name.startswith("전국 ")]
    logs: list[str] = []

    if not xlsxs:
        print("[collect] no '전국 *.xlsx' files found under artifacts")
        return

    # 구글 시트 연결
    gc = connect_gspread(Path(args.sa))
    sh = gc.open_by_key(args.sheet_id)

    for p in sorted(xlsxs, key=lambda x: x.name):
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

        # 전국 탭 처리
        if not nat_series.empty:
            nat_header = ["날짜"] + sorted(nat_series.index.tolist())
            ws_nat = ensure_ws(sh, nat_title, nat_header)
            op = write_row(ws_nat, write_day, nat_series.index.tolist(), nat_series, mode="smart")
            logs.append(f"[전국] {p.name} → {nat_title} @ {write_day}: {op}, sum={int(nat_series.sum())}")
        else:
            logs.append(f"[전국] {p.name} → no data")

        # 서울 탭 처리
        if not seoul_series.empty:
            seoul_header = ["날짜"] + sorted(seoul_series.index.tolist())
            ws_se = ensure_ws(sh, seoul_title, seoul_header)
            op2 = write_row(ws_se, write_day, seoul_series.index.tolist(), seoul_series, mode="smart")
            logs.append(f"[서울] {p.name} → {seoul_title} @ {write_day}: {op2}, sum={int(seoul_series.sum())}")
        else:
            logs.append(f"[서울] {p.name} → no seoul rows")

    # 간단 로그 출력(필요 시 analyze_report에 남기는 건 기존 워크플로우에서 처리)
    print("\n".join(logs))

if __name__ == "__main__":
    main()
