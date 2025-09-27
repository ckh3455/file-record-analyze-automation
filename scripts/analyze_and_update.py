# scripts/analyze_and_update.py
# -*- coding: utf-8 -*-

import os, re, json, time, math
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ===================== 설정 =====================
LOG_DIR = Path("analyze_report")
ART_DIR_DEFAULT = "artifacts"

# Sheets 탭 이름과 컬럼 매핑(요약 탭)
SUMMARY_SHEET_NAME = "거래요약"

# 요약 컬럼(왼쪽→오른쪽)
SUMMARY_COLS = [
    "전국","서울","강남구","압구정동","경기도","인천광역시","세종시","서초구","송파구","용산구",
    "강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구","강서구",
    "강북구","관악구","구로구","금천구","도봉구","동대문구","서대문구","성북구","은평구",
    "중구","중랑구","부산","대구","광주","대전","강원도","경남","경북","전남","전북","충남","충북","제주"
]

# 광역 표준화(요약 열 이름에 맞추기)
PROV_MAP = {
    "서울특별시": "서울",
    "세종특별자치시": "세종시",
    "강원특별자치도": "강원도",
    "경기도": "경기도",
    "인천광역시": "인천광역시",
    "부산광역시": "부산",
    "대구광역시": "대구",
    "광주광역시": "광주",
    "대전광역시": "대전",
    "울산광역시": "울산",
    "전라남도": "전남",
    "전북특별자치도": "전북",
    "경상남도": "경남",
    "경상북도": "경북",
    "충청남도": "충남",
    "충청북도": "충북",
    "제주특별자치도": "제주",
}

# 압구정동 탭 설정
APGU_SHEET = "압구정동"   # 탭 이름
APGU_CHANGE_HEADER = ["변경구분", "기록일", "계약년", "계약월", "계약일", "단지명", "전용면적(㎡)", "동", "층", "거래금액(만원)"]

# ===================== 로깅 =====================
def fmt_now():
    return datetime.now().strftime("%H:%M:%S")

def fmt_date_kor(dt: datetime) -> str:
    # 시트에서 쓰는 "YYYY. M. D" 포맷
    y = dt.year
    m = dt.month
    d = dt.day
    return f"{y}. {m}. {d}"

def log(msg: str):
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        # 파일로 존재한다면 지우고 폴더로 재생성
        if LOG_DIR.exists() and LOG_DIR.is_file():
            LOG_DIR.unlink()
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    line = f"[{fmt_now()}] {msg}\n"
    (LOG_DIR / "latest.log").write_text((LOG_DIR / "latest.log").read_text() + line if (LOG_DIR / "latest.log").exists() else line, encoding="utf-8")
    print(line, end="")

def log_block(title: str):
    log(f"[{title.upper()}]")

# 재시도 래퍼
def _retry(fn, *a, **kw):
    tries = 4
    last = None
    for i in range(tries):
        try:
            return fn(*a, **kw)
        except Exception as e:
            last = e
            if i < tries-1:
                time.sleep(1.2*(i+1))
            else:
                raise
    raise last

# gspread 버전별 배치 업데이트 호환
def _values_batch_update_compat(ws, body: dict):
    try:
        # gspread 6.x: Spreadsheet.values_batch_update(body)
        return _retry(ws.spreadsheet.values_batch_update, body)
    except TypeError:
        # gspread 5.x: Client.values_batch_update(spreadsheet_id, body)
        return _retry(ws.client.values_batch_update, ws.spreadsheet.id, body)

def batch_values_update(ws, payload):
    body = {
        "valueInputOption": "USER_ENTERED",
        "data": [{"range": p["range"], "values": p["values"]} for p in payload],
    }
    _values_batch_update_compat(ws, body)

# ===================== Google Sheets 유틸 =====================
A1_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
def a1_col(n):
    s = ""
    while n > 0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return default

def parse_file_meta(p: Path):
    # 파일명 예: "전국 2507_250927.xlsx"
    name = p.stem
    m = re.match(r"전국\s+(\d{2})(\d{2})_\d{6}$", name)
    if not m:
        # 2410_250926.xlsx 처럼 앞이 월 의미일 때도 처리
        m2 = re.match(r"전국\s+(\d{2})(\d{2})_\d{6}$", name)
        if not m2:
            return None
    yy = int(name.split()[1][:2])
    mm = int(name.split()[1][2:4])
    year = 2000 + (yy if yy <= 99 else 0)
    return year, mm

def fuzzy_ws(sh: gspread.Spreadsheet, wanted: str) -> Optional[gspread.Worksheet]:
    # 공백 제거, '년 ' vs '년' 같은 타입 보정
    norm = re.sub(r"\s+", "", wanted)
    for ws in sh.worksheets():
        n = re.sub(r"\s+", "", ws.title)
        if n == norm:
            return ws
    # 부분 일치 (시트에 '전국 25년 6월' / '전국 25년6월' 등)
    for ws in sh.worksheets():
        n = re.sub(r"\s+", "", ws.title)
        if n == norm.replace(" ", ""):
            return ws
    return None

def ensure_ws_exists(sh: gspread.Spreadsheet, title: str) -> Optional[gspread.Worksheet]:
    ws = fuzzy_ws(sh, title)
    if not ws:
        log(f"[ws] not found: '{title}' (skip)")
        return None
    return ws

def detect_date_col(ws: gspread.Worksheet, header_row: int = 1) -> int:
    # '날짜' 텍스트가 있는 열 찾기 (없으면 1열 가정)
    header = _retry(ws.row_values, header_row)
    for i, v in enumerate(header, start=1):
        if str(v).strip() == "날짜":
            return i
    return 1

def normalize_sheet_date_cell(v: str):
    # 셀에 들어있는 값들을 문자열로 받아 yyyy-mm-dd 추출
    s = str(v).strip()
    if not s:
        return None
    # "2025. 9. 26" → yyyy-mm-dd 로 비교
    m = re.match(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})", s)
    if m:
        y, mo, d = map(int, m.groups())
        return f"{y:04d}-{mo:02d}-{d:02d}"
    m2 = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m2:
        return s
    return None

def find_row_by_date(ws: gspread.Worksheet, date_col: int, target: datetime) -> Optional[int]:
    # 시트 안의 날짜 문자열을 모두 읽고 yyyy-mm-dd로 변환해 비교
    col_letter = a1_col(date_col)
    rng = f"{col_letter}2:{col_letter}"  # 헤더 제외
    vals = _retry(ws.col_values, date_col)
    # col_values 는 전체 컬럼을 반환하므로 2행부터 본다
    target_key = f"{target.year:04d}-{target.month:02d}-{target.day:02d}"
    for idx, cell in enumerate(vals[1:], start=2):
        key = normalize_sheet_date_cell(cell)
        if key == target_key:
            return idx
    return None

def write_date_cell(ws: gspread.Worksheet, row_idx: int, date_obj: datetime):
    label = fmt_date_kor(date_obj)
    _retry(ws.update, [[label]], f"A{row_idx}:A{row_idx}", value_input_option="USER_ENTERED")

def format_row_bold(ws: gspread.Worksheet, row_idx: int, first_col: int, last_col: int):
    body = {
        "requests": [{
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": row_idx-1,
                    "endRowIndex": row_idx,
                    "startColumnIndex": first_col-1,
                    "endColumnIndex": last_col
                },
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold"
            }
        }]
    }
    _retry(ws.spreadsheet.batch_update, body)

def color_cell(ws: gspread.Worksheet, row: int, col: int, rgb: tuple):
    r,g,b = rgb
    body = {
        "requests": [{
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": row-1,
                    "endRowIndex": row,
                    "startColumnIndex": col-1,
                    "endColumnIndex": col
                },
                "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {
                    "red": r, "green": g, "blue": b
                }}}},
                "fields": "userEnteredFormat.textFormat.foregroundColor"
            }
        }]
    }
    _retry(ws.spreadsheet.batch_update, body)

# ===================== 파일 읽기/집계 =====================
NEEDED_COLS = ["광역","구","법정동","도로명","번지","본번","부번","단지명","전용면적(㎡)","계약년","계약월","계약일","거래금액(만원)","동","층"]
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data")
    # 중복 헤더 드랍
    df = df.loc[:,~df.columns.duplicated()].copy()
    # 필요한 컬럼 보정
    for c in NEEDED_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    return df.copy()

def eok_series(ser):
    try:
        s = pd.to_numeric(ser, errors="coerce")
    except Exception:
        return pd.Series([], dtype=float)
    s = s.dropna()
    if s.empty: return pd.Series([], dtype=float)
    return s / 10000.0

def round2(v):
    try:
        return f"{float(v):.2f}"
    except Exception:
        return ""

def agg_all_stats(df: pd.DataFrame):
    counts = {col:0 for col in SUMMARY_COLS}
    med = {col:"" for col in SUMMARY_COLS}
    mean = {col:"" for col in SUMMARY_COLS}

    # 전국 전체
    all_eok = eok_series(df["거래금액(만원)"])
    counts["전국"] = int(len(df))
    if not all_eok.empty:
        med["전국"] = round2(all_eok.median())
        mean["전국"] = round2(all_eok.mean())

    # 광역
    if "광역" in df.columns:
        for prov, sub in df.groupby("광역"):
            prov_std = PROV_MAP.get(str(prov), str(prov))
            if prov_std in counts:
                counts[prov_std] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[prov_std] = round2(s.median())
                    mean[prov_std] = round2(s.mean())

    # 서울/자치구
    seoul = df[df.get("광역","")=="서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul)>0:
        s = eok_series(seoul["거래금액(만원)"])
        if not s.empty:
            med["서울"] = round2(s.median())
            mean["서울"] = round2(s.mean())

    if "구" in seoul.columns:
        for gu, sub in seoul.groupby("구"):
            gu = str(gu)
            if gu in counts:
                counts[gu] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[gu] = round2(s.median())
                    mean[gu] = round2(s.mean())

    # 압구정동
    ap = seoul[seoul.get("법정동","")=="압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap)>0:
        s = eok_series(ap["거래금액(만원)"])
        med["압구정동"] = round2(s.median())
        mean["압구정동"] = round2(s.mean())

    return counts, med, mean

# ===================== 전국/서울 탭 기록 =====================
def write_counts_to_sheet(ws: gspread.Worksheet, write_date: datetime, header_names: list[str], series_map: dict, sum_col_name: str = "총합계"):
    """해당 날짜 행에 series_map 값을 넣고, sum_col_name 에 총합계 기록."""
    date_col = detect_date_col(ws)
    row = find_row_by_date(ws, date_col, write_date)
    # 없으면 append
    if row is None:
        vals = _retry(ws.get_all_values) or []
        row = len(vals) + 1
        write_date_cell(ws, row, write_date)

    # 헤더 인덱스 맵
    header = _retry(ws.row_values, 1)
    hmap = {h: i+1 for i, h in enumerate(header)}

    # 값 payload
    payload = []
    total = 0
    for name in header_names:
        if name in hmap:
            col = hmap[name]
            v = int(series_map.get(name, 0) or 0)
            payload.append({"range": f"{a1_col(col)}{row}", "values": [[v]]})
            total += v

    # 총합계
    if sum_col_name in hmap:
        payload.append({"range": f"{a1_col(hmap[sum_col_name])}{row}", "values": [[total]]})

    if payload:
        batch_values_update(ws, payload)

# ===================== 거래요약 탭 =====================
def find_summary_row(ws: gspread.Worksheet, ym_label: str, label: str) -> int:
    # A열에 년월, B열에 라벨(거래건수, 중앙값(단위:억), 평균가(단위:억), 전월대비 건수증감, 예상건수)
    vals = _retry(ws.get_all_values) or []
    # 없으면 헤더만 추가
    if not vals:
        _retry(ws.update, [["년월","구분"] + SUMMARY_COLS], "A1")
        vals = _retry(ws.get_all_values) or []
    # 찾기
    for i, r in enumerate(vals[1:], start=2):
        ym = (r[0] if len(r)>0 else "").strip()
        lb = (r[1] if len(r)>1 else "").strip()
        if ym == ym_label and lb == label:
            return i
    # 없으면 맨 아래 추가
    return len(vals) + 1

def put_row(ws: gspread.Worksheet, row_idx: int, label: str, line_map: dict):
    # 헤더
    header = _retry(ws.row_values, 1)
    hmap = {h: i+1 for i, h in enumerate(header)}
    # B열 라벨
    _retry(ws.update, [[label]], f"B{row_idx}:B{row_idx}", value_input_option="USER_ENTERED")
    # 각 열 값
    payload = []
    for col_name in SUMMARY_COLS:
        if col_name in hmap:
            col = hmap[col_name]
            v = line_map.get(col_name, "")
            payload.append({"range": f"{a1_col(col)}{row_idx}", "values": [[v]]})
    if payload:
        batch_values_update(ws, payload)

def write_month_summary(ws: gspread.Worksheet, y: int, m: int, counts: dict, med: dict, mean: dict, prev_counts: Optional[dict]):
    ym_label = f"{str(y%100).zfill(2)}/{m}"

    # A열 년월 채워두기
    def ensure_row(what: str) -> int:
        r = find_summary_row(ws, ym_label, what)
        _retry(ws.update, [[ym_label]], f"A{r}:A{r}", value_input_option="USER_ENTERED")
        return r

    # 거래건수 (볼드)
    r1 = ensure_row("거래건수")
    put_row(ws, r1, "거래건수", counts)
    # 행 볼드 처리
    header = _retry(ws.row_values, 1)
    first_c = 3  # C부터 데이터
    last_c = len(header)
    format_row_bold(ws, r1, first_c, last_c)
    log(f"[summary] {ym_label} 거래건수 -> row={r1}")

    # 중앙값(단위:억) – 소수점 2자리
    r2 = ensure_row("중앙값(단위:억)")
    put_row(ws, r2, "중앙값(단위:억)", med)
    log(f"[summary] {ym_label} 중앙값 -> row={r2}")

    # 평균가(단위:억)
    r3 = ensure_row("평균가(단위:억)")
    put_row(ws, r3, "평균가(단위:억)", mean)
    log(f"[summary] {ym_label} 평균가 -> row={r3}")

    # 전월대비 건수증감 (+파랑 / −빨강)
    r4 = ensure_row("전월대비 건수증감")
    diff_line = {}
    color_jobs = []
    if prev_counts:
        for k in SUMMARY_COLS:
            cur = int(counts.get(k, 0) or 0)
            prv = int(prev_counts.get(k, 0) or 0)
            delta = cur - prv
            if delta > 0:
                diff_line[k] = f"+{delta}"
            elif delta < 0:
                diff_line[k] = f"{delta}"
            else:
                diff_line[k] = "0"
    else:
        for k in SUMMARY_COLS:
            diff_line[k] = ""
    put_row(ws, r4, "전월대비 건수증감", diff_line)
    # 색칠(숫자 있는 곳만)
    header_map = {h: i+1 for i,h in enumerate(header)}
    for k,v in diff_line.items():
        if k in header_map and v not in ("", "0"):
            col = header_map[k]
            if v.startswith("+"):
                color_cell(ws, r4, col, (0.0, 0.35, 1.0))  # 파랑
            elif v.startswith("-"):
                color_cell(ws, r4, col, (1.0, 0.0, 0.0))  # 빨강
    log(f"[summary] {ym_label} 전월대비 -> row={r4}")

    # 예상건수는 공란 (요청 보류)
    r5 = ensure_row("예상건수")
    log(f"[summary] {ym_label} 예상건수 -> row={r5}")

# ===================== 압구정동 탭 (원본 누적 + 변경 로그) =====================
def ensure_apgu_sheet(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, APGU_SHEET)
    if ws:
        return ws
    return _retry(sh.add_worksheet, title=APGU_SHEET, rows=2000, cols=60)

def make_row_key(d: dict) -> str:
    parts = [
        d.get("계약년",""), d.get("계약월",""), d.get("계약일",""),
        d.get("광역",""), d.get("구",""), d.get("법정동",""),
        d.get("단지명",""), d.get("전용면적(㎡)",""), d.get("층",""),
        d.get("거래금액(만원)","")
    ]
    return "|".join(str(x).strip() for x in parts)

def number_or_blank(v):
    if v is None: return ""
    if isinstance(v, float) and (pd.isna(v) or math.isnan(v)): return ""
    return v

def append_change_log(ws: gspread.Worksheet, added_rows: list[list], removed_rows: list[list], header: list[str]):
    """압구정동 탭 맨 아래에 빨간색으로 변경로그를 남긴다. 시트 크기 부족 시 확대."""
    all_vals = _retry(ws.get_all_values) or []
    start = len(all_vals) + 1

    # 변경 로그 헤더 보장
    rows = []
    now_label = fmt_date_kor(datetime.now())
    def to_log_row(kind, row):
        idx = {h:i for i,h in enumerate(header)}
        def take(col): 
            return row[idx[col]] if col in idx and idx[col] < len(row) else ""
        return [
            kind, now_label,
            take("계약년"), take("계약월"), take("계약일"), take("단지명"),
            take("전용면적(㎡)"), take("동"), take("층"), take("거래금액(만원)")
        ]

    for r in added_rows:
        rows.append(to_log_row("(신규)", r))
    for r in removed_rows:
        rows.append(to_log_row("(삭제)", r))

    if not rows:
        return

    # 시트 크기 확보
    needed_rows = start + len(rows)
    grid_rows = ws.row_count
    grid_cols = ws.col_count
    if needed_rows > grid_rows:
        _retry(ws.add_rows, needed_rows - grid_rows)

    end = start + len(rows) - 1
    rng = f"A{start}:{a1_col(len(APGU_CHANGE_HEADER))}{end}"
    # 헤더 존재 검사
    vals0 = all_vals
    if not vals0 or (vals0 and vals0[0] != APGU_CHANGE_HEADER):
        # 최상단 교체하지 않고, 단순히 맨 아래에 로그 테이블을 붙이는 형태
        pass
    _retry(ws.update, [APGU_CHANGE_HEADER], f"A{start}:J{start}")
    _retry(ws.update, rows, f"A{start+1}:J{end}")

    # 빨간 글씨 적용
    body = {
        "requests": [{
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": start-1,
                    "endRowIndex": end,
                    "startColumnIndex": 0,
                    "endColumnIndex": 10
                },
                "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red":1.0,"green":0.0,"blue":0.0}}}},
                "fields": "userEnteredFormat.textFormat.foregroundColor"
            }
        }]
    }
    _retry(ws.spreadsheet.batch_update, body)

def upsert_apgu_raw(ws: gspread.Worksheet, df_all: pd.DataFrame):
    # 압구정동만 선별
    cond = (df_all.get("광역","")=="서울특별시") & (df_all.get("법정동","")=="압구정동")
    df = df_all[cond].copy()
    log(f"[압구정동] filtered {len(df)} rows")
    if df.empty:
        log("[압구정동] no rows in files")
        return

    # 계약년/월/일 결손 보정 및 정렬
    for c in ["계약년","계약월","계약일"]:
        if c not in df.columns:
            df[c] = pd.NA
    df = df.sort_values(["계약년","계약월","계약일"], ascending=[True,True,True], kind="mergesort")

    # 시트 현재 값
    vals = _retry(ws.get_all_values) or []
    if not vals:
        header = list(df.columns) + ["기록일"]
        _retry(ws.update, [header], "A1")
        vals = [header]
    else:
        header = vals[0]
        # 헤더에 "기록일" 없으면 추가
        if "기록일" not in header:
            header = header + ["기록일"]
            _retry(ws.update, [header], "A1")
            vals = _retry(ws.get_all_values) or [header]

        # 파일에 있고 시트에 없는 컬럼은 헤더 뒤에 추가
        union = list(dict.fromkeys(header + [c for c in df.columns if c not in header]))
        if union != header:
            header = union
            _retry(ws.update, [header], "A1")
            vals = _retry(ws.get_all_values) or [header]

    # 기존 rows -> 키셋
    existing_rows = vals[1:]
    idx_map = {h:i for i,h in enumerate(header)}
    def row_to_dict(row):
        return {k:(row[i] if i<len(row) else "") for k,i in idx_map.items()}

    existing_keys = set()
    for r in existing_rows:
        existing_keys.add(make_row_key(row_to_dict(r)))

    # 신규 레코드
    new_records = []
    today_label = fmt_date_kor(datetime.now())
    for _, r in df.iterrows():
        d = {k: r.get(k, "") for k in header if k in df.columns}
        k = make_row_key(d)
        if k in existing_keys:
            continue
        rec = [number_or_blank(r.get(col, "")) for col in header if col != "기록일"]
        rec.append(today_label)
        new_records.append(rec)
        existing_keys.add(k)

    # 삭제된 레코드 탐지(시트에는 있는데, 이번 파일들에는 없는 키)
    file_keys = set()
    for _, r in df.iterrows():
        d = {k: r.get(k, "") for k in header if k in df.columns}
        file_keys.add(make_row_key(d))

    removed = []
    for r in existing_rows:
        d = row_to_dict(r)
        k = make_row_key(d)
        if k not in file_keys:
            removed.append([d.get(h,"") for h in header])

    # append
    if new_records:
        start_row = len(vals)+1
        end_row = start_row + len(new_records) - 1
        rng = f"A{start_row}:{a1_col(len(header))}{end_row}"
        _retry(ws.update, new_records, rng)
        log(f"[압구정동] appended {len(new_records)} rows")
    else:
        log("[압구정동] no new rows to append")

    # 변경 로그
    append_change_log(ws, new_records, removed, header)

# ===================== 메인 =====================
def main():
    log_block("main")

    artifacts_dir = os.environ.get("ARTIFACTS_DIR", ART_DIR_DEFAULT)
    sheet_id = os.environ.get("SHEET_ID", "").strip()
    sa_json_env = os.environ.get("SA_JSON", "").strip()
    sa_path = os.environ.get("SA_PATH", "sa.json")

    log(f"artifacts_dir={artifacts_dir}")

    # gspread 인증
    if sa_json_env:
        creds = Credentials.from_service_account_info(
            json.loads(sa_json_env),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
    else:
        if not Path(sa_path).exists():
            raise RuntimeError("service account not provided")
        creds = Credentials.from_service_account_file(
            sa_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, sheet_id)
    log("[gspread] spreadsheet opened")

    # 파일 수집
    xl = sorted(Path(artifacts_dir).rglob("*.xlsx"))
    log(f"[collect] found {len(xl)} xlsx files")

    # 월별 집계 캐시
    month_cache = {}  # key: (y,m) -> {"counts":..., "med":..., "mean":...}
    apgu_all = []     # 압구정동 전체 레코드

    # 오늘 날짜(시트 포맷으로)
    write_date = datetime.now()

    # 전국/서울 탭 기록
    for f in xl:
        meta = parse_file_meta(f)
        if not meta:
            continue
        year, mm = meta
        log(f"[read] {f.name}")
        df = read_month_df(f)

        # 집계
        counts, med, mean = agg_all_stats(df)
        month_cache[(year, mm)] = {"counts":counts, "med":med, "mean":mean}

        # 전국 탭 / 서울 탭 이름 만들기 (시트 실제 탭과 공백/띄어쓰기 무관하게 대응)
        nat_title = f"전국 {year%100:02d}년 {mm}월"
        se_title  = f"서울 {year%100:02d}년 {mm}월"

        ws_nat = ensure_ws_exists(sh, nat_title)
        ws_se  = ensure_ws_exists(sh, se_title)

        # 전국/서울 표로 기록
        # 전국(광역별 합계) — 해당 시트 헤더 기준
        if ws_nat:
            header_nat = _retry(ws_nat.row_values, 1)
            # 전국 탭: 전국/각 광역
            nat_cols = [c for c in header_nat if c in PROV_MAP.values() or c=="전국"]
            write_counts_to_sheet(ws_nat, write_date, nat_cols, counts, sum_col_name="총합계")
            log(f"[전국] {ws_nat.title} -> {fmt_date_kor(write_date)}")

        # 서울(구별 합계)
        if ws_se:
            header_se = _retry(ws_se.row_values, 1)
            se_cols = [c for c in header_se if c in ["서울"] or c in SUMMARY_COLS]  # 구들이 SUMMARY_COLS에 포함됨
            write_counts_to_sheet(ws_se, write_date, se_cols, counts, sum_col_name="총합계")
            log(f"[서울] {ws_se.title} -> {fmt_date_kor(write_date)}")

        # 압구정동 누적 재사용
        ap = df[(df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")].copy()
        if not ap.empty:
            apgu_all.append(ap)

    # 거래요약 탭
    ws_sum = ensure_ws_exists(sh, SUMMARY_SHEET_NAME)
    if ws_sum:
        # 헤더 보장
        vals = _retry(ws_sum.get_all_values) or []
        if not vals:
            _retry(ws_sum.update, [["년월","구분"] + SUMMARY_COLS], "A1")
        # 갱신(모든 월)
        for (y,m), cur in sorted(month_cache.items()):
            # 전월 값 찾기
            prev = month_cache.get((y, m-1)) if m>1 else month_cache.get((y-1, 12))
            prev_counts = (prev["counts"] if prev else None)
            write_month_summary(ws_sum, y, m, cur["counts"], cur["med"], cur["mean"], prev_counts)

    # 압구정동 탭(원본 누적 + 변경 로그)
    if apgu_all:
        ws_ap = ensure_apgu_sheet(sh)
        all_df = pd.concat(apgu_all, ignore_index=True)
        upsert_apgu_raw(ws_ap, all_df)

    log("[main] done")

if __name__ == "__main__":
    main()
