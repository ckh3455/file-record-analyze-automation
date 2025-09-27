#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyze_and_update.py

- 아티팩트 폴더의 '전국 YYYYMM_YYMMDD.xlsx' 파일들 읽기(sheet='data')
- 월별 집계(거래건수, 중앙값, 평균가) 및 서울/전국/광역/구 단위 집계
- 기존 시트의 해당 탭(예: '전국 25년 8월', '서울 25년 8월')에서 날짜가 있으면 그 행에 업데이트,
  없으면 하단에 날짜 추가 후 기록
- '총합계' 열에 당일 지역별 거래건수 합계를 기록(그 행에서)
- '거래요약' 탭에 년월별: 거래건수 / 중앙값(억, 소수점 2자리) / 평균가(억, 소수점 2자리) / 전월대비(+파랑, -빨강) 기록
- '압구정동' 탭: 원본 행을 그대로 누적(오래된→최신 정렬), 중복 제거, 기록일 추가
  + 기존과 비교해 신규/삭제 변동을 시트 하단에 빨간 글자로 기록

필수 환경변수:
- SHEET_ID : 대상 구글스프레드시트 ID
선택 환경변수:
- SA_JSON  : 서비스계정 JSON 문자열(권장). 없으면 로컬 인증(gspread.oauth) 사용 시도.
- ARTIFACTS_DIR : 아티팩트 루트(기본 'artifacts')

사용:
  python scripts/analyze_and_update.py --artifacts-dir artifacts --sheet-id "$SHEET_ID"
"""

from __future__ import annotations
import os, sys, re, json, time
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional

import pandas as pd
import numpy as np

# gspread
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# -------------------- 설정/상수 --------------------

LOG_DIR = Path("analyze_report")
LOG_DIR.mkdir(parents=True, exist_ok=True)  # 디렉터리 보장

# 요약 탭 이름
SUMMARY_SHEET_TITLE = "거래요약"

# 요약 열(표의 헤더) – 사용자가 요청한 모든 열 포함
SUMMARY_COLS: List[str] = [
    "전국","서울","강남구","압구정동","경기도","인천광역시","세종시","서초구","송파구","용산구",
    "강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구","강서구","강북구",
    "관악구","구로구","금천구","도봉구","동대문구","서대문구","성북구","은평구","중구","중랑구",
    "부산","대구","광주","대전","강원도","경남","경북","전남","전북","충남","충북","제주"
]

# 광역 -> 거래요약 열 라벨 매핑
PROV_TO_SUMMARY_COL = {
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
    "경상남도": "경남",
    "경상북도": "경북",
    "전라남도": "전남",
    "전북특별자치도": "전북",
    "충청남도": "충남",
    "충청북도": "충북",
    "제주특별자치도": "제주",
}

# 서울 구 목록(요약 열에 있는 것만)
SEOUL_GUS = [
    "강남구","강동구","강북구","강서구","관악구","광진구","구로구","금천구","노원구","도봉구",
    "동대문구","동작구","마포구","서대문구","서초구","성동구","성북구","송파구","양천구",
    "영등포구","용산구","은평구","종로구","중구","중랑구"
]

# 압구정동 탭 구성 관련
APGU_SHEET_TITLE = "압구정동"
APGU_KEY_FIELDS = [
    "계약년","계약월","계약일",
    "광역","구","법정동",
    "단지명","전용면적(㎡)","층","거래금액(만원)"
]

# 필요 컬럼 (원본이 없으면 생성해서 빈값으로 유지)
NEEDED_COLS = ["광역","구","법정동","거래금액(만원)","계약년","계약월","계약일"]

# -------------------- 로깅 --------------------

def _now_str():
    return datetime.now().strftime("%H:%M:%S")

def log(msg: str):
    line = f"[{_now_str()}] {msg}"
    print(line)
    # latest.log(덮어쓰기) + 개별 run-*.log(추가)
    try:
        (LOG_DIR / "latest.log").write_text(line + "\n", encoding="utf-8")
    except Exception:
        pass
    try:
        with open(LOG_DIR / f"run-{datetime.now().strftime('%Y%m%dT%H%M%S%z')}.log", "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def log_block(title: str):
    log(f"[{title.upper()}]")

# -------------------- 도우미 --------------------

def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def fmt_date_kor(dt: datetime) -> str:
    # 구글시트 표시형식 "yyyy. m. d" 에 맞게 작성(문자열로)
    return f"{dt.year}. {dt.month}. {dt.day}"

def ym_label(year: int, month: int) -> str:
    return f"{str(year%100).zfill(2)}/{month}"

def norm_name(s: str) -> str:
    if not s: return ""
    return re.sub(r"\s+", "", str(s))

def _retry(fn, *args, **kwargs):
    # 간단 재시도(쿼터/간헐 오류용)
    tries = kwargs.pop("_tries", 5)
    delay = kwargs.pop("_delay", 1.2)
    for i in range(tries):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            if i == tries-1:
                raise
            time.sleep(delay)
        except Exception:
            if i == tries-1:
                raise
            time.sleep(delay)

def fuzzy_ws(spreadsheet, title: str):
    want = norm_name(title)
    for ws in spreadsheet.worksheets():
        if norm_name(ws.title) == want:
            return ws
    # 근사치(띄어쓰기 제거 후 동일) 정도만
    for ws in spreadsheet.worksheets():
        if norm_name(ws.title) == norm_name(title.replace(" ", "")):
            return ws
    return None

# -------------------- 구글 인증 --------------------

def open_spreadsheet(sheet_id: str):
    sa_json = os.environ.get("SA_JSON", "").strip()
    if sa_json:
        creds = Credentials.from_service_account_info(
            json.loads(sa_json),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
        gc = gspread.authorize(creds)
    else:
        gc = gspread.oauth()  # 로컬 개발용
    sh = gc.open_by_key(sheet_id)
    log("[gspread] spreadsheet opened")
    return sh

# -------------------- 엑셀 읽기 --------------------

def read_month_df(path: Path) -> pd.DataFrame:
    # sheet='data' 기준
    df = pd.read_excel(path, sheet_name="data", dtype=str)
    # 공백/NaN 보정
    df = df.fillna("")
    # 필요 컬럼 보강
    for c in NEEDED_COLS:
        if c not in df.columns:
            df[c] = ""
    # 숫자컬럼은 문자열로 일단 통일(후처리시 변환)
    return df.copy()

# -------------------- 집계 모듈 --------------------

def eok_series(ser: pd.Series) -> pd.Series:
    # "거래금액(만원)"을 억 단위 실수로
    if ser is None or ser.empty:
        return pd.Series([], dtype=float)
    s = pd.to_numeric(ser, errors="coerce")
    s = s.dropna()
    if s.empty: return pd.Series([], dtype=float)
    return s / 10000.0

def round2(v) -> str:
    try:
        if v == "" or v is None:
            return ""
        return f"{float(v):.2f}"
    except Exception:
        return ""

def agg_all_stats(df: pd.DataFrame):
    # 반환: counts(지도), med(지도:소수점2자리 str), mean(지도:소수점2자리 str)
    counts = {col: 0 for col in SUMMARY_COLS}
    med = {col: "" for col in SUMMARY_COLS}
    mean = {col: "" for col in SUMMARY_COLS}

    # 전국 합계/가격
    all_eok = eok_series(df["거래금액(만원)"])
    counts["전국"] = int(len(df))
    if not all_eok.empty:
        med["전국"] = round2(all_eok.median())
        mean["전국"] = round2(all_eok.mean())

    # 광역
    if "광역" in df.columns:
        for prov, sub in df.groupby("광역"):
            col = PROV_TO_SUMMARY_COL.get(prov, prov)
            if col in counts:
                counts[col] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[col] = round2(s.median())
                    mean[col] = round2(s.mean())

    # 서울/자치구
    seoul = df[df.get("광역","") == "서울특별시"].copy()
    if len(seoul) > 0:
        counts["서울"] = int(len(seoul))
        s = eok_series(seoul["거래금액(만원)"])
        if not s.empty:
            med["서울"] = round2(s.median())
            mean["서울"] = round2(s.mean())
    if "구" in seoul.columns:
        for gu, sub in seoul.groupby("구"):
            if gu in counts:
                counts[gu] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[gu] = round2(s.median())
                    mean[gu] = round2(s.mean())

    # 압구정동
    ap = seoul[seoul.get("법정동","") == "압구정동"]
    if "압구정동" in counts:
        counts["압구정동"] = int(len(ap))
        s = eok_series(ap["거래금액(만원)"])
        if not s.empty:
            med["압구정동"] = round2(s.median())
            mean["압구정동"] = round2(s.mean())

    return counts, med, mean

# -------------------- 탭(전국/서울 월 탭) 기록 --------------------

def ensure_row_for_date(ws, date_label: str) -> Tuple[int, bool]:
    """
    첫 컬럼(날짜)에 date_label이 있으면 해당 행 index(1-based), 없으면 하단에 append 후 그 행 index.
    반환: (row_idx, existed)
    """
    vals = _retry(ws.get_all_values) or []
    header_row = 1 if vals else 0
    if not vals:
        _retry(ws.update, [[ "날짜" ]], "A1")
        vals = [["날짜"]]

    # 날짜가 있는 컬럼은 A(=1) 이라고 가정(기존 구성 유지)
    for i in range(1, len(vals)):  # 1부터: 데이터 시작
        v = (vals[i][0] if len(vals[i]) > 0 else "").strip()
        if v == date_label:
            return i+1, True  # 1-based

    # 없으면 append
    new_row_idx = len(vals) + 1
    _retry(ws.update, [[date_label]], f"A{new_row_idx}")
    return new_row_idx, False

def write_counts_row(ws, row_idx: int, header: List[str], counts_map: Dict[str,int]):
    """
    행 전체를 한 번에 업데이트. header의 라벨과 counts_map의 key를 맞춰 채우고,
    '총합계' 열이 있으면 해당 행에서 합산 결과를 넣음.
    또한 '거래건수' 행은 볼드 처리(여기서는 월 탭은 항상 건수이므로 볼드).
    """
    # 현재 행 전체 길이에 맞춰 배열 준비
    col_count = max(ws.col_count, len(header))
    if ws.col_count < col_count:
        _retry(ws.add_cols, col_count - ws.col_count)

    # 기존 행 값을 읽어오지 않고, 해당 행 전체를 새 배열로 생성
    row_vals = [""] * col_count
    # 날짜(A열)은 이미 세팅되어 있다고 가정(ensure_row_for_date에서 처리)
    # 나머지 헤더 이름에 맞춰 값 채우기
    hmap = {h:i for i,h in enumerate(header)}
    total_sum = 0
    for k, v in counts_map.items():
        if k in hmap:
            j = hmap[k]
            row_vals[j] = int(v)
            if k != "총합계":
                total_sum += int(v)

    # 총합계 열 채우기
    if "총합계" in hmap:
        row_vals[hmap["총합계"]] = total_sum

    # 배치 업데이트
    rng = f"A{row_idx}:{a1_col(col_count)}{row_idx}"
    _retry(ws.update, [row_vals], rng, value_input_option="USER_ENTERED")

    # 볼드(해당 행 전체)
    req = {
        "requests": [{
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": row_idx-1,
                    "endRowIndex": row_idx,
                    "startColumnIndex": 0,
                    "endColumnIndex": col_count
                },
                "cell": {
                    "userEnteredFormat": { "textFormat": { "bold": True } }
                },
                "fields": "userEnteredFormat.textFormat.bold"
            }
        }]
    }
    _retry(ws.spreadsheet.batch_update, req)

# -------------------- 거래요약 탭 기록 --------------------

def ensure_summary_header(ws) -> Tuple[List[str], Dict[str,int]]:
    vals = _retry(ws.get_all_values) or []
    if not vals:
        header = ["년월","구분"] + SUMMARY_COLS
        _retry(ws.update, [header], "A1")
        vals = [header]
    else:
        header = vals[0]
        # 부족한 열이 있으면 뒤에 추가
        for c in ["년월","구분"] + SUMMARY_COLS:
            if c not in header:
                header.append(c)
        if header != vals[0]:
            _retry(ws.update, [header], "A1")
    hmap = {h:i for i,h in enumerate(header)}
    return header, hmap

def find_summary_row(ws, ym: str, label: str) -> int:
    vals = _retry(ws.get_all_values) or []
    if not vals:
        _retry(ws.update, [["년월","구분"]], "A1")
        vals = [["년월","구분"]]
    for i in range(1, len(vals)):
        row = vals[i]
        col0 = row[0] if len(row)>0 else ""
        col1 = row[1] if len(row)>1 else ""
        if col0 == ym and col1 == label:
            return i+1
    # 없으면 맨 아래 추가
    new_idx = len(vals) + 1
    _retry(ws.update, [[ym, label]], f"A{new_idx}:B{new_idx}")
    return new_idx

def batch_values_update(ws, payload: List[Dict]):
    # payload: [{"range": "C10", "values": [[123]]}, ...]
    body = {"valueInputOption":"USER_ENTERED", "data":[{"range":p["range"],"values":p["values"]} for p in payload]}
    _retry(ws.spreadsheet.values_batch_update, ws.spreadsheet.id, body)

def colorize_cell(ws, row_idx: int, col_idx0: int, rgb: Tuple[float,float,float]):
    req = {
        "requests": [{
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": row_idx-1,
                    "endRowIndex": row_idx,
                    "startColumnIndex": col_idx0,
                    "endColumnIndex": col_idx0+1
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {
                            "foregroundColor": {"red": rgb[0], "green": rgb[1], "blue": rgb[2]}
                        }
                    }
                },
                "fields": "userEnteredFormat.textFormat.foregroundColor"
            }
        }]
    }
    _retry(ws.spreadsheet.batch_update, req)

def write_month_summary(ws, year: int, month: int,
                        counts: Dict[str,int], med: Dict[str,str], mean: Dict[str,str],
                        prev_counts: Optional[Dict[str,int]]):
    ym = ym_label(year, month)
    header, hmap = ensure_summary_header(ws)

    # 1) 거래건수(굵게)
    row1 = find_summary_row(ws, ym, "거래건수")
    payload = []
    # SUMMARY_COLS 순서대로 기록
    for col in SUMMARY_COLS:
        if col in hmap:
            c = hmap[col]
            payload.append({"range": f"{a1_col(c+1)}{row1}", "values": [[counts.get(col,0)]]})
    batch_values_update(ws, payload)
    # 행 볼드
    first_c = hmap[SUMMARY_COLS[0]]
    last_c = hmap[SUMMARY_COLS[-1]]
    req = {
        "requests": [{
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": row1-1, "endRowIndex": row1,
                    "startColumnIndex": first_c, "endColumnIndex": last_c+1
                },
                "cell": { "userEnteredFormat": { "textFormat": { "bold": True } } },
                "fields": "userEnteredFormat.textFormat.bold"
            }
        }]
    }
    _retry(ws.spreadsheet.batch_update, req)
    log(f"[summary] {ym} 거래건수 -> row={row1}")

    # 2) 중앙값(억, 소수점 2자리 문자열)
    row2 = find_summary_row(ws, ym, "중앙값(단위:억)")
    payload = []
    for col in SUMMARY_COLS:
        if col in hmap:
            c = hmap[col]
            payload.append({"range": f"{a1_col(c+1)}{row2}", "values": [[med.get(col,"")]]})
    batch_values_update(ws, payload)
    log(f"[summary] {ym} 중앙값 -> row={row2}")

    # 3) 평균가(억, 소수점 2자리 문자열)
    row3 = find_summary_row(ws, ym, "평균가(단위:억)")
    payload = []
    for col in SUMMARY_COLS:
        if col in hmap:
            c = hmap[col]
            payload.append({"range": f"{a1_col(c+1)}{row3}", "values": [[mean.get(col,"")]]})
    batch_values_update(ws, payload)
    log(f"[summary] {ym} 평균가 -> row={row3}")

    # 4) 전월대비 건수증감(색상/기호 포함 텍스트)
    row4 = find_summary_row(ws, ym, "전월대비 건수증감")
    payload = []
    color_marks = []  # (row, col0, rgb)
    if prev_counts:
        for col in SUMMARY_COLS:
            diff = counts.get(col,0) - prev_counts.get(col,0)
            if diff > 0:
                val = f"+{diff}"
                rgb = (0.0, 0.3, 1.0)  # 파랑
            elif diff < 0:
                val = f"-{abs(diff)}"
                rgb = (1.0, 0.0, 0.0)  # 빨강
            else:
                val = "0"
                rgb = None
            c = hmap[col]
            payload.append({"range": f"{a1_col(c+1)}{row4}", "values": [[val]]})
            if rgb:
                color_marks.append((row4, c, rgb))
    else:
        # 전월 없음 -> 모두 공란
        for col in SUMMARY_COLS:
            c = hmap[col]
            payload.append({"range": f"{a1_col(c+1)}{row4}", "values": [[""]]})
    batch_values_update(ws, payload)
    for r, c0, rgb in color_marks:
        colorize_cell(ws, r, c0, rgb)
    log(f"[summary] {ym} 전월대비 -> row={row4}")

    # 5) 예상건수(미구현: 공란)
    row5 = find_summary_row(ws, ym, "예상건수")
    payload = []
    for col in SUMMARY_COLS:
        c = hmap[col]
        payload.append({"range": f"{a1_col(c+1)}{row5}", "values": [[""]]})
    batch_values_update(ws, payload)
    log(f"[summary] {ym} 예상건수 -> row={row5}")

# -------------------- 압구정동 탭: 원본 누적 + 변동 로그 --------------------

def ensure_apgu_sheet(sh):
    ws = fuzzy_ws(sh, APGU_SHEET_TITLE)
    if ws:
        return ws
    return _retry(sh.add_worksheet, title=APGU_SHEET_TITLE, rows=2000, cols=40)

def ensure_apgu_header(ws) -> List[str]:
    vals = _retry(ws.get_all_values) or []
    header = vals[0] if vals else []
    must_have = set(APGU_KEY_FIELDS + ["도로명","번지","본번","부번","동"])  # 보강 필드
    union = list(dict.fromkeys(header + list(must_have)))
    if "기록일" not in union:
        union.append("기록일")
    if union != header:
        _retry(ws.update, [union], "A1")
    return union

def make_apgu_key_from_dict(d: dict) -> tuple:
    return tuple(str(d.get(k, "")).strip() for k in APGU_KEY_FIELDS)

def make_apgu_key_from_row(row: List[str], header: List[str]) -> tuple:
    idx = {h:i for i,h in enumerate(header)}
    def get(h):
        i = idx.get(h, -1)
        return row[i].strip() if (i>=0 and i<len(row)) else ""
    return tuple(get(k) for k in APGU_KEY_FIELDS)

def append_apgu_change_log(ws, added: set[tuple], removed: set[tuple], header: List[str]):
    if not added and not removed:
        return

    used_vals = _retry(ws.get_all_values) or []
    used_rows = len(used_vals)
    if used_rows < 1: used_rows = 1

    now = datetime.now().strftime("%Y.%m.%d %H:%M")
    _retry(ws.append_rows, [[f"{now} 변동 로그"]], value_input_option="USER_ENTERED")
    used_rows += 1

    def line_from_key(tup: tuple, tag: str) -> List[str]:
        m = {k:v for k,v in zip(APGU_KEY_FIELDS, tup)}
        y = m.get("계약년",""); mo = m.get("계약월",""); d = m.get("계약일","")
        apt = m.get("단지명",""); area = m.get("전용면적(㎡)","")
        fl = m.get("층",""); price = m.get("거래금액(만원)","")
        return [f"({tag}) {y}/{mo}/{d} {apt} {area}㎡ {fl}층 {price}만원"]

    lines = []
    for k in sorted(added):
        lines.append(line_from_key(k, "신규"))
    for k in sorted(removed):
        lines.append(line_from_key(k, "삭제"))

    if not lines:
        return

    _retry(ws.append_rows, lines, value_input_option="USER_ENTERED")

    # 방금 추가한 lines 범위를 빨간 글자로
    start = used_rows + 1
    end = used_rows + len(lines)
    req = {
        "requests": [{
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": start-1,
                    "endRowIndex": end,
                    "startColumnIndex": 0,
                    "endColumnIndex": 1
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {
                            "foregroundColor": {"red":1.0,"green":0.0,"blue":0.0}
                        }
                    }
                },
                "fields": "userEnteredFormat.textFormat.foregroundColor"
            }
        }]
    }
    _retry(ws.spreadsheet.batch_update, req)

def upsert_apgu_raw(ws, df_all: pd.DataFrame, record_date: str):
    # 1) 압구정동만 선별
    cond = (df_all.get("광역","")=="서울특별시") & (df_all.get("법정동","")=="압구정동")
    df = df_all[cond].copy()
    log(f"[압구정동] filtered rows in file(s): {len(df)}")
    if df.empty:
        return

    # 날짜 보강/정렬
    for c in ["계약년","계약월","계약일"]:
        if c not in df.columns:
            df[c] = ""
    df = df.sort_values(
        ["계약년","계약월","계약일","단지명","전용면적(㎡)","층","거래금액(만원)"],
        ascending=[True,True,True,True,True,True,True],
        kind="mergesort"
    )

    # 헤더 보장
    header = ensure_apgu_header(ws)
    idx = {h:i for i,h in enumerate(header)}

    # 기존 키셋
    vals = _retry(ws.get_all_values) or []
    existing_rows = vals[1:] if vals else []
    old_keys = set()
    for r in existing_rows:
        old_keys.add(make_apgu_key_from_row(r, header))

    # 신규 본문(헤더 순서) + 기록일
    def nb(v):
        if v is None: return ""
        if isinstance(v, float) and pd.isna(v): return ""
        return v

    new_rows = []
    new_keys = set()
    for _, sr in df.iterrows():
        d = {col: sr.get(col, "") for col in header if col in df.columns}
        d["기록일"] = record_date
        key = make_apgu_key_from_dict(d)
        if key in new_keys:
            continue
        row = [nb(d.get(col, "")) for col in header]
        new_rows.append(row)
        new_keys.add(key)

    added = new_keys - old_keys
    removed = old_keys - new_keys

    # 본문 교체: 2행 이후 비우고 다시 씀
    needed = 1 + max(1, len(new_rows))
    if ws.row_count < needed:
        _retry(ws.add_rows, needed - ws.row_count)
    last = ws.row_count
    if last >= 2:
        _retry(ws.batch_clear, [f"2:{last}"])
    if new_rows:
        _retry(ws.update, new_rows, "A2", value_input_option="USER_ENTERED")

    append_apgu_change_log(ws, added, removed, header)
    log(f"[압구정동] updated rows={len(new_rows)}, +{len(added)} / -{len(removed)}")

# -------------------- 메인 --------------------

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts-dir", default=os.environ.get("ARTIFACTS_DIR","artifacts"))
    parser.add_argument("--sheet-id", default=os.environ.get("SHEET_ID","").strip(), required=False)
    args = parser.parse_args()

    log_block("MAIN")

    if not args.sheet_id:
        print("SHEET_ID 가 비어있습니다.", file=sys.stderr)
        sys.exit(1)

    sh = open_spreadsheet(args.sheet_id)

    # 아티팩트 내 xlsx 나열
    root = Path(args.artifacts_dir)
    if not root.exists():
        print(f"artifacts dir not found: {root}", file=sys.stderr)
        sys.exit(1)

    files = sorted(root.rglob("*.xlsx"))
    files = [p for p in files if "서울시" not in p.name]  # '전국 ' 파일만 대상
    log(f"[collect] found {len(files)} xlsx files")

    # 오늘 날짜 라벨
    today_label = fmt_date_kor(datetime.now())

    # 압구정동 누적용
    apgu_df_list = []

    # 월별 요약 집계 캐시(전월대비용)
    # key = (year, month) -> counts/med/mean
    summary_cache: Dict[Tuple[int,int], Dict[str,Dict]] = {}

    # 전국/서울 월 탭 기록
    for path in files:
        m = re.search(r"전국\s*(\d{2})(\d{2})_", path.name)
        if not m:
            continue
        yy = int(m.group(1))  # 24,25...
        mm = int(m.group(2))  # 01..12
        year = 2000 + yy
        month = mm

        nat_title = f"전국 {yy}년 {int(mm)}월" if mm.startswith("0") else f"전국 {yy}년 {month}월"
        nat_title = f"전국 {yy}년 {month}월"       # 공백/0패딩 차이는 fuzzy_ws로 보정
        seoul_title = f"서울 {yy}년 {month}월"

        log(f"[file] {path.name} -> nat='{nat_title}' seoul='{seoul_title}'")
        df = read_month_df(path)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        # 집계
        counts, med, mean = agg_all_stats(df)

        # 월 탭 쓰기(전국)
        ws_nat = fuzzy_ws(sh, nat_title)
        if ws_nat:
            vals = _retry(ws_nat.get_all_values) or []
            header = vals[0] if vals else ["날짜"] + SUMMARY_COLS + ["총합계"]
            if "총합계" not in header:
                header = header + ["총합계"]
                _retry(ws_nat.update, [header], "A1")
            row_idx, existed = ensure_row_for_date(ws_nat, today_label)
            write_counts_row(ws_nat, row_idx, header, counts)
            log(f"[전국] {nat_title} -> {today_label} {'update' if existed else 'append'} row={row_idx}")
        else:
            log(f"[전국] {nat_title} -> sheet not found (skip)")

        # 월 탭 쓰기(서울)
        ws_se = fuzzy_ws(sh, seoul_title)
        if ws_se:
            vals = _retry(ws_se.get_all_values) or []
            header = vals[0] if vals else ["날짜"] + SUMMARY_COLS + ["총합계"]
            if "총합계" not in header:
                header = header + ["총합계"]
                _retry(ws_se.update, [header], "A1")
            row_idx, existed = ensure_row_for_date(ws_se, today_label)
            write_counts_row(ws_se, row_idx, header, counts)
            log(f"[서울] {seoul_title} -> {today_label} {'update' if existed else 'append'} row={row_idx}")
        else:
            log(f"[서울] {seoul_title} -> sheet not found (skip)")

        # 거래요약 집계 캐시
        summary_cache[(year, month)] = {"counts": counts, "med": med, "mean": mean}

        # 압구정동 누적
        ap = df[(df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")].copy()
        if not ap.empty:
            apgu_df_list.append(ap)

    # 거래요약 탭 반영(모든 월)
    if summary_cache:
        ws_sum = fuzzy_ws(sh, SUMMARY_SHEET_TITLE)
        if not ws_sum:
            ws_sum = _retry(sh.add_worksheet, title=SUMMARY_SHEET_TITLE, rows=2000, cols=100)

        header, _ = ensure_summary_header(ws_sum)
        # 월 순으로 정렬 후 기록
        for (year, month) in sorted(summary_cache.keys()):
            cur = summary_cache[(year, month)]
            prev = summary_cache.get((year, month-1 if month>1 else 12 if year>2000 else 0,
                                      12 if month==1 else month-1))
            prev_counts = prev["counts"] if prev else None
            write_month_summary(ws_sum, year, month, cur["counts"], cur["med"], cur["mean"], prev_counts)

    # 압구정동 시트 최종 반영(원본 그대로, 변동 로그 포함)
    if apgu_df_list:
        ws_ap = ensure_apgu_sheet(sh)
        all_ap = pd.concat(apgu_df_list, ignore_index=True)
        upsert_apgu_raw(ws_ap, all_ap, record_date=today_label)

    # where_written 간단 기록(옵션)
    (LOG_DIR / "where_written.txt").write_text(
        "월별 탭/거래요약/압구정동 반영 완료\n", encoding="utf-8"
    )
    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    main()
