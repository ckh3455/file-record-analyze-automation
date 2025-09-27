# -*- coding: utf-8 -*-
import os, sys, re, json, time, random
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd

# ====== 설정 ======
LOG_DIR = Path("analyze_report")
WORK_DIR_DEFAULT = "artifacts"   # workflow에서 다운로드한 엑셀 위치
SHEET_NAME_DATA = "data"         # 엑셀 내부 시트명

# 시트 제목과 헤더명 정규화용
def norm(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).strip()

# “날짜” 표기: 2025. 9. 26 형식
def fmt_ymd_kor(d: datetime) -> str:
    return f"{d.year}. {d.month}. {d.day}"

# 거래요약 열(사용자 지정 순서 그대로)
SUMMARY_COLS = [
    "전국","서울","강남구","압구정동","경기도","인천광역시","세종시","서초구","송파구",
    "용산구","강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구",
    "강서구","강북구","관악구","구로구","금천구","도봉구","동대문구","서대문구","성북구",
    "은평구","중구","중랑구","부산","대구","광주","대전","강원도","경남","경북","전남",
    "전북","충남","충북","제주"
]

# 시/도 표준화 매핑(전국 탭 집계에 사용)
PROV_MAP = {
    "서울특별시": "서울",
    "세종특별자치시": "세종시",
    "강원특별자치도": "강원도",
    "경상남도": "경남",
    "경상북도": "경북",
    "광주광역시": "광주",
    "대구광역시": "대구",
    "대전광역시": "대전",
    "부산광역시": "부산",
    "울산광역시": "울산",
    "전라남도": "전남",
    "전북특별자치도": "전북",
    "제주특별자치도": "제주",
    "충청남도": "충남",
    "충청북도": "충북",
    "경기도": "경기도",
    "인천광역시": "인천광역시",
}

SEOUL_GU = [
    "강남구","강동구","강북구","강서구","관악구","광진구","구로구","금천구","노원구","도봉구",
    "동대문구","동작구","마포구","서대문구","서초구","성동구","성북구","송파구","양천구",
    "영등포구","용산구","은평구","종로구","중구","중랑구"
]

# ====== 로깅 ======
def log(msg: str):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("[%H:%M:%S]")
    line = f"{ts} {msg}"
    print(line)
    with open(LOG_DIR / "latest.log", "a", encoding="utf-8") as f:
        f.write(line + "\n")

def log_block(title: str):
    log(f"[{title.upper()}]")

# ====== Google Sheets ======
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

_LAST_CALL_TS = 0.0
def _throttle(min_interval_sec=0.45):
    global _LAST_CALL_TS
    now = time.time()
    delta = now - _LAST_CALL_TS
    if delta < min_interval_sec:
        time.sleep(min_interval_sec - delta)
    _LAST_CALL_TS = time.time()

def _with_retry(callable_fn, *args, **kwargs):
    base = 0.8
    for attempt in range(6):
        try:
            _throttle()
            return callable_fn(*args, **kwargs)
        except APIError as e:
            msg = str(e)
            if any(code in msg for code in ["429","500","502","503"]):
                sleep_s = base * (2 ** attempt) + random.uniform(0, 0.3)
                time.sleep(sleep_s)
                continue
            raise

def a1_col(idx: int) -> str:
    s = ""
    n = idx
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def batch_values_update(ws, data_payload):
    body = {"valueInputOption": "USER_ENTERED", "data": data_payload}
    return _with_retry(ws.spreadsheet.values_batch_update, body=body)

def open_sheet(sheet_id: str, sa_path: str|None):
    log("[gspread] auth with service account")
    if sa_path and Path(sa_path).exists():
        creds = Credentials.from_service_account_file(
            sa_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        )
    else:
        raw = os.environ.get("SA_JSON","").strip()
        if not raw:
            raise RuntimeError("No service account (sa.json or SA_JSON) provided")
        creds = Credentials.from_service_account_info(
            json.loads(raw),
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    log("[gspread] spreadsheet opened")
    return sh

def fuzzy_find_worksheet(sh, title: str):
    t_norm = norm(title)
    for ws in sh.worksheets():
        if norm(ws.title) == t_norm:
            return ws
    # 못 찾으면 None
    return None

# ====== 엑셀 로드 & 표준화 ======
NEEDED_COLS = ["광역","구","법정동","계약년","계약월","계약일","거래금액(만원)"]

def read_month_file(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=SHEET_NAME_DATA, dtype=str)
    # 숫자 열 튜닝
    for col in ["계약년","계약월","계약일","거래금액(만원)"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    # 기본 컬럼 리네이밍 시도(파일에 따라 약간 다른 이름 있을 수 있음)
    rename_map = {}
    # 가장 흔한 오타/공백/괄호 제거
    for c in df.columns:
        cn = str(c).strip()
        if cn in ["전용면적(㎡)","전용면적(㎥)"]:
            continue
    # 최소 필요 컬럼이 없으면 빈 DF 리턴
    if not set(["계약년","계약월","계약일"]).issubset(set(df.columns)):
        return pd.DataFrame(columns=NEEDED_COLS)
    # 없는 컬럼은 채움
    for c in NEEDED_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    return df[NEEDED_COLS].copy()

# ====== 집계 (전국/서울) ======
def agg_national(df: pd.DataFrame):
    """광역별 건수 집계 + 총합"""
    if df.empty: 
        return {}, 0
    cnts = df.groupby("광역").size()
    out = {}
    for prov, cnt in cnts.items():
        prov_std = PROV_MAP.get(str(prov), str(prov))
        out[prov_std] = int(cnt)
    total = int(len(df))
    return out, total

def agg_seoul_detail(df: pd.DataFrame):
    """서울만 구별 집계 + 압구정동"""
    if df.empty: 
        return {}, 0, 0
    seoul = df[df["광역"]=="서울특별시"].copy()
    if seoul.empty:
        return {}, 0, 0
    by_gu = seoul.groupby("구").size().to_dict()
    # 압구정동
    apg = seoul[seoul["법정동"]=="압구정동"]
    apg_cnt = int(len(apg)) if not apg.empty else 0
    total_seoul = int(len(seoul))
    # 표준화
    out = {}
    for gu, cnt in by_gu.items():
        out[str(gu)] = int(cnt)
    return out, total_seoul, apg_cnt

# ====== 가격 통계 (억 단위) ======
def price_stats(df: pd.DataFrame):
    """전체(전국) 기준 중앙값/평균가 (억원)"""
    if df.empty or "거래금액(만원)" not in df.columns:
        return None, None
    s = pd.to_numeric(df["거래금액(만원)"], errors="coerce").dropna()
    if s.empty:
        return None, None
    # 만원 -> 억
    s_eok = s / 10000.0
    med = float(s_eok.median())
    mean = float(s_eok.mean())
    return round(med, 2), round(mean, 2)

# ====== 월/탭 제목 파싱 ======
def parse_from_filename(fn: str):
    """
    '전국 2410_250926.xlsx' → nat='전국 24년 10월', seoul='서울 24년 10월'
    """
    m = re.search(r"(\d{2})(\d{2})", fn)
    if not m:
        return None, None
    yy, mm = m.group(1), m.group(2)
    nat = f"전국 20{yy}년 {int(mm)}월"
    seoul = f"서울 20{yy}년 {int(mm)}월"
    ym_for_summary = f"{yy}/{int(mm)}"  # 거래요약 탭의 년월 셀
    return nat, seoul, ym_for_summary

# ====== 날짜/행 찾기 ======
def find_or_append_date_row(ws, target_label: str):
    """
    target_label: '2025. 9. 27' 같은 문자열
    첫 열(날짜)에서 동일 텍스트가 있으면 그 행, 없으면 마지막+1 행 리턴
    """
    vals = _with_retry(ws.get_all_values)
    if not vals:
        return 2  # 헤더만 있다고 가정
    # 헤더 1행
    header = vals[0] if vals else []
    # 날짜 열은 보통 A(1)
    for i, row in enumerate(vals[1:], start=2):
        if (len(row) > 0) and str(row[0]).strip() == target_label:
            return i
    return len(vals) + 1

def find_summary_row(ws, ym: str, label: str):
    """
    거래요약 탭에서 '년월'(A열)과 '구분'(B열)을 동시에 찾아 행 번호 반환.
    없으면 새로 추가할 다음 행 번호 반환.
    """
    vals = _with_retry(ws.get_all_values)
    if not vals:
        return 2
    for i, row in enumerate(vals[1:], start=2):
        a = str(row[0]).strip() if len(row)>0 else ""
        b = str(row[1]).strip() if len(row)>1 else ""
        if a == ym and b == label:
            return i
    return len(vals) + 1

# ====== 쓰기 (배치) ======
def write_month_sheet(ws, date_label: str, header: list[str], values_by_colname: dict[str,int]):
    """
    월별 탭(전국/서울): 날짜 행 찾아 해당 열들 + '총합계'까지 한 번에 기록
    """
    # 헤더 맵
    hmap = {str(h).strip(): idx+1 for idx,h in enumerate(header) if str(h).strip()}
    row_idx = find_or_append_date_row(ws, date_label)

    payload = []
    # 날짜(A열)
    payload.append({"range": f"A{row_idx}", "values": [[date_label]]})

    # 각 열 값
    for col_name, val in values_by_colname.items():
        if col_name not in hmap:
            continue
        c = hmap[col_name]
        payload.append({"range": f"{a1_col(c)}{row_idx}", "values": [[val]]})

    if payload:
        batch_values_update(ws, payload)
        log(f"[ws] write row {row_idx} ({ws.title}) -> {date_label}")

def write_month_summary(ws, ym: str, counts_map: dict, seoul_map: dict,
                        apg_cnt: int, med_eok: float|None, mean_eok: float|None,
                        prev_counts_map: dict|None):
    """
    거래요약 탭 한 달(ym)의 5개 라인:
    - 거래건수
    - 중앙값(단위:억)
    - 평균가(단위:억)
    - 전월대비 건수증감 (+/- 숫자)
    - 예상건수 (여기는 우선 공란/0 처리)
    -> 모두 SUMMARY_COLS 순서로 한 번의 배치로 기록
    """
    header = _with_retry(ws.row_values, 1)
    hmap = {str(h).strip(): i+1 for i,h in enumerate(header) if str(h).strip()}
    # A/B 라벨 먼저 한 번에
    def row_update(label_row_idx, label_text):
        payload = [
            {"range": f"A{label_row_idx}", "values": [[ym]]},
            {"range": f"B{label_row_idx}", "values": [[label_text]]},
        ]
        batch_values_update(ws, payload)

    # 1) 거래건수
    row1 = find_summary_row(ws, ym, "거래건수")
    row_update(row1, "거래건수")
    payload1 = []
    # 전국
    total_nat = int(counts_map.get("전국", 0))
    # 서울
    total_seoul = int(seoul_map.get("서울합계", 0))
    # 집계 소스에서 "전국" 필드는 없을 수 있으니 만들어 둔다
    values_line1 = {
        "전국": total_nat,
        "서울": total_seoul,
        "압구정동": apg_cnt,
    }
    # 수도권/광역
    for prov_std in PROV_MAP.values():
        if prov_std in SUMMARY_COLS:
            values_line1[prov_std] = int(counts_map.get(prov_std, 0))
    # 서울 구
    for gu in SEOUL_GU:
        if gu in SUMMARY_COLS:
            values_line1[gu] = int(seoul_map.get(gu, 0))
    # 나머지 SUMMARY_COLS 중 빠진 건 0으로 채우기
    for col in SUMMARY_COLS:
        if col not in values_line1:
            values_line1[col] = 0
    # 배치 구성
    for col in SUMMARY_COLS:
        if col not in hmap: 
            continue
        c = hmap[col]
        payload1.append({"range": f"{a1_col(c)}{row1}", "values": [[values_line1[col]]]})
    batch_values_update(ws, payload1)
    log(f"[summary] {ym} 거래건수 -> row={row1}")

    # 2) 중앙값(단위:억)
    row2 = find_summary_row(ws, ym, "중앙값(단위:억)")
    row_update(row2, "중앙값(단위:억)")
    payload2 = []
    # 전국/서울/구는 ‘가격’이 아닌데, 요구는 "전국 열"에만 전국 중앙값,
    # 서울/구 등은 비워두는게 자연스러워서 전국에만 기록 (원하면 동일 값 보급 가능)
    line2 = {col: "" for col in SUMMARY_COLS}
    if med_eok is not None:
        line2["전국"] = med_eok
    for col in SUMMARY_COLS:
        if col not in hmap: 
            continue
        payload2.append({"range": f"{a1_col(hmap[col])}{row2}", "values": [[line2[col]]]})
    batch_values_update(ws, payload2)
    log(f"[summary] {ym} 중앙값 -> row={row2}")

    # 3) 평균가(단위:억)
    row3 = find_summary_row(ws, ym, "평균가(단위:억)")
    row_update(row3, "평균가(단위:억)")
    payload3 = []
    line3 = {col: "" for col in SUMMARY_COLS}
    if mean_eok is not None:
        line3["전국"] = mean_eok
    for col in SUMMARY_COLS:
        if col not in hmap: 
            continue
        payload3.append({"range": f"{a1_col(hmap[col])}{row3}", "values": [[line3[col]]]})
    batch_values_update(ws, payload3)
    log(f"[summary] {ym} 평균가 -> row={row3}")

    # 4) 전월대비 건수증감 (+/-)
    # 이전 달 카운트 맵이 있으면 차이 계산
    row4 = find_summary_row(ws, ym, "전월대비 건수증감")
    row_update(row4, "전월대비 건수증감")
    payload4 = []
    line4 = {}
    if prev_counts_map:
        diff_nat = total_nat - int(prev_counts_map.get("전국", 0))
        def signed(n): 
            return f"+{n}" if n>0 else (f"{n}" if n<0 else "0")
        line4["전국"] = signed(diff_nat)
        # 서울
        diff_seoul = total_seoul - int(prev_counts_map.get("서울합계", 0))
        line4["서울"] = signed(diff_seoul)
        # 압구정동
        diff_apg = apg_cnt - int(prev_counts_map.get("압구정동", 0))
        line4["압구정동"] = signed(diff_apg)
        # 광역/구
        for col in SUMMARY_COLS:
            if col in ["전국","서울","압구정동"]: 
                continue
            cur = values_line1.get(col, 0)
            prev = int(prev_counts_map.get(col, 0))
            line4[col] = signed(cur - prev)
    else:
        # 이전 달 없으면 전부 0
        for col in SUMMARY_COLS:
            line4[col] = "0"

    for col in SUMMARY_COLS:
        if col not in hmap: continue
        payload4.append({"range": f"{a1_col(hmap[col])}{row4}", "values": [[line4[col]]]})
    batch_values_update(ws, payload4)
    log(f"[summary] {ym} 전월대비 -> row={row4}")

    # 5) 예상건수 (규칙 미정 → 우선 빈칸)
    row5 = find_summary_row(ws, ym, "예상건수")
    row_update(row5, "예상건수")
    payload5 = []
    line5 = {col: "" for col in SUMMARY_COLS}
    for col in SUMMARY_COLS:
        if col not in hmap: continue
        payload5.append({"range": f"{a1_col(hmap[col])}{row5}", "values": [[line5[col]]]})
    batch_values_update(ws, payload5)
    log(f"[summary] {ym} 예상건수 -> row={row5}")

# ====== 메인 ======
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts-dir", default=WORK_DIR_DEFAULT)
    parser.add_argument("--sheet-id", default=os.environ.get("SHEET_ID",""))
    parser.add_argument("--sa", default="sa.json")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_DIR/"latest.log","w",encoding="utf-8") as f:
        f.write("")

    log_block("main")
    log(f"artifacts_dir={args.artifacts_dir}")

    # 수집
    work = Path(args.artifacts_dir)
    files = sorted([p for p in work.rglob("*.xlsx") if p.is_file()])
    log(f"total xlsx under work_dir: {len(files)}")

    # 시트 열기
    sh = open_sheet(args.sheet_id, args.sa)

    # 요약 탭 핸들
    ws_summary = fuzzy_find_worksheet(sh, "거래요약")

    # 전월 대비를 위해 월별 누적 결과 저장 (요약 기록 직전에 사용)
    monthly_prev_cache = {}

    where_file = open(LOG_DIR/"where_written.txt","w",encoding="utf-8")

    for path in files:
        fn = path.name
        if not fn.startswith("전국"):
            continue
        nat_title, seoul_title, ym = parse_from_filename(fn)
        if not nat_title:
            continue

        # 해당 월 데이터 로드
        df = read_month_file(path)
        log(f"[read] rows={len(df)} cols={len(df.columns)} ({fn})")
        # 집계
        nat_map, nat_total = agg_national(df)
        seoul_map_raw, seoul_total, apg_cnt = agg_seoul_detail(df)
        # ‘서울합계’ 키 추가
        seoul_map = {"서울합계": seoul_total, **seoul_map_raw}

        # 가격 통계(전국 기준)
        med, mean = price_stats(df)

        # ----- 월별 탭 쓰기 -----
        # ‘전국’ 탭
        ws_nat = fuzzy_find_worksheet(sh, nat_title)
        if ws_nat:
            header_nat = _with_retry(ws_nat.row_values, 1)
            # 총합계 값 계산
            values_nat = {}
            for prov_std, colname in PROV_MAP.items():
                pass  # 키 충돌 방지용 (미사용)
            # 값 채우기
            for k, v in nat_map.items():
                values_nat[k] = v
            values_nat["총합계"] = nat_total  # 총합계
            date_label = fmt_ymd_kor(datetime.now())
            write_month_sheet(ws_nat, date_label, header_nat, values_nat)
            where_file.write(f"{ws_nat.title}\t{date_label}\tOK\n")
        else:
            log(f"[전국] {nat_title} -> sheet not found (skip)")

        # ‘서울’ 탭
        ws_se = fuzzy_find_worksheet(sh, seoul_title)
        if ws_se:
            header_se = _with_retry(ws_se.row_values, 1)
            values_se = {}
            for gu in SEOUL_GU:
                values_se[gu] = int(seoul_map.get(gu, 0))
            values_se["총합계"] = seoul_total
            date_label = fmt_ymd_kor(datetime.now())
            write_month_sheet(ws_se, date_label, header_se, values_se)
            where_file.write(f"{ws_se.title}\t{date_label}\tOK\n")
        else:
            log(f"[서울] {seoul_title} -> sheet not found (skip)")

        # ----- 거래요약 쓰기 -----
        if ws_summary:
            # 현월 counts_map: SUMMARY_COLS 기준으로 맞춰둔 맵을 만들어 prev 비교에 사용
            counts_map_for_prev = {}
            # 전국/서울/압구정동
            counts_map_for_prev["전국"] = nat_total
            counts_map_for_prev["서울합계"] = seoul_total
            counts_map_for_prev["압구정동"] = apg_cnt
            # 광역
            for prov_std in SUMMARY_COLS:
                if prov_std in ["전국","서울","압구정동"]: 
                    continue
                if prov_std in SEOUL_GU:
                    counts_map_for_prev[prov_std] = int(seoul_map.get(prov_std, 0))
                else:
                    counts_map_for_prev[prov_std] = int(nat_map.get(prov_std, 0))

            prev = monthly_prev_cache.get(ym)
            write_month_summary(
                ws_summary, ym,
                counts_map_for_prev,   # 전국/시도/서울구 섞인 현재
                {"서울합계": seoul_total, **seoul_map_raw},  # seoul_map (합계+구)
                apg_cnt,
                med, mean,
                prev_counts_map = prev
            )
            # 현재 값을 캐시에 저장 → 다음 파일(다음 월)에서 전월 비교에 사용
            monthly_prev_cache[ym] = counts_map_for_prev

    where_file.close()
    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {e!r}")
        raise
