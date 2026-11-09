"""빠진 날짜의 전국·서울 월별 탭 행을 채워 넣는 일회성 보정 스크립트.

입력: 환경변수 BACKFILL_JSON
  {"2026-11-07": {"202611": {"전국": 3105, "서울": 135, "강남구": 5, ...}, ...}, ...}
날짜별 집계값은 그날 수집기 아티팩트의 '아파트 YYYYMM.xlsx'를
analyze_and_update.agg_all_stats 로 계산한 값입니다.
거래요약 탭은 건드리지 않습니다(항상 최신값만 유지).
이미 늦은 날짜 행이 있으면 날짜 순서에 맞는 위치에 행을 끼워 넣습니다.
"""
import json
import os
from datetime import date

import gspread

import analyze_and_update as A


def ensure_row_in_date_order(ws: gspread.Worksheet, day: date):
    """해당 날짜 행이 없고 그보다 늦은 날짜 행이 이미 있으면, 그 앞에 빈 행을 끼워 날짜를 적는다."""
    col = A._retry(ws.get, f"A2:A{ws.row_count}") or []
    insert_at = None
    for idx, row in enumerate(col, start=2):
        s = str(row[0]).strip() if row else ""
        d = A.parse_any_date(s) if s else None
        if d is None:
            continue
        if d == day:
            return
        if d > day and insert_at is None:
            insert_at = idx
    if insert_at is None:
        return  # 맨 아래에 붙이면 되므로 write_month_sheet 에 맡김
    label = A.date_label_for_sheet(day, style=A.detect_date_style(ws))
    A._retry(ws.insert_row, [label], index=insert_at, value_input_option="RAW")
    A._invalidate_cache(ws)
    A.log(f"[ws] {ws.title} inserted {label} at row {insert_at}")


def main():
    sheet_id = os.environ.get("SHEET_ID", "").strip()
    raw = os.environ.get("BACKFILL_JSON", "").strip()
    if not sheet_id or not raw:
        raise RuntimeError("SHEET_ID, BACKFILL_JSON 환경변수가 필요합니다.")

    data = json.loads(raw)
    creds = A.load_creds()
    sh = A._retry(gspread.authorize(creds).open_by_key, sheet_id)

    for day_str in sorted(data):
        day = date.fromisoformat(day_str)
        for ym in sorted(data[day_str], reverse=True):
            year, month = int(ym[:4]), int(ym[4:])
            counts = data[day_str][ym]

            ws_nat = A.ensure_month_ws(sh, "전국", year, month)
            ensure_row_in_date_order(ws_nat, day)
            values_nat = {k: int(counts.get(k, 0)) for k in A.NATION_REGIONS}
            A.write_month_sheet(ws_nat, day, ["날짜"] + A.NATION_REGIONS, values_nat)

            ws_seoul = A.ensure_month_ws(sh, "서울", year, month)
            ensure_row_in_date_order(ws_seoul, day)
            values_seoul = {k: int(counts.get(k, 0)) for k in A.SEOUL_REGIONS if k != "총합계"}
            values_seoul["총합계"] = int(counts.get("서울", 0))
            A.write_month_sheet(ws_seoul, day, ["날짜"] + A.SEOUL_REGIONS, values_seoul)

    A.log("[BACKFILL] done")


if __name__ == "__main__":
    main()
