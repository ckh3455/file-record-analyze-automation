# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import io
import re
import json
from pathlib import Path
from datetime import datetime
from typing import List, Optional

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
]

ARTIFACTS_DIR = Path(os.environ.get("ARTIFACTS_DIR", "artifacts"))
# 둘 중 하나 사용:
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "").strip()
DRIVE_FOLDER_NAME = os.environ.get("DRIVE_FOLDER_NAME", "").strip()

MAX_FILES = int(os.environ.get("DRIVE_MAX_FILES", "40"))  # 너무 많이 받지 않게 제한


def load_sa_info() -> dict:
    sa_json = os.environ.get("SA_JSON", "").strip()
    sa_path = os.environ.get("SA_PATH", "").strip()

    if sa_json:
        return json.loads(sa_json)
    if sa_path:
        return json.loads(Path(sa_path).read_text(encoding="utf-8"))
    raise RuntimeError("SA_JSON 또는 SA_PATH 환경변수가 필요합니다.")


def drive_service():
    info = load_sa_info()
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def find_folder_id_by_name(svc, name: str) -> str:
    # 동일 이름 폴더가 여러 개면 가장 최근 modifiedTime 기준 1개 선택
    q = (
        f"name = '{name}' and "
        "mimeType = 'application/vnd.google-apps.folder' and "
        "trashed = false"
    )
    res = svc.files().list(
        q=q,
        fields="files(id,name,modifiedTime)",
        orderBy="modifiedTime desc",
        pageSize=5,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = res.get("files", [])
    if not files:
        raise RuntimeError(f"Drive 폴더를 찾지 못했습니다: {name}")
    return files[0]["id"]


def list_xlsx_in_folder(svc, folder_id: str) -> List[dict]:
    q = (
        f"'{folder_id}' in parents and "
        "mimeType != 'application/vnd.google-apps.folder' and "
        "trashed = false and "
        "(name contains '.xlsx' or mimeType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')"
    )
    res = svc.files().list(
        q=q,
        fields="files(id,name,modifiedTime,size,mimeType)",
        orderBy="modifiedTime desc",
        pageSize=MAX_FILES,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    return res.get("files", [])


def download_file(svc, file_id: str, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    req = svc.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.FileIO(out_path, "wb")
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.close()


def safe_name(name: str) -> str:
    # 경로/특수문자 최소 정리
    name = name.replace("/", "_").replace("\\", "_").strip()
    return name


def main():
    svc = drive_service()

    folder_id = DRIVE_FOLDER_ID
    if not folder_id:
        if not DRIVE_FOLDER_NAME:
            raise RuntimeError("DRIVE_FOLDER_ID 또는 DRIVE_FOLDER_NAME 둘 중 하나는 필요합니다.")
        folder_id = find_folder_id_by_name(svc, DRIVE_FOLDER_NAME)

    files = list_xlsx_in_folder(svc, folder_id)
    if not files:
        raise RuntimeError("Drive 폴더에서 xlsx 파일을 찾지 못했습니다.")

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[drive] folder_id={folder_id} files={len(files)} (show top 5)")
    for f in files[:5]:
        print(f"  - {f['name']}  modified={f.get('modifiedTime')}")

    # 전부 다운로드(또는 필요 시 규칙으로 필터)
    for f in files:
        fname = safe_name(f["name"])
        out = ARTIFACTS_DIR / fname
        print(f"[download] {fname} -> {out}")
        download_file(svc, f["id"], out)

    print("[done] downloaded xlsx into", ARTIFACTS_DIR)


if __name__ == "__main__":
    main()
