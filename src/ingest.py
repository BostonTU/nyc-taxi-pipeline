"""Step 1: ดาวน์โหลดไฟล์ Parquet (เที่ยว) และ CSV (zone lookup) ข้ามไฟล์ที่มีแล้ว แล้วบันทึก manifest"""
import os
import time
from pathlib import Path

import requests

from .common import ensure_dir, expected_files, now_iso, reference_path, write_json


def _download(url, dest, retries=3, timeout=120):
    """โหลดแบบ stream ลงไฟล์ .part แล้วค่อยเปลี่ยนชื่อ กันไฟล์เสียครึ่งๆ กลางๆ"""
    part = dest.with_suffix(dest.suffix + ".part")
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                with open(part, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
            os.replace(part, dest)
            return None
        except Exception as e:  # noqa: BLE001
            last_error = str(e)
            time.sleep(2 * attempt)
    if part.exists():
        part.unlink()
    return last_error


def _ensure(url, path):
    if path.exists() and path.stat().st_size > 0:
        return "skipped_exists", None
    err = _download(url, path)
    return ("failed", err) if err else ("downloaded", None)


def run(cfg):
    raw = ensure_dir(cfg["paths"]["raw_dir"])
    manifest = {}
    counts = {"downloaded": 0, "skipped_exists": 0, "failed": 0}

    for taxi_type, fname, month in expected_files(cfg):
        path = Path(raw) / fname
        url = f"{cfg['base_url']}/{fname}"
        status, err = _ensure(url, path)
        manifest[fname] = {"taxi_type": taxi_type, "month": month, "format": "parquet",
                           "url": url, "status": status, "error": err,
                           "size_bytes": path.stat().st_size if path.exists() else 0,
                           "checked_at": now_iso()}
        counts[status] += 1
        print(f"  [{status}] {fname}")

    # ไฟล์อ้างอิงรูปแบบ CSV
    ensure_dir(cfg["paths"]["reference_dir"])
    rpath = reference_path(cfg)
    rurl = cfg["reference"]["zone_lookup"]["url"]
    status, err = _ensure(rurl, rpath)
    manifest[rpath.name] = {"taxi_type": "reference", "month": None, "format": "csv",
                            "url": rurl, "status": status, "error": err,
                            "size_bytes": rpath.stat().st_size if rpath.exists() else 0,
                            "checked_at": now_iso()}
    counts[status] += 1
    print(f"  [{status}] {rpath.name}")

    write_json(Path(cfg["paths"]["state_dir"]) / "manifest.json", manifest)
    if counts["failed"]:
        raise RuntimeError(f"ดาวน์โหลดไม่สำเร็จ {counts['failed']} ไฟล์ (ดู data/state/manifest.json)")
    return counts
