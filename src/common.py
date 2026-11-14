"""ฟังก์ชันกลางที่ใช้ร่วมกันทุก step"""
import csv
import json
from datetime import datetime
from pathlib import Path

import yaml


def load_config(path="config.yaml"):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def month_list(start_month, end_month):
    y, m = map(int, start_month.split("-"))
    ey, em = map(int, end_month.split("-"))
    months = []
    while (y, m) <= (ey, em):
        months.append(f"{y}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return months


def period_bounds(cfg):
    """คืน (start, end_exclusive) เป็นสตริงวันที่ เช่น ('2024-01-01', '2024-07-01')"""
    start_month = cfg["period"]["start_month"]
    ey, em = map(int, cfg["period"]["end_month"].split("-"))
    ny, nm = (ey + 1, 1) if em == 12 else (ey, em + 1)
    return f"{start_month}-01", f"{ny}-{nm:02d}-01"


def expected_files(cfg):
    """รายการ (taxi_type, filename, month) ที่ต้องมีตาม config"""
    months = month_list(cfg["period"]["start_month"], cfg["period"]["end_month"])
    return [
        (t, f"{spec['prefix']}_{mo}.parquet", mo)
        for t, spec in cfg["taxi_types"].items()
        for mo in months
    ]


def ensure_dir(p):
    p = Path(p)
    p.mkdir(parents=True, exist_ok=True)
    return p


def read_json(path, default=None):
    p = Path(path)
    if not p.exists():
        return default
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def write_json(path, obj):
    ensure_dir(Path(path).parent)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def write_csv(path, rows, fieldnames=None):
    ensure_dir(Path(path).parent)
    if fieldnames is None:
        fieldnames = list(rows[0].keys()) if rows else []
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def state_path(cfg):
    return Path(cfg["paths"]["state_dir"]) / "processed_files.json"


def reference_path(cfg):
    return Path(cfg["paths"]["reference_dir"]) / cfg["reference"]["zone_lookup"]["filename"]
