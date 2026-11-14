"""ตรวจ schema (validate_schema) ตรวจไฟล์อ้างอิง CSV (validate_reference) และตรวจคุณภาพหลังแปลง (check_quality)"""
from pathlib import Path

import duckdb

from .common import expected_files, read_json, reference_path, state_path, write_csv


class QualityError(RuntimeError):
    pass


def validate_schema(cfg):
    """ทุกไฟล์ต้องมีอยู่ และมีคอลัมน์ pickup/dropoff ตามที่ config ระบุ (คอลัมน์โซนตรวจแบบไม่สนตัวพิมพ์ใหญ่เล็ก)"""
    rows = []
    for taxi_type, fname, month in expected_files(cfg):
        path = Path(cfg["paths"]["raw_dir"]) / fname
        spec = cfg["taxi_types"][taxi_type]
        if not path.exists():
            rows.append({"file": fname, "taxi_type": taxi_type, "exists": False,
                         "pickup_ok": False, "dropoff_ok": False, "pickup_zone_ok": False, "n_columns": 0})
            continue
        cur = duckdb.connect().execute(f"SELECT * FROM read_parquet('{path.as_posix()}') LIMIT 0")
        names = [d[0] for d in cur.description]
        lower = {n.lower() for n in names}
        rows.append({"file": fname, "taxi_type": taxi_type, "exists": True,
                     "pickup_ok": spec["pickup"] in names,
                     "dropoff_ok": spec["dropoff"] in names,
                     "pickup_zone_ok": spec["pickup_zone"].lower() in lower,
                     "n_columns": len(names)})
    write_csv(Path(cfg["paths"]["outputs_dir"]) / "schema_check.csv", rows)
    bad = [r["file"] for r in rows if not (r["exists"] and r["pickup_ok"] and r["dropoff_ok"])]
    if bad:
        raise QualityError(f"schema check ล้มเหลว: {bad}")
    no_zone = [r["file"] for r in rows if not r["pickup_zone_ok"]]
    return {"files_checked": len(rows), "all_ok": True, "files_without_zone_column": len(no_zone)}


def validate_reference(cfg):
    """ไฟล์ zone lookup (CSV) ต้องมีคอลัมน์ครบ และ LocationID ต้องไม่ซ้ำ (ถ้าซ้ำ การ join จะทำให้จำนวนเที่ยวเพิ่มเกินจริง)"""
    path = reference_path(cfg)
    if not path.exists():
        raise QualityError(f"ไม่พบไฟล์อ้างอิง {path}")
    con = duckdb.connect()
    p = path.as_posix()
    cols = [d[0] for d in con.execute(f"SELECT * FROM read_csv_auto('{p}') LIMIT 0").description]
    lower = {c.lower() for c in cols}
    missing = [c for c in ("LocationID", "Borough", "Zone", "service_zone") if c.lower() not in lower]
    if missing:
        raise QualityError(f"ไฟล์ zone lookup ขาดคอลัมน์ {missing}")
    n, n_unique, n_null_borough = con.execute(
        f"SELECT count(*), count(DISTINCT LocationID), count(*) FILTER (WHERE Borough IS NULL) FROM read_csv_auto('{p}')"
    ).fetchone()
    row = {"file": path.name, "rows": n, "unique_location_ids": n_unique,
           "null_borough": n_null_borough, "status": "PASS" if (n > 0 and n == n_unique) else "FAIL"}
    write_csv(Path(cfg["paths"]["outputs_dir"]) / "reference_check.csv", [row])
    if row["status"] == "FAIL":
        raise QualityError("zone lookup ว่างหรือมี LocationID ซ้ำ")
    return {"rows": n, "unique_location_ids": n_unique}


def check_quality(cfg):
    """ตรวจผลหลัง transform: แถวรวมต้องเท่ากับผลรวมทุกกลุ่ม, daily parquet (หลัง join โซน) ต้องตรงกับ state, สัดส่วนที่ตัดต้องไม่เกินเกณฑ์"""
    state = read_json(state_path(cfg), {})
    th = cfg["quality"]
    con = duckdb.connect()
    report = []

    for taxi_type, fname, month in expected_files(cfg):
        s = state.get(fname)
        if s is None:
            report.append({"file": fname, "taxi_type": taxi_type, "month": month,
                           "status": "FAIL", "note": "ยังไม่ได้ประมวลผล"})
            continue

        total = s["rows_total"]
        parts = s["valid"] + s["null_pickup"] + s["out_of_period"] + s["dropoff_before_pickup"]
        excluded = total - s["valid"]
        excl_pct = round(excluded / total * 100, 4) if total else 100.0
        zone_missing = s.get("zone_missing", 0)
        zone_unmatched = s.get("zone_unmatched", 0)
        zone_missing_pct = round(zone_missing / s["valid"] * 100, 2) if s["valid"] else 0.0

        daily = Path(cfg["paths"]["curated_dir"]) / "daily" / fname.replace(".parquet", "_daily.parquet")
        daily_sum = con.execute(
            f"SELECT coalesce(sum(trip_count),0) FROM read_parquet('{daily.as_posix()}')"
        ).fetchone()[0]

        notes, status = [], "PASS"
        if parts != total:
            status = "FAIL"; notes.append("ผลรวมกลุ่มไม่เท่าแถวทั้งหมด")
        if daily_sum != s["valid"]:
            status = "FAIL"; notes.append("curated daily ไม่ตรงกับจำนวน valid (join โซนอาจทำให้แถวเพิ่ม/หาย)")
        if s["valid"] == 0:
            status = "FAIL"; notes.append("ไม่มีแถว valid")
        if excl_pct > th["fail_excluded_pct"]:
            status = "FAIL"; notes.append(f"ตัดทิ้ง {excl_pct}% เกินเกณฑ์ fail")
        elif excl_pct > th["warn_excluded_pct"] and status == "PASS":
            status = "WARN"; notes.append(f"ตัดทิ้ง {excl_pct}% เกินเกณฑ์ warn")
        if status == "PASS" and not s.get("zone_col_found", True):
            status = "WARN"; notes.append("ไม่พบคอลัมน์รหัสโซนในไฟล์")
        if status == "PASS" and zone_unmatched > 0:
            status = "WARN"; notes.append(f"มีรหัสโซน {zone_unmatched} แถวที่ไม่อยู่ใน lookup")

        report.append({
            "file": fname, "taxi_type": taxi_type, "month": month,
            "rows_total": total, "null_pickup": s["null_pickup"],
            "out_of_period": s["out_of_period"],
            "dropoff_before_pickup": s["dropoff_before_pickup"],
            "valid_rows": s["valid"], "excluded_pct": excl_pct,
            "valid_in_other_month_than_file": s["month_mismatch"],
            "zone_missing_pct": zone_missing_pct, "zone_unmatched_rows": zone_unmatched,
            "status": status, "note": "; ".join(notes),
        })

    fields = ["file", "taxi_type", "month", "rows_total", "null_pickup", "out_of_period",
              "dropoff_before_pickup", "valid_rows", "excluded_pct",
              "valid_in_other_month_than_file", "zone_missing_pct", "zone_unmatched_rows",
              "status", "note"]
    write_csv(Path(cfg["paths"]["outputs_dir"]) / "quality_report.csv", report, fields)

    summary = {k: sum(1 for r in report if r["status"] == k) for k in ("PASS", "WARN", "FAIL")}
    if summary["FAIL"]:
        raise QualityError(f"data quality ล้มเหลว {summary['FAIL']} ไฟล์ (ดู outputs/quality_report.csv)")
    return summary
