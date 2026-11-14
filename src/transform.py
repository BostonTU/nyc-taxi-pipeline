"""Step 3: แปลงทุกไฟล์เข้า schema กลาง กรองระดับแถว join โซน (CSV) นับเหตุผลที่ตัดทิ้ง เขียน curated output

หลักการ:
- ทุกแถวถูกจัดเข้า 1 ใน 4 กลุ่มที่ไม่ซ้อนกัน (null_pickup / out_of_period / dropoff_before_pickup / valid)
- dropoff เป็น NULL ได้ (ไฟล์ FHV มักไม่มี) จึงไม่ตัดทิ้ง
- join โซนด้วย LEFT JOIN จึงไม่ทำให้แถวหาย และ LocationID ใน lookup ถูกตรวจแล้วว่าไม่ซ้ำ จึงไม่ทำให้แถวเพิ่ม
  แถวที่ไม่มีรหัสโซนเป็น 'Missing zone' แถวที่รหัสไม่อยู่ใน lookup เป็น 'Unmatched ID'
- รันซ้ำแล้วข้ามไฟล์ที่ประมวลผลแล้ว (ขนาดไฟล์ตรงกัน และ schema_version ตรงกัน)
"""
from pathlib import Path

import duckdb

from .common import (ensure_dir, expected_files, now_iso, period_bounds,
                     read_json, reference_path, state_path, write_json)

SCHEMA_VERSION = 2  # เพิ่มรหัสโซน/borough


def _reason_sql(start, end):
    return f"""
        CASE
            WHEN p IS NULL THEN 'null_pickup'
            WHEN p < TIMESTAMP '{start}' OR p >= TIMESTAMP '{end}' THEN 'out_of_period'
            WHEN d IS NOT NULL AND d < p THEN 'dropoff_before_pickup'
            ELSE 'valid'
        END
    """


def load_zones(con, cfg):
    path = reference_path(cfg).as_posix()
    con.execute(f"""
        CREATE OR REPLACE TABLE zones AS
        SELECT CAST(LocationID AS INTEGER) AS loc_id, Borough AS borough,
               Zone AS zone, service_zone
        FROM read_csv_auto('{path}')
    """)


def process_file(con, cfg, taxi_type, fname, month):
    """ต้องเรียก load_zones(con, cfg) ก่อน"""
    spec = cfg["taxi_types"][taxi_type]
    start, end = period_bounds(cfg)
    src = (Path(cfg["paths"]["raw_dir"]) / fname).as_posix()
    curated = Path(cfg["paths"]["curated_dir"])
    daily_dir = ensure_dir(curated / "daily")
    sample_dir = ensure_dir(curated / "sample")
    stem = fname.replace(".parquet", "")

    cols = [d[0] for d in con.execute(f"SELECT * FROM read_parquet('{src}') LIMIT 0").description]
    zone_col = next((c for c in cols if c.lower() == spec["pickup_zone"].lower()), None)
    zid_expr = f'CAST("{zone_col}" AS INTEGER)' if zone_col else "CAST(NULL AS INTEGER)"

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW src AS
        SELECT CAST("{spec['pickup']}" AS TIMESTAMP) AS p,
               CAST("{spec['dropoff']}" AS TIMESTAMP) AS d,
               {zid_expr} AS zid
        FROM read_parquet('{src}')
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW classified AS
        SELECT s.p, s.d, s.zid,
               {_reason_sql(start, end)} AS reason,
               CASE WHEN s.zid IS NULL THEN 'Missing zone'
                    WHEN z.borough IS NULL THEN 'Unmatched ID'
                    ELSE z.borough END AS pickup_borough
        FROM src s LEFT JOIN zones z ON s.zid = z.loc_id
    """)

    counts = dict(con.execute("SELECT reason, count(*) FROM classified GROUP BY reason").fetchall())
    mismatch = con.execute(
        f"SELECT count(*) FROM classified WHERE reason='valid' AND strftime(p, '%Y-%m') <> '{month}'"
    ).fetchone()[0]
    zone_missing, zone_unmatched = con.execute("""
        SELECT count(*) FILTER (WHERE pickup_borough = 'Missing zone'),
               count(*) FILTER (WHERE pickup_borough = 'Unmatched ID')
        FROM classified WHERE reason = 'valid'
    """).fetchone()

    daily_out = (daily_dir / f"{stem}_daily.parquet").as_posix()
    con.execute(f"""
        COPY (
            SELECT '{taxi_type}' AS taxi_type, CAST(p AS DATE) AS pickup_date,
                   pickup_borough, count(*) AS trip_count, '{fname}' AS source_file
            FROM classified WHERE reason = 'valid'
            GROUP BY 2, 3 ORDER BY 2, 3
        ) TO '{daily_out}' (FORMAT PARQUET)
    """)

    n = int(cfg.get("sample_rows_per_file", 1000))
    sample_out = (sample_dir / f"{stem}_sample.parquet").as_posix()
    con.execute(f"""
        COPY (
            SELECT '{taxi_type}' AS taxi_type, p AS pickup_datetime, d AS dropoff_datetime,
                   zid AS pickup_location_id, pickup_borough, '{fname}' AS source_file
            FROM classified WHERE reason = 'valid' LIMIT {n}
        ) TO '{sample_out}' (FORMAT PARQUET)
    """)

    return {
        "schema_version": SCHEMA_VERSION,
        "taxi_type": taxi_type,
        "month": month,
        "size_bytes": Path(src).stat().st_size,
        "rows_total": sum(counts.values()),
        "valid": counts.get("valid", 0),
        "null_pickup": counts.get("null_pickup", 0),
        "out_of_period": counts.get("out_of_period", 0),
        "dropoff_before_pickup": counts.get("dropoff_before_pickup", 0),
        "month_mismatch": mismatch,
        "zone_col_found": zone_col is not None,
        "zone_missing": zone_missing,
        "zone_unmatched": zone_unmatched,
        "processed_at": now_iso(),
    }


def run(cfg, force=False):
    sp = state_path(cfg)
    state = {} if force else read_json(sp, {})
    con = duckdb.connect()
    load_zones(con, cfg)
    processed, skipped = 0, 0

    for taxi_type, fname, month in expected_files(cfg):
        raw_path = Path(cfg["paths"]["raw_dir"]) / fname
        daily = Path(cfg["paths"]["curated_dir"]) / "daily" / fname.replace(".parquet", "_daily.parquet")
        prev = state.get(fname)
        if (prev and prev.get("schema_version") == SCHEMA_VERSION
                and prev.get("size_bytes") == raw_path.stat().st_size and daily.exists()):
            skipped += 1
            print(f"  [skip] {fname}")
            continue
        print(f"  [process] {fname}")
        state[fname] = process_file(con, cfg, taxi_type, fname, month)
        write_json(sp, state)  # บันทึกทีละไฟล์ ถ้าล้มกลางทางรันใหม่แล้วทำต่อได้
        processed += 1

    write_json(sp, state)

    sample_dir = ensure_dir(cfg["paths"]["sample_dir"])
    sample_glob = (Path(cfg["paths"]["curated_dir"]) / "sample" / "*.parquet").as_posix()
    sample_csv = (sample_dir / "trips_sample.csv").as_posix()
    con.execute(f"""
        COPY (SELECT * FROM read_parquet('{sample_glob}') ORDER BY taxi_type, pickup_datetime)
        TO '{sample_csv}' (HEADER, DELIMITER ',')
    """)
    return {"processed": processed, "skipped": skipped}
