import duckdb

from src import transform
from src.common import month_list, period_bounds


def test_month_list_and_bounds():
    assert month_list("2024-01", "2024-06") == [f"2024-0{i}" for i in range(1, 7)]
    cfg = {"period": {"start_month": "2024-01", "end_month": "2024-06"}}
    assert period_bounds(cfg) == ("2024-01-01", "2024-07-01")
    cfg["period"]["end_month"] = "2024-12"
    assert period_bounds(cfg)[1] == "2025-01-01"


def _make_cfg(tmp_path):
    raw = tmp_path / "raw"; raw.mkdir()
    ref = tmp_path / "reference"; ref.mkdir()
    con = duckdb.connect()
    # ไฟล์อ้างอิงรูปแบบ CSV
    con.execute(f"""
        COPY (SELECT * FROM (VALUES (1,'Manhattan','Zone A','Yellow Zone'),
                                    (2,'Queens','Zone B','Boro Zone'))
              t(LocationID, Borough, Zone, service_zone))
        TO '{(ref / "taxi_zone_lookup.csv").as_posix()}' (HEADER, DELIMITER ',')
    """)
    # ไฟล์เที่ยวรูปแบบ Parquet (คอลัมน์โซนเป็น DOUBLE และสะกด PUlocationID เหมือน FHV จริง)
    con.execute(f"""
        COPY (SELECT * FROM (VALUES
            (TIMESTAMP '2024-01-05 10:00', TIMESTAMP '2024-01-05 10:10', 1.0::DOUBLE),
            (TIMESTAMP '2024-01-06 10:00', NULL, 1.0::DOUBLE),
            (TIMESTAMP '2024-01-07 10:00', TIMESTAMP '2024-01-07 10:30', NULL::DOUBLE),
            (TIMESTAMP '2024-01-08 10:00', TIMESTAMP '2024-01-08 10:30', 999.0::DOUBLE),
            (TIMESTAMP '2023-12-31 23:00', TIMESTAMP '2024-01-01 00:10', 2.0::DOUBLE),
            (NULL, TIMESTAMP '2024-01-09 10:00', 2.0::DOUBLE),
            (TIMESTAMP '2024-01-10 10:00', TIMESTAMP '2024-01-10 09:00', 2.0::DOUBLE)
        ) t(tpep_pickup_datetime, tpep_dropoff_datetime, PUlocationID))
        TO '{(raw / "yellow_tripdata_2024-01.parquet").as_posix()}' (FORMAT PARQUET)
    """)
    cfg = {
        "period": {"start_month": "2024-01", "end_month": "2024-06"},
        "paths": {"raw_dir": str(raw), "curated_dir": str(tmp_path / "curated"),
                  "reference_dir": str(ref)},
        "reference": {"zone_lookup": {"filename": "taxi_zone_lookup.csv", "url": ""}},
        "taxi_types": {"Yellow": {"prefix": "yellow_tripdata",
                                  "pickup": "tpep_pickup_datetime",
                                  "dropoff": "tpep_dropoff_datetime",
                                  "pickup_zone": "PULocationID"}},
        "sample_rows_per_file": 10,
    }
    return cfg, con


def test_process_file_classifies_every_row(tmp_path):
    cfg, con = _make_cfg(tmp_path)
    transform.load_zones(con, cfg)
    s = transform.process_file(con, cfg, "Yellow", "yellow_tripdata_2024-01.parquet", "2024-01")
    assert s["rows_total"] == 7
    assert s["valid"] == 4                      # รวมแถวที่ dropoff เป็น NULL
    assert s["out_of_period"] == 1
    assert s["null_pickup"] == 1
    assert s["dropoff_before_pickup"] == 1
    assert s["valid"] + s["out_of_period"] + s["null_pickup"] + s["dropoff_before_pickup"] == s["rows_total"]


def test_zone_join_keeps_row_count_and_labels_borough(tmp_path):
    cfg, con = _make_cfg(tmp_path)
    transform.load_zones(con, cfg)
    s = transform.process_file(con, cfg, "Yellow", "yellow_tripdata_2024-01.parquet", "2024-01")
    daily = (tmp_path / "curated" / "daily" / "yellow_tripdata_2024-01_daily.parquet").as_posix()
    got = dict(con.execute(
        f"SELECT pickup_borough, sum(trip_count)::BIGINT FROM read_parquet('{daily}') GROUP BY 1").fetchall())
    assert got == {"Manhattan": 2, "Missing zone": 1, "Unmatched ID": 1}
    assert sum(got.values()) == s["valid"]      # LEFT JOIN ต้องไม่ทำให้แถวเพิ่มหรือหาย
    assert s["zone_missing"] == 1 and s["zone_unmatched"] == 1 and s["zone_col_found"] is True
