"""Entry point: python run_pipeline.py [--config config.yaml] [--force]"""
import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

from src import analyze, ingest, transform, validate
from src.common import ensure_dir, load_config, now_iso, write_json


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--force", action="store_true", help="ประมวลผลทุกไฟล์ใหม่ ไม่ใช้ state เดิม")
    args = ap.parse_args()

    cfg = load_config(args.config)
    outputs = ensure_dir(cfg["paths"]["outputs_dir"])

    steps = [
        ("ingest", ingest.run),
        ("validate_schema", validate.validate_schema),
        ("validate_reference", validate.validate_reference),
        ("transform", lambda c: transform.run(c, force=args.force)),
        ("quality_checks", validate.check_quality),
        ("analyze", analyze.run),
    ]

    log = {"run_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
           "started_at": now_iso(), "force": args.force, "steps": [], "status": "SUCCESS"}

    for name, fn in steps:
        print(f"\n== {name} ==")
        t0 = time.time()
        try:
            detail = fn(cfg)
            log["steps"].append({"step": name, "status": "OK",
                                 "seconds": round(time.time() - t0, 2), "detail": detail})
        except Exception as e:  # noqa: BLE001
            log["steps"].append({"step": name, "status": "FAILED",
                                 "seconds": round(time.time() - t0, 2), "error": str(e)})
            log["status"] = "FAILED"
            print(f"!! {name} failed: {e}")
            break

    log["finished_at"] = now_iso()
    write_json(outputs / "run_log.json", log)
    with open(Path(outputs) / "run_history.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(log, ensure_ascii=False) + "\n")

    print(f"\nPipeline {log['status']} -> {outputs / 'run_log.json'}")
    sys.exit(0 if log["status"] == "SUCCESS" else 1)


if __name__ == "__main__":
    main()
