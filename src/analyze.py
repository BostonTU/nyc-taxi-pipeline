"""Step 5: most used จาก curated data เทียบ baseline + แยกตามเขต (borough) จากการ join ไฟล์ CSV แล้วสร้างกราฟ"""
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from .common import ensure_dir, read_json, state_path, write_csv, write_json  # noqa: E402

NON_GEOGRAPHIC = {"Missing zone", "Unmatched ID", "Unknown", "N/A"}


def _rank(totals, members):
    subset = {k: v for k, v in totals.items() if k in members}
    ordered = sorted(subset.items(), key=lambda kv: kv[1], reverse=True)
    grand = sum(subset.values())
    return [{"rank": i + 1, "taxi_type": k, "trips": v,
             "share_percent": round(v / grand * 100, 2) if grand else 0}
            for i, (k, v) in enumerate(ordered)]


def run(cfg):
    out = ensure_dir(cfg["paths"]["outputs_dir"])
    charts = ensure_dir(out / "charts")
    state = read_json(state_path(cfg), {})
    daily_glob = (Path(cfg["paths"]["curated_dir"]) / "daily" / "*.parquet").as_posix()
    con = duckdb.connect()

    curated = dict(con.execute(
        f"SELECT taxi_type, sum(trip_count)::BIGINT FROM read_parquet('{daily_glob}') GROUP BY 1"
    ).fetchall())

    baseline = {}
    for s in state.values():
        baseline[s["taxi_type"]] = baseline.get(s["taxi_type"], 0) + s["rows_total"]

    comparison = []
    for t in cfg["taxi_types"]:
        b, c = baseline.get(t, 0), curated.get(t, 0)
        comparison.append({"taxi_type": t, "baseline_rows": b, "curated_trips": c,
                           "excluded": b - c,
                           "excluded_pct": round((b - c) / b * 100, 4) if b else 0})
    write_csv(out / "baseline_vs_curated.csv", comparison)

    results, summary_rows = {}, []
    for name, members in cfg["definitions"].items():
        ranking = _rank(curated, members)
        results[name] = {"winner": ranking[0]["taxi_type"] if ranking else None, "ranking": ranking}
        for r in ranking:
            summary_rows.append({"definition": name, **r})
    write_csv(out / "result_summary.csv", summary_rows)
    write_json(out / "result_summary.json", results)

    # ---- แยกตามเขตรับผู้โดยสาร (ผลจาก join CSV กับ Parquet) ----
    by_b = con.execute(f"""
        SELECT taxi_type, pickup_borough, sum(trip_count)::BIGINT
        FROM read_parquet('{daily_glob}') GROUP BY 1, 2
    """).fetchall()
    # reconcile: ผลรวมรายเขตต้องเท่ายอดรวมของประเภทนั้น
    for t in curated:
        s_b = sum(n for tt, _, n in by_b if tt == t)
        if s_b != curated[t]:
            raise RuntimeError(f"ผลรวมรายเขตของ {t} ({s_b}) ไม่เท่ายอดรวม ({curated[t]})")
    borough_rows = sorted(
        [{"taxi_type": t, "pickup_borough": b, "trips": n,
          "share_within_type_percent": round(n / curated[t] * 100, 2)} for t, b, n in by_b],
        key=lambda r: (r["taxi_type"], -r["trips"]))
    write_csv(out / "trips_by_borough.csv", borough_rows)

    boroughs = sorted({b for _, b, _ in by_b if b not in NON_GEOGRAPHIC})
    winners = []
    for b in boroughs:
        per_type = {t: n for t, bb, n in by_b if bb == b}
        total = sum(per_type.values())
        t_win = max(per_type, key=per_type.get)
        winners.append({"pickup_borough": b, "most_used_type": t_win,
                        "trips": per_type[t_win], "total_trips_in_borough": total,
                        "share_percent": round(per_type[t_win] / total * 100, 2)})
    write_csv(out / "most_used_by_borough.csv", winners)

    # ---- กราฟ ----
    items = sorted(curated.items(), key=lambda kv: kv[1])
    plt.figure(figsize=(9, 5))
    plt.barh([k for k, _ in items], [v for _, v in items])
    plt.xlabel("Valid trips (curated)")
    plt.title("NYC TLC trips by service type")
    plt.tight_layout()
    plt.savefig(charts / "total_by_type.png", dpi=150)
    plt.close()

    monthly = con.execute(f"""
        SELECT taxi_type, strftime(pickup_date, '%Y-%m') AS m, sum(trip_count)::BIGINT
        FROM read_parquet('{daily_glob}') GROUP BY 1, 2 ORDER BY 2
    """).fetchall()
    write_csv(out / "monthly_trips.csv", [{"taxi_type": t, "month": m, "trips": n} for t, m, n in monthly])
    plt.figure(figsize=(10, 5))
    for t in cfg["taxi_types"]:
        pts = [(m, n) for tt, m, n in monthly if tt == t]
        if pts:
            plt.plot([p[0] for p in pts], [p[1] for p in pts], marker="o", label=t)
    plt.yscale("log")
    plt.xlabel("Month")
    plt.ylabel("Valid trips (log scale)")
    plt.title("Monthly trips by service type")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(charts / "monthly_trend.png", dpi=150)
    plt.close()

    all_b = sorted({b for _, b, _ in by_b})
    plt.figure(figsize=(10, 5))
    types = list(cfg["taxi_types"])
    left = [0.0] * len(types)
    for b in all_b:
        shares = [next((n for t, bb, n in by_b if t == ty and bb == b), 0) / curated.get(ty, 1) * 100
                  for ty in types]
        plt.barh(types, shares, left=left, label=b)
        left = [l + s for l, s in zip(left, shares)]
    plt.xlabel("Share of trips by pickup borough (%)")
    plt.title("Pickup borough mix by service type (CSV zone lookup joined to Parquet)")
    plt.legend(fontsize=7, loc="upper center", bbox_to_anchor=(0.5, -0.15), ncol=4)
    plt.tight_layout()
    plt.savefig(charts / "borough_share_by_type.png", dpi=150)
    plt.close()

    return {"winners": {d: r["winner"] for d, r in results.items()},
            "boroughs_with_data": len(boroughs)}
