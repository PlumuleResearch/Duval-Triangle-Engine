"""
Duval Triangle CLI Tool
=======================
Quick command-line utilities:
  • diagnose   — diagnose a single reading from CLI arguments
  • seed-db    — populate a SQLite DB with sample data
  • batch-csv  — run batch diagnosis on a CSV file and print report

Usage:
    python cli.py diagnose --ch4 35 --c2h4 95 --c2h2 45
    python cli.py seed-db --db transformer_dga.db --rows 200
    python cli.py batch-csv --file dga_data.csv
"""

import argparse
import datetime
import random
import sys

from duval_engine import DGASample, diagnose, batch_diagnose
from db_connector import SQLiteAdapter, CSVAdapter, MockAdapter


def cmd_diagnose(args):
    sample = DGASample(
        transformer_id="CLI-INPUT",
        timestamp=datetime.datetime.now().isoformat(),
        ch4_ppm=args.ch4,
        c2h4_ppm=args.c2h4,
        c2h2_ppm=args.c2h2,
        source="cli",
    )
    r = diagnose(sample)
    print("\n" + "=" * 55)
    print("  DUVAL TRIANGLE DIAGNOSIS")
    print("=" * 55)
    print(f"  CH₄    : {args.ch4:.2f} ppm  ({sample.ch4_pct:.1f}%)")
    print(f"  C₂H₄   : {args.c2h4:.2f} ppm  ({sample.c2h4_pct:.1f}%)")
    print(f"  C₂H₂   : {args.c2h2:.2f} ppm  ({sample.c2h2_pct:.1f}%)")
    print("-" * 55)
    print(f"  FAULT ZONE : {r.fault_zone}")
    print(f"  SEVERITY   : {r.severity}")
    print(f"  DESCRIPTION: {r.description}")
    print(f"  COORDINATES: x={r.x:.3f}, y={r.y:.3f}")
    print("=" * 55 + "\n")


def cmd_seed_db(args):
    adapter = SQLiteAdapter(args.db)
    mock = MockAdapter(n_transformers=args.transformers, n_readings=args.rows)
    samples = mock.fetch_all()
    for s in samples:
        adapter.insert_sample(s)
    print(f"✅  Inserted {len(samples)} rows into '{args.db}'")
    adapter.close()


def cmd_batch_csv(args):
    adapter = CSVAdapter(args.file)
    samples = adapter.fetch_all()
    results = batch_diagnose(samples)

    from collections import Counter
    zone_counts = Counter(r.fault_zone for r in results)

    print(f"\n{'='*60}")
    print(f"  BATCH DGA DIAGNOSIS — {args.file}")
    print(f"  Total samples analysed: {len(results)}")
    print(f"{'='*60}")
    for zone, count in sorted(zone_counts.items()):
        bar = "█" * count
        print(f"  {zone:4s}  {count:4d}  {bar}")
    print(f"{'='*60}\n")

    if args.verbose:
        print(f"{'Transformer':<16} {'Timestamp':<22} {'Zone':<6} {'Severity'}")
        print("-" * 70)
        for r in results:
            print(f"{r.sample.transformer_id:<16} {r.sample.timestamp:<22} "
                  f"{r.fault_zone:<6} {r.severity}")


def main():
    parser = argparse.ArgumentParser(description="Duval Triangle CLI Tool")
    sub = parser.add_subparsers(dest="command")

    # diagnose
    p_diag = sub.add_parser("diagnose", help="Diagnose a single DGA sample")
    p_diag.add_argument("--ch4",  type=float, required=True, help="CH4 concentration in ppm")
    p_diag.add_argument("--c2h4", type=float, required=True, help="C2H4 concentration in ppm")
    p_diag.add_argument("--c2h2", type=float, required=True, help="C2H2 concentration in ppm")

    # seed-db
    p_seed = sub.add_parser("seed-db", help="Seed a SQLite database with mock data")
    p_seed.add_argument("--db",          default="transformer_dga.db", help="SQLite DB path")
    p_seed.add_argument("--rows",        type=int, default=200, help="Number of rows to insert")
    p_seed.add_argument("--transformers", type=int, default=4,  help="Number of transformers")

    # batch-csv
    p_csv = sub.add_parser("batch-csv", help="Run batch diagnosis from a CSV file")
    p_csv.add_argument("--file",    required=True, help="Path to CSV file")
    p_csv.add_argument("--verbose", action="store_true", help="Print per-row results")

    args = parser.parse_args()

    if args.command == "diagnose":
        cmd_diagnose(args)
    elif args.command == "seed-db":
        cmd_seed_db(args)
    elif args.command == "batch-csv":
        cmd_batch_csv(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
