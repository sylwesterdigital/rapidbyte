#!/usr/bin/env python3
"""Compare benchmark results across runs.

Usage:
    python3 tests/bench/analyze.py                          # Compare last 2 runs
    python3 tests/bench/analyze.py --last 5                 # Show last 5 runs
    python3 tests/bench/analyze.py --sha abc123 def456      # Compare specific commits
    python3 tests/bench/analyze.py --session-id bench-...   # Compare one isolated invocation
"""
import argparse
import json
import sys
from pathlib import Path
from collections import defaultdict

RESULTS_FILE = Path(__file__).parent.parent.parent / "target" / "bench_results" / "results.jsonl"

def load_results():
    if not RESULTS_FILE.exists():
        print("No benchmark results found.")
        print("Run a benchmark first:  just bench")
        print("Then compare runs:      just bench-compare")
        sys.exit(1)
    results = []
    for line in RESULTS_FILE.read_text().splitlines():
        if line.strip():
            results.append(json.loads(line))
    return results

def group_by_run(results):
    """Group results by (git_sha, mode, bench_rows)."""
    groups = defaultdict(list)
    for r in results:
        key = (r.get("git_sha", "?"), r.get("mode", "?"), r.get("bench_rows", 0))
        groups[key].append(r)
    return groups

def metric_value(result, key):
    if key == "cpu_cores_mean":
        pct = result.get("process_cpu_pct_one_core")
        return (float(pct) / 100.0) if isinstance(pct, (int, float)) else 0.0
    if key == "cpu_cores_max":
        pct = result.get("process_cpu_pct_one_core")
        return (float(pct) / 100.0) if isinstance(pct, (int, float)) else 0.0
    if key == "cpu_total_util_pct_mean":
        pct = result.get("process_cpu_pct_available_cores")
        return float(pct) if isinstance(pct, (int, float)) else 0.0
    if key == "cpu_total_util_pct_max":
        pct = result.get("process_cpu_pct_available_cores")
        return float(pct) if isinstance(pct, (int, float)) else 0.0
    if key == "mem_rss_mb_mean":
        rss_mb = result.get("process_peak_rss_mb")
        return float(rss_mb) if isinstance(rss_mb, (int, float)) else 0.0
    if key == "mem_rss_mb_max":
        rss_mb = result.get("process_peak_rss_mb")
        return float(rss_mb) if isinstance(rss_mb, (int, float)) else 0.0
    if key == "resource_samples":
        return 1.0 if isinstance(result.get("process_cpu_secs"), (int, float)) else 0.0

    val = result.get(key)
    return float(val) if isinstance(val, (int, float)) else 0.0


def aggregate(results, key, stat_mode):
    vals = [metric_value(r, key) for r in results]
    if not vals:
        return 0
    if stat_mode == "max":
        return max(vals)
    return sum(vals) / len(vals)

def main():
    parser = argparse.ArgumentParser(description="Compare benchmark results across runs")
    parser.add_argument("--last", type=int, default=2, help="Compare last N runs")
    parser.add_argument("--sha", nargs="+", help="Compare specific git SHAs")
    parser.add_argument("--session-id", help="Filter to a single benchmark session id")
    parser.add_argument("--rows", type=int, help="Filter by row count")
    parser.add_argument("--profile", help="Filter by benchmark profile")
    args = parser.parse_args()

    results = load_results()
    if args.rows:
        results = [r for r in results if r.get("bench_rows") == args.rows]
    if args.profile:
        results = [r for r in results if r.get("profile") == args.profile]
    if args.session_id:
        results = [r for r in results if r.get("bench_session_id") == args.session_id]

    if args.sha:
        results = [r for r in results if r.get("git_sha") in args.sha]

    groups = group_by_run(results)
    if not groups:
        print("No matching results found.")
        return

    # Get unique SHAs in order
    seen = []
    for r in results:
        sha = r.get("git_sha", "?")
        if sha not in seen:
            seen.append(sha)

    shas = seen[-args.last:] if not args.sha else args.sha

    metrics = [
        ("Duration (s)", "duration_secs", "s", "mean"),
        ("Source (s)", "source_duration_secs", "s", "mean"),
        ("  Arrow encode (s)", "source_arrow_encode_secs", "s", "mean"),
        ("Dest (s)", "dest_duration_secs", "s", "mean"),
        ("  Flush (s)", "dest_flush_secs", "s", "mean"),
        ("  Arrow decode (s)", "dest_arrow_decode_secs", "s", "mean"),
        ("  Commit (s)", "dest_commit_secs", "s", "mean"),
        ("  WASM overhead (s)", "wasm_overhead_secs", "s", "mean"),
        ("  VM setup (s)", "dest_vm_setup_secs", "s", "mean"),
        ("  Recv loop (s)", "dest_recv_secs", "s", "mean"),
        ("Source load (ms)", "source_module_load_ms", "ms", "mean"),
        ("Dest load (ms)", "dest_module_load_ms", "ms", "mean"),
        ("CPU cores avg", "cpu_cores_mean", "cores", "mean"),
        ("CPU cores peak", "cpu_cores_max", "cores", "max"),
        ("CPU total util avg (%)", "cpu_total_util_pct_mean", "%", "mean"),
        ("CPU total util peak (%)", "cpu_total_util_pct_max", "%", "max"),
        ("RSS avg (MB)", "mem_rss_mb_mean", "MB", "mean"),
        ("RSS peak (MB)", "mem_rss_mb_max", "MB", "max"),
        ("Resource samples", "resource_samples", "count", "mean"),
    ]

    for mode in ["insert", "copy"]:
        print(f"\n{'=' * 64}")
        print(f"  Mode: {mode.upper()}")
        print(f"{'=' * 64}")

        header = f"  {'Metric':<22s}"
        for sha in shas:
            header += f"  {sha:>12s}"
        if len(shas) == 2:
            header += f"  {'Change':>10s}"
        print(header)
        print(f"  {'-' * 22}" + f"  {'-' * 12}" * len(shas) + ("  " + "-" * 10 if len(shas) == 2 else ""))

        for label, key, unit, stat_mode in metrics:
            line = f"  {label:<22s}"
            vals = []
            if unit == "ms":
                fmt = ".1f"
            elif unit in {"cores", "%", "MB"}:
                fmt = ".2f"
            elif unit == "count":
                fmt = ".1f"
            else:
                fmt = ".4f"
            for sha in shas:
                matching = [r for r in results if r.get("git_sha") == sha and r.get("mode") == mode]
                v = aggregate(matching, key, stat_mode)
                vals.append(v)
                line += f"  {v:>12{fmt}}"
            if len(vals) == 2 and vals[0] > 0.0001:
                pct = (vals[1] - vals[0]) / vals[0] * 100
                sign = "+" if pct > 0 else ""
                line += f"  {sign}{pct:.1f}%"
            print(line)

        # Throughput summary
        print()
        for label, calc in [("Throughput (rows/s)", "rows"), ("Throughput (MB/s)", "mb")]:
            line = f"  {label:<22s}"
            vals = []
            for sha in shas:
                matching = [r for r in results if r.get("git_sha") == sha and r.get("mode") == mode]
                # Filter to runs that actually processed data
                valid = [r for r in matching if r.get("records_read", 0) > 0]
                if not valid:
                    vals.append(0)
                    line += f"  {'N/A':>12s}"
                    continue
                if calc == "rows":
                    v = sum(r["records_read"] / r["duration_secs"] for r in valid) / len(valid)
                    vals.append(v)
                    line += f"  {v:>12,.0f}"
                else:
                    v = sum(r.get("bytes_read", 0) / r["duration_secs"] / 1_048_576 for r in valid) / len(valid)
                    vals.append(v)
                    line += f"  {v:>12.2f}"
            if len(vals) == 2 and vals[0] > 0:
                speedup = vals[1] / vals[0]
                line += f"  {speedup:.2f}x"
            print(line)

if __name__ == "__main__":
    main()
