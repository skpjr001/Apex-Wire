#!/usr/bin/env bash
set -euo pipefail

BINARY="${1:-./target/release/aw_itch_replay_v5}"
DATA="${2:-/home/sif/Project Life/data/itch_1gb_aa}"
OUTDIR="${3:-bench_results}"
CORES="${4:-2,3}"

mkdir -p "$OUTDIR"
REPORT="$OUTDIR/report.md"

{
echo "# Apex-Wire V5 Benchmark Report"
echo ""
echo "## Hardware Disclosure"
echo '```'
lscpu 2>/dev/null | grep -E 'Model name|^CPU\(s\)|Thread|Core|Socket|MHz' || true
free -h 2>/dev/null | head -2 || true
echo "kernel: $(uname -r)"
echo '```'
echo ""
echo "All runs use mmap'd sequential IO. If the data file is page-cache warm from"
echo "a prior run, IO throughput reflects memcpy+parse speed, not disk bandwidth."
echo "Latency histograms use RDTSC (18-cycle overhead on this CPU) with 1-in-16"
echo "sampling. L1 cache is warmed by the time steady-state measurements begin."
echo ""
} > "$REPORT"

MODES=("--legacy-single-lob" "--no-matching" "")
LABELS=("legacy-single-lob" "no-matching" "sharded-matching")

for idx in 0 1 2; do
    mode="${MODES[$idx]}"
    label="${LABELS[$idx]}"
    outfile="$OUTDIR/run_${label}.txt"
    echo "Running $label..."
    taskset -c "$CORES" "$BINARY" "$DATA" $mode > "$outfile" 2>&1 || true
    {
    echo "## Mode: $label"
    echo '```'
    if grep -q "msg rate" "$outfile" 2>/dev/null; then
        grep -E 'wall elapsed|msg rate|order event rate|evictions|apply latency p50|peak fill|IO throughput' "$outfile" || true
    else
        echo "(run failed or was OOM-killed -- see $outfile)"
    fi
    echo '```'
    echo ""
    } >> "$REPORT"
done

echo "Report: $REPORT"
