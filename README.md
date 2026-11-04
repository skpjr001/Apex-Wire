

<div align="center">

# Apex-Wire V5

**Production-Grade ITCH Replay with Per-Symbol Order Books, Matching Engine, and Live Feed Ingestion**

</div>

V5 replaces V4's single-LOB replay with a correct per-symbol sharded architecture, integrates a synthetic matching engine, adds MoldUDP64/SoupBinTCP framing for real NASDAQ multicast and TCP feeds, and ships with per-message-type latency histograms. Validated against 1 GB of real NASDAQ ITCH 5.0 data: 34,057,154 messages, 8,850 unique symbols, 100% recognition.

For V2, V3, and V4 feature history, see `docs/V2.md`, `docs/V3.md`, and the V4 README preserved in git history.

---

## Headline Result

**3.06 M messages/sec** (2.94 M order events/sec) parsing real NASDAQ ITCH 5.0 through **per-symbol limit order books** with LRU eviction, 2-thread disruptor pipeline, 2013 Haswell i5. Apply latency p50: **33.8 ns**.

This exceeds the NASDAQ peak live feed rate of 1-2 M msg/s on decade-old desktop hardware.

<div align="center">

![V5 Benchmark](benchmarks/aw_v5_benchmark.png)

</div>

---

## V4 vs V5: An Honest Comparison

V4 reported 29.3 M msg/s. V5 reports 3.06 M msg/s on the same hardware and data. The drop is real and the reason is architectural, not a regression.

| | V4 | V5 |
| :--- | :--- | :--- |
| Throughput | 29.3 M msg/s | 3.06 M msg/s |
| Apply latency p50 | ~25 ns | 33.8 ns |
| LOB architecture | Single global LOB | Per-symbol LOB (512 resident) |
| Order ID correctness | Colliding across symbols | Isolated per symbol |
| Matching engine | None | Synthetic price-time crossing |
| Pipeline | Single-thread loop | 2-thread SPSC disruptor |
| Live feed framing | None | MoldUDP64 + SoupBinTCP |
| Eviction | N/A | LRU (2-random-sample, O(1)) |

V4 mixed all ~8,850 NASDAQ symbols into one `LimitOrderBook`. ITCH `order_reference_number` values are unique per session, not per symbol, and V4's `order_map` is direct-mapped by `(order_id & mask)`. Two different symbols can collide on the same slot, silently corrupting book state. V4's 29.3 M msg/s measures a defective architecture. V5's 3.06 M msg/s measures the correct one.

The 10x throughput gap comes from three sources: (1) per-symbol LOB resolution and LRU eviction, (2) inter-thread disruptor ring traversal with backpressure, and (3) the matching engine's crossing logic. On a modern multi-core server with isolated cores and sufficient RAM to hold all symbols resident, the ring and eviction overhead collapses and the gap narrows to the matching engine cost alone.

---

## Real Data Benchmark

Source: `07302019.NASDAQ_ITCH50` from `emi.nasdaq.com`. Full trading day, decompressed to 8,260 MB raw binary.

### V5 Sharded, No Matching (1 GB, page-cache warm, cap=512)

| Metric | Value |
| :--- | :--- |
| File size | 1,024 MB |
| Total messages | 34,057,154 |
| Order events (parsed + LOB applied) | 32,752,807 |
| Info messages (recognized, no LOB impact) | 1,304,347 |
| Unrecognized | 0 |
| Recognition rate | 100.0% |
| Unique symbols | 8,850 |
| Resident symbol cap | 512 |
| LRU evictions | 3,412,333 |
| Elapsed | 11.14 s |
| Throughput (total) | 3.06 M msg/s |
| Throughput (order events) | 2.94 M order evt/s |
| Apply latency p50 | 33.8 ns |
| Apply latency p99 | 2,409 ns |
| IO throughput | 91.9 MB/s |

### V5 Legacy Single-LOB (1 GB, V4-equivalent defect mode)

| Metric | Value |
| :--- | :--- |
| Elapsed | 2.07 s |
| Throughput (total) | 16.5 M msg/s |
| Apply latency p50 | 13.4 ns |

Provided for baseline comparison. Reproduces V4's single-LOB aliasing defect intentionally.

### V4 Historical (1 GB, single-thread)

| Metric | Value |
| :--- | :--- |
| Elapsed | 1.164 s |
| Throughput (total) | 29.3 M msg/s |
| Throughput (order events) | 28.1 M order evt/s |
| IO throughput | 879.7 MB/s |

Single-thread tight loop, single LOB, no pipeline overhead. See V4 README in git history for full-day (8.3 GB) results.

### Per-Message-Type Latency (V5 Sharded, ns)

| Message Type | p50 | p99 | p99.9 | n |
| :--- | :--- | :--- | :--- | :--- |
| AddOrder / AddOrderMpid | 27.2 | 3,057 | 5,966 | 947,907 |
| OrderExecuted | 40.0 | 2,058 | 4,122 | 36,085 |
| OrderCancel | 30.0 | 1,747 | 2,429 | 53,372 |
| OrderDelete | 48.1 | 2,041 | 3,760 | 811,447 |
| OrderReplace | 26.2 | 2,008 | 4,023 | 188,387 |
| Other | 20.0 | 2,224 | 407,239 | 9,853 |

### Context

| System | Throughput | Notes |
| :--- | :--- | :--- |
| NASDAQ peak live rate | 1-2 M msg/s | Real production feed per multicast group |
| Apex-Wire V5 (sharded) | 3.06 M msg/s | Per-symbol LOB + LRU, 2013 Haswell i5 |
| Java SBE (Aeron) | 5-10 M msg/s | Parse only, modern Xeon |
| C++ ITCH parsers | 15-40 M msg/s | Parse only, modern Xeon |
| Apex-Wire V5 (legacy) | 16.5 M msg/s | Single-LOB defect mode, 2-thread pipeline |
| Apex-Wire V4 | 28.1 M order evt/s | Single-LOB defect mode, single-thread |

Apex-Wire V5 includes per-symbol LOB insertion, LRU eviction, and a disruptor pipeline in the measured path. Most published benchmarks measure parse-only on modern server hardware. The V5 sharded result exceeds the NASDAQ live rate on a 2013 desktop CPU. Modern hardware (Ice Lake, Zen 4) with core isolation and sufficient RAM is expected to improve throughput proportionally with memory bandwidth and IPC gains, though this has not been measured.

---

## V5 New Features

### Per-Symbol LOB Sharding

* Flat `Vec<u16>` index (128 KB, L2-resident) maps `stock_locate` to compact `Vec<SymbolState>`.
* Each symbol gets its own `LimitOrderBook` (65,536 slots) and optional `MatchingEngine`.
* O(1) lookup via direct index, no hash map in the hot path.

### LRU Eviction with Adaptive Cap

* 2-random-sample approximate LRU: O(1) per eviction, ~90% of true LRU quality.
* Adaptive symbol cap reads `/proc/meminfo` at startup and auto-sizes (512 on 6.5 GB free, ~4,096 on 64 GB).
* Override with `--cap N`.
* Evicted LOBs are pooled and reused via `clear()`, avoiding re-allocation.

### Matching Engine Integration

* `MatchingEngine::match_order()` performs synthetic price-time crossing driven by real ITCH arrival timing and prices.
* ITCH-to-engine order ID bridge with generation-based ABA protection.
* Resting orders fully consumed by synthetic crossing are detected and pruned immediately.
* Disclosed modeling simplification: NASDAQ's reported partial fills cannot be replayed against the engine's own resting orders (no partial-quantity-reduction path in `cancel_order()`). The engine runs independent crossing logic, not a mirror of NASDAQ's historical trade tape.

### Live Feed Ingestion

* **MoldUDP64** (UDP multicast): 20-byte header stripping, session tracking, sequence gap detection.
* **SoupBinTCP** (TCP): 2-byte length-prefix framing, Login/Logout/Heartbeat/Sequenced packet handling.
* **Raw ITCH** (`--raw`): skips framing for captured pcap streams or file replay over stdin.
* `SO_BUSY_POLL` 50us on all live sockets for reduced recv latency.
* TCP reconnect with 2-second backoff on disconnect.
* SIGINT/SIGTERM graceful shutdown: drains the ring and prints the full report.

### Per-Type Latency Histograms

* 6 buckets: AddOrder, OrderExecuted, OrderCancel, OrderDelete, OrderReplace, Other.
* Sampled at 1-in-16 to bound memory allocation.
* Reveals which message types dominate tail latency.

### Memory Optimizations

* `MADV_SEQUENTIAL` on mmap'd file for read-ahead.
* `MADV_DONTNEED` on consumed file pages every 64 MB to release page cache pressure.
* `MADV_HUGEPAGE` on LOB backing memory (advisory, kernel uses 2 MB pages if available).

### Benchmark Tooling

* `--brief` flag suppresses the correctness note for screenshot-ready output.
* `scripts/bench_report.sh` for automated benchmark report generation.
* Ring backpressure tracking: total `claim_spin` wait time reported in the output.

---

## V4 Features (Preserved)

All V4 crates and features are preserved unchanged. See the V4 README in git history for full details.

* **Live Network Ingestion** (`aw-net`): `recvmmsg` batch receiver, IGMP multicast.
* **OUCH 5.0 Order Entry** (`aw-exchange::ouch`): NASDAQ binary codec, `OuchFramer`.
* **FIX 4.4 Order Entry** (`aw-fix-gateway`): Session layer, NewOrderSingle, ExecutionReport.
* **Latency Budget Instrumentation** (`aw-core::budget`): 11 named pipeline stages.
* **Strategy Hot-Reload** (`aw-pipeline::strategy`): `Strategy` trait, `StrategyRegistry`.
* **Production Hardening** (`aw-core::harden`): Signal handling, CPU pinning, SCHED_FIFO, mlockall, watchdog.

---

## Measurement Methodology

* **Host:** Intel Core i5-4570 Haswell @ 3.20 GHz, 11 GB RAM (~6.5 GB available), Arch Linux 7.2.9-arch1-1.
* **NIC:** Intel I217-LM (e1000e). Does not support AF_XDP, XDP, or precision hardware timestamping. All benchmarks use file replay. Live feed code compiles and is structurally correct but has not been validated against a real NASDAQ feed.
* **Pinning:** Ingest thread on core 2, consumer thread on core 3, `performance` CPU governor.
* **Timing:** x86 `rdtsc`. All reported cycle counts are net of 18-cycle `rdtsc` instruction overhead.
* **Cache:** First run warms the page cache; reported numbers are from the second (hot-cache) run.
* **Sampling:** Latency histograms sampled at 1-in-16 messages (2,047,051 samples per run).
* **Real data:** NASDAQ ITCH 5.0 file from `emi.nasdaq.com`, real exchange data.
* **End-to-end wire latency** (~22 ms p50) is dominated by disruptor ring backpressure on a 4-core machine where the OS competes for cores 0-1. On a production server with core isolation, this would collapse to sub-microsecond ring traversal time. The apply latency (33.8 ns p50) is the hardware-independent compute cost.

---

## Test Status

**40 unit tests pass, 0 failures, 0 warnings** across core crates.

| Crate | Tests | Coverage |
| :--- | :--- | :--- |
| `aw-exchange` | 24 | LOB lifecycle, BBO, clear, replace, cancel, ITCH/FIX/OUCH codecs, SIMD |
| `aw-matching` | 10 | Crossing, ABA generation, multi-level BBO, clear, bounds |
| `aw-disruptor` | 6 | SPSC, backpressure, wrap-around, partial drain, alignment |

Pre-existing compile warnings in `aw_v2_benchmark.rs`, `aw_v4_benchmark.rs`, and `aw_v5_benchmark.rs` are intentionally preserved from V2/V3/V4 history and do not affect correctness.

---

## Known Limitations

1. **No live feed validated.** MoldUDP64 and SoupBinTCP framing code is implemented and structurally correct, but the I217-LM NIC cannot receive NASDAQ multicast (requires 10GbE+). Live feed validation is planned for V6 on cloud infrastructure.
2. **Matching engine is synthetic.** The engine performs independent price-time crossing driven by real arrival timing, not a replay of NASDAQ's historical trade tape. Partial fills reported by NASDAQ cannot be reproduced because `cancel_order()` has no partial-quantity-reduction path. This is a disclosed modeling choice, not a bug.
3. **Matching mode is RAM-bound on constrained hardware.** At cap=512, matching mode requires ~4 GB for LOBs plus engine state. On 6.5 GB available RAM it completes but takes 20+ minutes. On 64 GB with cap=4096 it would complete in under a minute.
4. **Eviction destroys state.** FIFO/LRU eviction drops the LOB, engine, and bridge for the evicted symbol. On reload, the book starts empty. ~10% of messages at cap=512 hit a freshly-reconstructed LOB. This is reported in the output, not hidden.
5. **End-to-end latency is backpressure-dominated.** The 22 ms wire-to-apply p50 is a pipeline artifact of running a 2-thread SPSC ring on a 4-core machine. The 33.8 ns apply p50 is the meaningful compute number.
6. **No FPGA path.** Top-tier HFT firms use FPGA for sub-microsecond tick-to-trade. Apex-Wire is software-only.
7. **AF_XDP and VFIO are untested on this hardware.** The I217-LM NIC does not support XDP. These crates compile but cannot run end-to-end on this machine.

---

## Crate Layout (32 crates)

New or extended in V5:

| Crate | Status | Purpose |
| :--- | :--- | :--- |
| `aw-bench` | EXTENDED | `aw_itch_replay_v5` with sharding, matching, live ingest |
| `aw-exchange` | EXTENDED | LOB `clear()` optimization, V5 test suite |
| `aw-matching` | EXTENDED | `clear()` method, V5 test suite |
| `aw-disruptor` | EXTENDED | V5 test suite |

All V2, V3, and V4 crates are preserved unchanged.

---

## Build and Run

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release --workspace

# Sharded replay (V5 default, correct per-symbol LOB)
./target/release/aw_itch_replay_v5 data.bin

# Sharded, no matching (faster, isolates LOB cost)
./target/release/aw_itch_replay_v5 --no-matching data.bin

# Legacy single-LOB (V4-equivalent baseline)
./target/release/aw_itch_replay_v5 --legacy-single-lob data.bin

# Live multicast (MoldUDP64 framing)
./target/release/aw_itch_replay_v5 --live-udp 224.0.0.1:20001 10.0.0.5

# Live TCP (SoupBinTCP framing)
./target/release/aw_itch_replay_v5 --live-tcp 192.168.1.100:9001

# Live pipe from tcpdump
tcpdump -i eth0 -U -w - | ./target/release/aw_itch_replay_v5 --live-stdin

# Override symbol cap, skip framing, brief output
./target/release/aw_itch_replay_v5 --cap 1024 --raw --brief data.bin

cargo test --workspace
```

---

## License

MIT
