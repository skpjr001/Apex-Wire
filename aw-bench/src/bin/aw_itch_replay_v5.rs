use aw_disruptor::{Consumer, Disruptor, Publisher};
use aw_exchange::itch::parse_itch_message;
use aw_exchange::lob::LimitOrderBook;
use aw_exchange::order::{EventKind, MarketEvent};
use aw_matching::{MatchResult, MatchingEngine, NULL_IDX};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::Read;
use std::net::{TcpStream, UdpSocket};
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

const RING_CAPACITY: usize = 65536;
const DRAIN_BATCH: u64 = 256;
const GHZ: f64 = 3.2;
const LOCATE_MAX: usize = 65536;
const NOT_RESIDENT: u16 = u16::MAX;
const MADVISE_CHUNK: usize = 64 * 1024 * 1024;
const LIVE_BUF_SIZE: usize = 1 << 20;
const MOLD_HEADER_SIZE: usize = 20;
const HIST_BUCKETS: usize = 6;

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn signal_handler(_: libc::c_int) {
    SHUTDOWN.store(true, Ordering::Release);
}

fn install_signal_handlers() {
    // SAFETY: signal() with a valid handler function pointer and standard signal numbers.
    unsafe {
        libc::signal(libc::SIGINT, signal_handler as *const () as libc::sighandler_t);
        libc::signal(libc::SIGTERM, signal_handler as *const () as libc::sighandler_t);
    }
}

#[inline(always)]
fn rdtsc() -> u64 {
    // SAFETY: _rdtsc is always available on x86_64.
    unsafe { core::arch::x86_64::_rdtsc() }
}

fn set_affinity(core: usize) {
    // SAFETY: cpu_set_t is zeroed then populated with a valid core id.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

#[derive(Clone, Copy, Default)]
struct WireEvent {
    event: MarketEvent,
    publish_rdtsc: u64,
    global_idx: u64,
}

struct MappedFile {
    ptr: *const u8,
    len: usize,
}

impl MappedFile {
    fn open(path: &str) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len() as usize;
        let fd = file.as_raw_fd();
        // SAFETY: fd refers to a just-opened regular file, len matches its size.
        let ptr = unsafe {
            libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, fd, 0)
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: ptr and len describe the mapping; madvise is advisory.
        unsafe { libc::madvise(ptr, len, libc::MADV_SEQUENTIAL); }
        Ok(Self { ptr: ptr as *const u8, len })
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid for len bytes.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for MappedFile {
    fn drop(&mut self) {
        // SAFETY: ptr/len from open(), not unmapped elsewhere.
        unsafe { libc::munmap(self.ptr as *mut libc::c_void, self.len); }
    }
}

unsafe impl Send for MappedFile {}
unsafe impl Sync for MappedFile {}

#[derive(Clone, Copy, PartialEq)]
enum RunMode {
    Sharded,
    LegacySingleLob,
    ShardedNoMatching,
}

impl RunMode {
    fn matching_enabled(self) -> bool { matches!(self, RunMode::Sharded) }
    fn sharded(self) -> bool { !matches!(self, RunMode::LegacySingleLob) }
    fn label(self) -> &'static str {
        match self {
            RunMode::Sharded => "sharded + matching (V5 default)",
            RunMode::LegacySingleLob => "legacy single-LOB (V4-equivalent baseline, matching disabled)",
            RunMode::ShardedNoMatching => "sharded, matching disabled",
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum IngestSource { File, LiveUdp, LiveTcp, LiveStdin }

struct MoldState {
    expected_seq: u64,
    gaps_detected: u64,
    heartbeats: u64,
    session: [u8; 10],
    session_known: bool,
}

impl MoldState {
    fn new() -> Self {
        Self { expected_seq: 0, gaps_detected: 0, heartbeats: 0, session: [0; 10], session_known: false }
    }

    fn process_datagram<'a>(&mut self, datagram: &'a [u8]) -> Option<&'a [u8]> {
        if datagram.len() < MOLD_HEADER_SIZE { return None; }
        let session = &datagram[..10];
        let seq = u64::from_be_bytes(datagram[10..18].try_into().ok()?);
        let msg_count = u16::from_be_bytes(datagram[18..20].try_into().ok()?);
        if !self.session_known {
            self.session.copy_from_slice(session);
            self.session_known = true;
            self.expected_seq = seq;
        }
        if msg_count == 0xFFFF {
            self.heartbeats += 1;
            return None;
        }
        if self.expected_seq != 0 && seq > self.expected_seq {
            self.gaps_detected += seq - self.expected_seq;
        }
        self.expected_seq = seq.wrapping_add(msg_count as u64);
        Some(&datagram[MOLD_HEADER_SIZE..])
    }
}

struct SoupState {
    heartbeats: u64,
    logged_in: bool,
}

impl SoupState {
    fn new() -> Self {
        Self { heartbeats: 0, logged_in: false }
    }

    fn process_packet<'a>(&mut self, packet: &'a [u8]) -> Option<&'a [u8]> {
        if packet.is_empty() { return None; }
        match packet[0] {
            b'S' => Some(&packet[1..]),
            b'H' => { self.heartbeats += 1; None }
            b'O' => None,
            b'A' => { self.logged_in = true; None }
            b'J' | b'L' => { self.logged_in = false; None }
            _ => None,
        }
    }
}

struct SymbolState {
    locate: u16,
    lob: LimitOrderBook,
    engine: Option<MatchingEngine>,
    bridge: HashMap<u64, (u32, u16)>,
    resting_by_engine_id: HashMap<u32, u64>,
    last_touch: u64,
    touch_count: u64,
    is_reload: bool,
    ticker_known: bool,
}

fn placeholder_ticker(loc: u16) -> [u8; 8] {
    let s = format!("LOC{:05}", loc);
    let mut out = [0u8; 8];
    out.copy_from_slice(s.as_bytes());
    out
}

struct SymbolTable {
    index: Vec<u16>,
    states: Vec<SymbolState>,
    seen: Vec<bool>,
    unique_count: u64,
    cap: usize,
    matching_enabled: bool,
    lob_pool: Vec<LimitOrderBook>,
    evictions: u64,
    fill_constructions: u64,
    evict_constructions: u64,
    rng: u64,
}

impl SymbolTable {
    fn new(cap: usize, matching_enabled: bool) -> Self {
        let mut states = Vec::with_capacity(cap + 1);
        // SAFETY: madvise on valid heap allocation, advisory only, kernel ignores if unsupported.
        unsafe {
            libc::madvise(
                states.as_mut_ptr() as *mut libc::c_void,
                (cap + 1) * std::mem::size_of::<SymbolState>(),
                libc::MADV_HUGEPAGE,
            );
        }
        Self {
            index: vec![NOT_RESIDENT; LOCATE_MAX],
            states,
            seen: vec![false; LOCATE_MAX],
            unique_count: 0,
            cap,
            matching_enabled,
            lob_pool: Vec::new(),
            evictions: 0,
            fill_constructions: 0,
            evict_constructions: 0,
            rng: 0xdeadbeefcafebabe,
        }
    }

    fn find_lru_victim(&mut self) -> usize {
        let len = self.states.len();
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        let a = (self.rng as usize) % len;
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        let b = (self.rng as usize) % len;
        if self.states[a].last_touch <= self.states[b].last_touch { a } else { b }
    }

    fn resolve(&mut self, loc: u16, global_idx: u64) -> (&mut SymbolState, Option<(u64, bool)>) {
        let slot = self.index[loc as usize];
        if slot != NOT_RESIDENT {
            let state = &mut self.states[slot as usize];
            state.last_touch = global_idx;
            state.touch_count += 1;
            return (state, None);
        }
        let construction;
        let was_eviction = self.states.len() >= self.cap;
        if was_eviction {
            let evict_slot = self.find_lru_victim();
            let evict_loc = self.states[evict_slot].locate;
            self.index[evict_loc as usize] = NOT_RESIDENT;
            let last_idx = self.states.len() - 1;
            if evict_slot != last_idx {
                self.states.swap(evict_slot, last_idx);
                self.index[self.states[evict_slot].locate as usize] = evict_slot as u16;
            }
            let evicted = self.states.pop().unwrap();
            self.lob_pool.push(evicted.lob);
            self.evictions += 1;
            self.evict_constructions += 1;
        } else {
            self.fill_constructions += 1;
        }
        let is_reload = self.seen[loc as usize];
        if !is_reload {
            self.seen[loc as usize] = true;
            self.unique_count += 1;
        }
        let t0 = rdtsc();
        let ticker = placeholder_ticker(loc);
        let lob = match self.lob_pool.pop() {
            Some(mut l) => { l.clear(ticker); l }
            None => LimitOrderBook::new(ticker),
        };
        let engine = if self.matching_enabled { Some(MatchingEngine::new(ticker)) } else { None };
        let t1 = rdtsc();
        let new_slot = self.states.len() as u16;
        self.index[loc as usize] = new_slot;
        self.states.push(SymbolState {
            locate: loc, lob, engine,
            bridge: HashMap::with_capacity(256),
            resting_by_engine_id: HashMap::with_capacity(256),
            last_touch: global_idx, touch_count: 1, is_reload, ticker_known: false,
        });
        construction = Some((t1.wrapping_sub(t0), was_eviction));
        let state = &mut self.states[new_slot as usize];
        (state, construction)
    }
}

struct IngestStats {
    messages_total: u64,
    itch_parsed: u64,
    unrecognized: u64,
    published: u64,
    elapsed_cycles: u64,
    ring_backpressure_ns: u64,
    mold_gaps: u64,
    mold_heartbeats: u64,
    soup_heartbeats: u64,
}

struct ConsumerStats {
    wire_samples: Vec<u64>,
    apply_samples: Vec<u64>,
    end_to_end_samples: Vec<u64>,
    apply_by_type: [Vec<u64>; HIST_BUCKETS],
    fill_construction_samples: Vec<u64>,
    evict_construction_samples: Vec<u64>,
    events_consumed: u64,
    messages_against_reloaded_symbol: u64,
    engine_adds: u64,
    engine_fills: u64,
    engine_resting: u64,
    engine_fully_filled: u64,
    engine_cancels_hit: u64,
    engine_cancels_miss: u64,
    engine_replaces: u64,
    max_ring_used: u64,
    elapsed_cycles: u64,
    evictions: u64,
    fill_constructions_total: u64,
    evict_constructions_total: u64,
    unique_symbols_touched: u64,
    resident_at_end: u64,
}

impl ConsumerStats {
    fn new() -> Self {
        Self {
            wire_samples: Vec::with_capacity(1 << 21),
            apply_samples: Vec::with_capacity(1 << 21),
            end_to_end_samples: Vec::with_capacity(1 << 21),
            apply_by_type: [
                Vec::with_capacity(1 << 20),
                Vec::with_capacity(1 << 20),
                Vec::with_capacity(1 << 19),
                Vec::with_capacity(1 << 19),
                Vec::with_capacity(1 << 18),
                Vec::with_capacity(1 << 18),
            ],
            fill_construction_samples: Vec::with_capacity(1024),
            evict_construction_samples: Vec::with_capacity(1 << 16),
            events_consumed: 0, messages_against_reloaded_symbol: 0,
            engine_adds: 0, engine_fills: 0, engine_resting: 0,
            engine_fully_filled: 0, engine_cancels_hit: 0,
            engine_cancels_miss: 0, engine_replaces: 0,
            max_ring_used: 0, elapsed_cycles: 0,
            evictions: 0, fill_constructions_total: 0,
            evict_constructions_total: 0, unique_symbols_touched: 0,
            resident_at_end: 0,
        }
    }
}

#[inline(always)]
fn hist_bucket(kind: EventKind) -> usize {
    match kind {
        EventKind::AddOrder | EventKind::AddOrderMpid => 0,
        EventKind::OrderExecuted | EventKind::OrderExecutedWithPrice => 1,
        EventKind::OrderCancel => 2,
        EventKind::OrderDelete => 3,
        EventKind::OrderReplace => 4,
        _ => 5,
    }
}

#[inline(always)]
fn prune_filled_resting(
    engine: &MatchingEngine, result: &MatchResult,
    bridge: &mut HashMap<u64, (u32, u16)>,
    resting_by_engine_id: &mut HashMap<u32, u64>,
    engine_fully_filled: &mut u64,
) {
    if result.fill_count == 0 { return; }
    let consumed_id = result.fill.order_id as u32;
    if !engine.order_active(consumed_id) {
        if let Some(itch_id) = resting_by_engine_id.remove(&consumed_id) {
            bridge.remove(&itch_id);
            *engine_fully_filled += 1;
        }
    }
}

#[inline(always)]
fn process_legacy(wire: &WireEvent, lob: &mut LimitOrderBook, stats: &mut ConsumerStats, sample: bool, t_dequeue: u64) {
    let t0 = rdtsc();
    lob.apply_event(&wire.event);
    let t1 = rdtsc();
    if sample {
        stats.wire_samples.push(t_dequeue.wrapping_sub(wire.publish_rdtsc));
        let apply = t1.wrapping_sub(t0);
        stats.apply_samples.push(apply);
        stats.apply_by_type[hist_bucket(wire.event.kind)].push(apply);
        stats.end_to_end_samples.push(t1.wrapping_sub(wire.publish_rdtsc));
    }
    stats.events_consumed += 1;
}

#[inline(always)]
fn process_sharded_nomatch(wire: &WireEvent, table: &mut SymbolTable, stats: &mut ConsumerStats, sample: bool, t_dequeue: u64) {
    let ev = wire.event;
    let t0 = rdtsc();
    let (state, construction) = table.resolve(ev.stock_locate, wire.global_idx);
    if let Some((cycles, was_eviction)) = construction {
        if was_eviction { if sample { stats.evict_construction_samples.push(cycles); } }
        else { stats.fill_construction_samples.push(cycles); }
    }
    if state.is_reload { stats.messages_against_reloaded_symbol += 1; }
    if !state.ticker_known && ev.symbol != [0u8; 8] { state.lob.symbol = ev.symbol; state.ticker_known = true; }
    state.lob.apply_event(&ev);
    let t1 = rdtsc();
    if sample {
        stats.wire_samples.push(t_dequeue.wrapping_sub(wire.publish_rdtsc));
        let apply = t1.wrapping_sub(t0);
        stats.apply_samples.push(apply);
        stats.apply_by_type[hist_bucket(ev.kind)].push(apply);
        stats.end_to_end_samples.push(t1.wrapping_sub(wire.publish_rdtsc));
    }
    stats.events_consumed += 1;
}

#[inline(always)]
fn process_sharded_match(wire: &WireEvent, table: &mut SymbolTable, stats: &mut ConsumerStats, sample: bool, t_dequeue: u64) {
    let ev = wire.event;
    let t0 = rdtsc();
    let (state, construction) = table.resolve(ev.stock_locate, wire.global_idx);
    if let Some((cycles, was_eviction)) = construction {
        if was_eviction { if sample { stats.evict_construction_samples.push(cycles); } }
        else { stats.fill_construction_samples.push(cycles); }
    }
    if state.is_reload { stats.messages_against_reloaded_symbol += 1; }
    if !state.ticker_known && ev.symbol != [0u8; 8] { state.lob.symbol = ev.symbol; state.ticker_known = true; }
    let replace_side = if ev.kind == EventKind::OrderReplace {
        state.lob.order_info(ev.order_id).map(|(_, _, side)| side)
    } else { None };
    state.lob.apply_event(&ev);
    if let Some(engine) = state.engine.as_mut() {
        match ev.kind {
            EventKind::AddOrder | EventKind::AddOrderMpid => {
                let result = engine.match_order(ev.price, ev.shares, ev.side, ev.timestamp_ns);
                stats.engine_adds += 1;
                stats.engine_fills += result.fill_count as u64;
                prune_filled_resting(engine, &result, &mut state.bridge, &mut state.resting_by_engine_id, &mut stats.engine_fully_filled);
                if result.resting_order_id != NULL_IDX {
                    stats.engine_resting += 1;
                    state.bridge.insert(ev.order_id, (result.resting_order_id, result.resting_generation));
                    state.resting_by_engine_id.insert(result.resting_order_id, ev.order_id);
                }
            }
            EventKind::OrderCancel | EventKind::OrderDelete => {
                if let Some((oid, gen)) = state.bridge.remove(&ev.order_id) {
                    state.resting_by_engine_id.remove(&oid);
                    if engine.cancel_order(oid, gen) { stats.engine_cancels_hit += 1; }
                    else { stats.engine_cancels_miss += 1; }
                } else { stats.engine_cancels_miss += 1; }
            }
            EventKind::OrderReplace => {
                if let Some((oid, gen)) = state.bridge.remove(&ev.order_id) {
                    state.resting_by_engine_id.remove(&oid);
                    engine.cancel_order(oid, gen);
                }
                if let Some(side) = replace_side {
                    let result = engine.match_order(ev.price, ev.shares, side, ev.timestamp_ns);
                    stats.engine_replaces += 1;
                    stats.engine_fills += result.fill_count as u64;
                    prune_filled_resting(engine, &result, &mut state.bridge, &mut state.resting_by_engine_id, &mut stats.engine_fully_filled);
                    if result.resting_order_id != NULL_IDX {
                        stats.engine_resting += 1;
                        state.bridge.insert(ev.new_order_id, (result.resting_order_id, result.resting_generation));
                        state.resting_by_engine_id.insert(result.resting_order_id, ev.new_order_id);
                    }
                }
            }
            _ => {}
        }
    }
    let t1 = rdtsc();
    if sample {
        stats.wire_samples.push(t_dequeue.wrapping_sub(wire.publish_rdtsc));
        let apply = t1.wrapping_sub(t0);
        stats.apply_samples.push(apply);
        stats.apply_by_type[hist_bucket(ev.kind)].push(apply);
        stats.end_to_end_samples.push(t1.wrapping_sub(wire.publish_rdtsc));
    }
    stats.events_consumed += 1;
}

fn run_consumer(
    mode: RunMode, sample_mask: u64, symbol_cap: usize,
    mut cons: Consumer<'static, WireEvent>,
    running: Arc<AtomicBool>, ingest_done: Arc<AtomicBool>,
    live: bool,
) -> ConsumerStats {
    let mut stats = ConsumerStats::new();
    let mut single_lob = if mode == RunMode::LegacySingleLob { Some(LimitOrderBook::new(*b"REPLAY  ")) } else { None };
    let mut table = if mode.sharded() { Some(SymbolTable::new(symbol_cap, mode.matching_enabled())) } else { None };

    while !running.load(Ordering::Acquire) && !SHUTDOWN.load(Ordering::Acquire) { std::hint::spin_loop(); }
    while cons.available() == u64::MAX {
        if ingest_done.load(Ordering::Acquire) || SHUTDOWN.load(Ordering::Acquire) { return stats; }
        std::hint::spin_loop();
    }

    let t_start = rdtsc();
    let mut live_counter: u64 = 0;

    loop {
        if SHUTDOWN.load(Ordering::Acquire) { break; }

        let avail = cons.available();
        let local = cons.next_seq();
        if avail >= local {
            let used = avail.wrapping_sub(local).wrapping_add(1);
            if used > stats.max_ring_used { stats.max_ring_used = used; }
        }

        let drained = cons.drain(DRAIN_BATCH, |wire| {
            let sample = wire.global_idx & sample_mask == 0;
            let t_dequeue = rdtsc();
            match mode {
                RunMode::LegacySingleLob => process_legacy(&wire, single_lob.as_mut().unwrap(), &mut stats, sample, t_dequeue),
                RunMode::ShardedNoMatching => process_sharded_nomatch(&wire, table.as_mut().unwrap(), &mut stats, sample, t_dequeue),
                RunMode::Sharded => process_sharded_match(&wire, table.as_mut().unwrap(), &mut stats, sample, t_dequeue),
            }
            if live {
                live_counter += 1;
                if live_counter >= 500_000 {
                    live_counter = 0;
                    let elapsed = (rdtsc() - t_start) as f64 / (GHZ * 1e9);
                    let evict = table.as_ref().map_or(0, |t| t.evictions);
                    eprint!("\r[live] {:.1}M msgs | {:.2}M msg/s | evict: {} | ring: {}/{}   ",
                        stats.events_consumed as f64 / 1e6,
                        if elapsed > 0.0 { stats.events_consumed as f64 / elapsed / 1e6 } else { 0.0 },
                        evict, stats.max_ring_used, RING_CAPACITY);
                }
            }
        });

        if drained == 0 {
            if ingest_done.load(Ordering::Acquire) && !cons.has_next() { break; }
            std::hint::spin_loop();
        }
    }

    stats.elapsed_cycles = rdtsc() - t_start;
    if let Some(t) = table.as_ref() {
        stats.evictions = t.evictions;
        stats.fill_constructions_total = t.fill_constructions;
        stats.evict_constructions_total = t.evict_constructions;
        stats.unique_symbols_touched = t.unique_count;
        stats.resident_at_end = t.states.len() as u64;
    }
    stats
}

fn publish_event(pub_handle: &Publisher<'static, WireEvent>, ev: MarketEvent, published: &mut u64) -> u64 {
    let t_claim_start = rdtsc();
    let seq = pub_handle.claim_spin();
    let t_claim_end = rdtsc();
    let slot = pub_handle.get_mut(seq);
    slot.event = ev;
    slot.global_idx = *published;
    slot.publish_rdtsc = t_claim_end;
    pub_handle.publish(seq);
    *published += 1;
    t_claim_end.wrapping_sub(t_claim_start)
}

fn run_ingest_file(buf: &[u8], pub_handle: Publisher<'static, WireEvent>) -> IngestStats {
    let mut messages_total = 0u64;
    let mut itch_parsed = 0u64;
    let mut unrecognized = 0u64;
    let mut published = 0u64;
    let mut pos = 0usize;
    let len = buf.len();
    let page_mask: usize = !(4096 - 1);
    let mut madvise_pos = 0usize;
    let mut backpressure_ns = 0u64;
    let t_start = rdtsc();

    while pos + 2 <= len && !SHUTDOWN.load(Ordering::Relaxed) {
        let msg_len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
        if msg_len == 0 { pos += 1; continue; }
        let total = 2 + msg_len;
        if pos + total > len { break; }
        messages_total += 1;
        pos += total;
        let mut ev = MarketEvent::default();
        if parse_itch_message(&buf[pos - msg_len..pos], &mut ev) {
            itch_parsed += 1;
            backpressure_ns += publish_event(&pub_handle, ev, &mut published);
        } else { unrecognized += 1; }
        if pos >= madvise_pos + MADVISE_CHUNK {
            let a_s = madvise_pos & page_mask;
            let a_e = pos & page_mask;
            if a_e > a_s {
                // SAFETY: aligned range within mmap, already consumed.
                unsafe { libc::madvise(buf.as_ptr().add(a_s) as *mut libc::c_void, a_e - a_s, libc::MADV_DONTNEED); }
            }
            madvise_pos = pos;
        }
    }
    let a_s = madvise_pos & page_mask;
    let a_e = len & page_mask;
    if a_e > a_s {
        // SAFETY: final flush.
        unsafe { libc::madvise(buf.as_ptr().add(a_s) as *mut libc::c_void, a_e - a_s, libc::MADV_DONTNEED); }
    }
    IngestStats { messages_total, itch_parsed, unrecognized, published, elapsed_cycles: rdtsc() - t_start, ring_backpressure_ns: backpressure_ns, mold_gaps: 0, mold_heartbeats: 0, soup_heartbeats: 0 }
}

fn parse_and_publish(
    buf: &[u8], len: usize,
    pub_handle: &Publisher<'static, WireEvent>,
    published: &mut u64, messages_total: &mut u64,
    itch_parsed: &mut u64, unrecognized: &mut u64,
    backpressure_ns: &mut u64,
) -> usize {
    let mut pos = 0;
    while pos + 2 <= len && !SHUTDOWN.load(Ordering::Relaxed) {
        let msg_len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
        if msg_len == 0 { pos += 1; continue; }
        let total = 2 + msg_len;
        if pos + total > len { break; }
        *messages_total += 1;
        pos += total;
        let mut ev = MarketEvent::default();
        if parse_itch_message(&buf[pos - msg_len..pos], &mut ev) {
            *itch_parsed += 1;
            *backpressure_ns += publish_event(pub_handle, ev, published);
        } else { *unrecognized += 1; }
    }
    pos
}

fn set_busy_poll(fd: i32, usec: u32) {
    // SAFETY: setsockopt with valid fd and SO_BUSY_POLL value.
    unsafe {
        libc::setsockopt(
            fd, libc::SOL_SOCKET, 46,
            &usec as *const u32 as *const libc::c_void,
            std::mem::size_of::<u32>() as libc::socklen_t,
        );
    }
}

fn run_ingest_udp(addr: &str, iface: Option<&str>, raw: bool, pub_handle: Publisher<'static, WireEvent>) -> IngestStats {
    let socket = UdpSocket::bind(addr).expect("failed to bind UDP");
    set_busy_poll(socket.as_raw_fd(), 50);
    if let Some(iface_addr) = iface {
        let group = addr.split(':').next().unwrap();
        // SAFETY: setsockopt with valid fd and IP_ADD_MEMBERSHIP.
        unsafe {
            let mreq = libc::ip_mreq {
                imr_multiaddr: { let p: Vec<u8> = group.split('.').map(|s| s.parse().unwrap()).collect(); libc::in_addr { s_addr: u32::from_ne_bytes([p[0],p[1],p[2],p[3]]) } },
                imr_interface: { let p: Vec<u8> = iface_addr.split('.').map(|s| s.parse().unwrap()).collect(); libc::in_addr { s_addr: u32::from_ne_bytes([p[0],p[1],p[2],p[3]]) } },
            };
            libc::setsockopt(socket.as_raw_fd(), libc::IPPROTO_IP, libc::IP_ADD_MEMBERSHIP, &mreq as *const _ as *const _, std::mem::size_of::<libc::ip_mreq>() as _);
        }
    }
    socket.set_read_timeout(Some(std::time::Duration::from_millis(200))).ok();
    let mut buf = vec![0u8; LIVE_BUF_SIZE];
    let mut messages_total = 0u64;
    let mut itch_parsed = 0u64;
    let mut unrecognized = 0u64;
    let mut published = 0u64;
    let mut backpressure_ns = 0u64;
    let t_start = rdtsc();
    let mut mold = MoldState::new();
    let mut leftover = vec![0u8; 65536];
    let mut leftover_len = 0;

    while !SHUTDOWN.load(Ordering::Acquire) {
        match socket.recv_from(&mut buf) {
            Ok((n, _)) => {
                if raw {
                    if leftover_len > 0 {
                        let copy = n.min(leftover.len() - leftover_len);
                        leftover[leftover_len..leftover_len + copy].copy_from_slice(&buf[..copy]);
                        leftover_len += copy;
                        let c = parse_and_publish(&leftover, leftover_len, &pub_handle, &mut published, &mut messages_total, &mut itch_parsed, &mut unrecognized, &mut backpressure_ns);
                        if c < leftover_len { leftover.copy_within(c..leftover_len, 0); leftover_len -= c; } else { leftover_len = 0; }
                        if copy < n {
                            let rem = &buf[copy..n];
                            let c2 = parse_and_publish(rem, rem.len(), &pub_handle, &mut published, &mut messages_total, &mut itch_parsed, &mut unrecognized, &mut backpressure_ns);
                            let tail = rem.len() - c2;
                            if tail > 0 && tail < leftover.len() { leftover[..tail].copy_from_slice(&rem[c2..]); leftover_len = tail; }
                        }
                    } else {
                        let c = parse_and_publish(&buf, n, &pub_handle, &mut published, &mut messages_total, &mut itch_parsed, &mut unrecognized, &mut backpressure_ns);
                        let tail = n - c;
                        if tail > 0 && tail < leftover.len() { leftover[..tail].copy_from_slice(&buf[c..n]); leftover_len = tail; }
                    }
                } else {
                    if let Some(itch_payload) = mold.process_datagram(&buf[..n]) {
                        parse_and_publish(itch_payload, itch_payload.len(), &pub_handle, &mut published, &mut messages_total, &mut itch_parsed, &mut unrecognized, &mut backpressure_ns);
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => continue,
            Err(_) => break,
        }
    }
    IngestStats { messages_total, itch_parsed, unrecognized, published, elapsed_cycles: rdtsc() - t_start, ring_backpressure_ns: backpressure_ns, mold_gaps: mold.gaps_detected, mold_heartbeats: mold.heartbeats, soup_heartbeats: 0 }
}

fn run_ingest_tcp(addr: &str, raw: bool, pub_handle: Publisher<'static, WireEvent>) -> IngestStats {
    let mut messages_total = 0u64;
    let mut itch_parsed = 0u64;
    let mut unrecognized = 0u64;
    let mut published = 0u64;
    let mut backpressure_ns = 0u64;
    let t_start = rdtsc();
    let mut soup = SoupState::new();

    loop {
        if SHUTDOWN.load(Ordering::Acquire) { break; }
        eprintln!("[live] connecting to TCP {} ...", addr);
        let mut stream = match TcpStream::connect(addr) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[live] TCP connect failed: {}, retrying in 2s...", e);
                std::thread::sleep(std::time::Duration::from_secs(2));
                continue;
            }
        };
        stream.set_read_timeout(Some(std::time::Duration::from_millis(200))).ok();
        stream.set_nodelay(true).ok();
        set_busy_poll(stream.as_raw_fd(), 50);
        eprintln!("[live] TCP connected");

        let mut buf = vec![0u8; LIVE_BUF_SIZE];
        let mut buf_len = 0;

        while !SHUTDOWN.load(Ordering::Acquire) {
            match stream.read(&mut buf[buf_len..]) {
                Ok(0) => {
                    eprintln!("[live] TCP disconnected, reconnecting...");
                    break;
                }
                Ok(n) => {
                    buf_len += n;
                    if raw {
                        let c = parse_and_publish(&buf, buf_len, &pub_handle, &mut published, &mut messages_total, &mut itch_parsed, &mut unrecognized, &mut backpressure_ns);
                        if c < buf_len { buf.copy_within(c..buf_len, 0); buf_len -= c; } else { buf_len = 0; }
                    } else {
                        let mut pos = 0;
                        while pos + 2 <= buf_len {
                            let pkt_len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
                            if pkt_len == 0 { pos += 2; continue; }
                            let total = 2 + pkt_len;
                            if pos + total > buf_len { break; }
                            if let Some(itch_payload) = soup.process_packet(&buf[pos + 2..pos + total]) {
                                parse_and_publish(itch_payload, itch_payload.len(), &pub_handle, &mut published, &mut messages_total, &mut itch_parsed, &mut unrecognized, &mut backpressure_ns);
                            }
                            pos += total;
                        }
                        if pos > 0 { buf.copy_within(pos..buf_len, 0); buf_len -= pos; }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => continue,
                Err(e) => {
                    eprintln!("[live] TCP read error: {}, reconnecting...", e);
                    break;
                }
            }
        }
        if SHUTDOWN.load(Ordering::Acquire) { break; }
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
    IngestStats { messages_total, itch_parsed, unrecognized, published, elapsed_cycles: rdtsc() - t_start, ring_backpressure_ns: backpressure_ns, mold_gaps: 0, mold_heartbeats: 0, soup_heartbeats: soup.heartbeats }
}

fn run_ingest_stdin(pub_handle: Publisher<'static, WireEvent>) -> IngestStats {
    let mut messages_total = 0u64;
    let mut itch_parsed = 0u64;
    let mut unrecognized = 0u64;
    let mut published = 0u64;
    let mut backpressure_ns = 0u64;
    let t_start = rdtsc();
    let mut buf = vec![0u8; LIVE_BUF_SIZE];
    let mut leftover = vec![0u8; 65536];
    let mut leftover_len = 0;
    let stdin = std::io::stdin();
    let mut handle = stdin.lock();

    while !SHUTDOWN.load(Ordering::Acquire) {
        let space = buf.len() - leftover_len;
        if space == 0 { break; }
        match handle.read(&mut buf[leftover_len..leftover_len + space]) {
            Ok(0) => break,
            Ok(n) => {
                let total = leftover_len + n;
                let c = parse_and_publish(&buf, total, &pub_handle, &mut published, &mut messages_total, &mut itch_parsed, &mut unrecognized, &mut backpressure_ns);
                if c < total {
                    let tail = total - c;
                    if tail < leftover.len() { leftover[..tail].copy_from_slice(&buf[c..total]); leftover_len = tail; }
                    else { leftover_len = 0; }
                } else { leftover_len = 0; }
            }
            Err(_) => break,
        }
    }
    IngestStats { messages_total, itch_parsed, unrecognized, published, elapsed_cycles: rdtsc() - t_start, ring_backpressure_ns: backpressure_ns, mold_gaps: 0, mold_heartbeats: 0, soup_heartbeats: 0 }
}

fn pct_idx(len: usize, frac: f64) -> usize { ((len as f64 * frac) as usize).min(len - 1) }

fn percentiles(samples: &mut [u64]) -> (u64, u64, u64, u64, u64) {
    if samples.is_empty() { return (0, 0, 0, 0, 0); }
    samples.sort_unstable();
    let len = samples.len();
    (samples[len / 2], samples[pct_idx(len, 0.99)], samples[pct_idx(len, 0.999)], samples[pct_idx(len, 0.9999)], samples[len - 1])
}

fn print_latency_row(label: &str, samples: &mut Vec<u64>) {
    if samples.is_empty() { println!("  {:<30}: no samples recorded", label); return; }
    let (p50, p99, p999, p9999, max) = percentiles(samples);
    println!("  {:<30}: p50={:.1} p99={:.1} p99.9={:.1} p99.99={:.1} max={:.1} n={}",
        label, p50 as f64 / GHZ, p99 as f64 / GHZ, p999 as f64 / GHZ, p9999 as f64 / GHZ, max as f64 / GHZ, samples.len());
}

fn print_header(path: &str, file_size: u64, mode: RunMode, source: IngestSource, sample_mask: u64, symbol_cap: usize, raw: bool, brief: bool) {
    println!();
    println!("  +=============================================================================+");
    println!("  |          A P E X - W I R E   V 5   I T C H   R E P L A Y                    |");
    println!("  +=============================================================================+");
    println!();
    match source {
        IngestSource::File => {
            println!("  source              : file ({})", path);
            println!("  file size           : {} bytes ({:.1} MB)", file_size, file_size as f64 / (1024.0 * 1024.0));
        }
        IngestSource::LiveUdp => {
            let framing = if raw { "raw ITCH" } else { "MoldUDP64" };
            println!("  source              : live UDP multicast ({}) [{}]", path, framing);
        }
        IngestSource::LiveTcp => {
            let framing = if raw { "raw ITCH" } else { "SoupBinTCP" };
            println!("  source              : live TCP stream ({}) [{}]", path, framing);
        }
        IngestSource::LiveStdin => println!("  source              : live stdin (raw ITCH, pipe from tcpdump/capture)"),
    }
    println!("  run mode            : {}", mode.label());
    if mode.sharded() {
        println!("  symbol cap          : {} resident (flat-index lookup, LRU eviction)", symbol_cap);
    } else {
        println!("  symbol cap          : n/a (single global LimitOrderBook, matching disabled)");
    }
    println!("  latency sampling    : every {}th message (1-in-{})", sample_mask + 1, sample_mask + 1);
    if source != IngestSource::File {
        println!("  SO_BUSY_POLL        : 50us (kernel busy-poll NIC, reduces recv latency)");
        println!("  signal handling     : SIGINT/SIGTERM -> graceful drain + report");
    }
    println!();
    if brief {
        println!("  +=============================================================================+");
        println!();
        return;
    }
    println!("  +----------------------------- CORRECTNESS NOTE ------------------------------+");
    println!("  V4's aw_itch_replay used one LimitOrderBook for every symbol in the file. ITCH");
    println!("  order_reference_number values are unique per session, not per symbol, and");
    println!("  LimitOrderBook::order_map is direct-mapped, indexed by (order_id & mask). Two");
    println!("  different symbols can collide on the same slot in V4, silently corrupting book");
    println!("  state. This is a correctness defect in V4, fixed in V5 by sharding one");
    println!("  LimitOrderBook per stock_locate. --legacy-single-lob intentionally reproduces");
    println!("  the V4 defect, for baseline comparison only, and disables matching to avoid");
    println!("  measuring a second aliasing bug as if it were a feature.");
    println!();
    println!("  V5 feeds AddOrder/AddOrderMpid/OrderReplace into MatchingEngine::match_order().");
    println!("  OrderExecuted/OrderExecutedWithPrice update the LOB only: MatchingEngine's");
    println!("  cancel_order() has no partial-quantity-reduction path for externally tracked");
    println!("  resting orders, so NASDAQ's reported partial fills cannot be replayed against");
    println!("  our engine's own resting orders. The MatchingEngine therefore runs independent");
    println!("  synthetic crossing logic driven by real arrival timing and prices, not a mirror");
    println!("  of NASDAQ's historical trade tape. This is a disclosed modeling simplification.");
    println!("  A resting order fully consumed by this synthetic crossing is detected via");
    println!("  engine.order_active() after match_order() and its ITCH-id bridge entry is pruned");
    println!("  immediately. With that pruning in place, bridge size per symbol is bounded by");
    println!("  the engine's own live-order capacity (65536 slots), not by cumulative order flow.");
    println!();
    println!("  LRU eviction destroys state, it does not preserve it: an evicted symbol's" );
    println!("  LimitOrderBook, MatchingEngine, and order-id bridge are dropped, not snapshotted.");
    println!("  On reload that symbol's book starts empty, not reconstructed from prior flow, so");
    println!("  its BBO/depth is incomplete until fresh orders accumulate, and any Cancel/Delete/");
    println!("  Replace referencing an order that rested pre-eviction will miss the bridge. Both");
    println!("  the eviction count and the messages processed against a reloaded symbol are");
    println!("  reported below, so this effect is measurable, not hidden.");
    if !raw && source == IngestSource::LiveUdp {
        println!();
        println!("  MoldUDP64 gap detection tracks sequence numbers. Any gap count > 0 indicates");
        println!("  dropped multicast packets. Retransmission requests are not implemented; gaps");
        println!("  mean the LOB state for affected symbols may be incomplete.");
    }
    println!("  +-----------------------------------------------------------------------------+");
    println!();
}

const HIST_LABELS: [&str; HIST_BUCKETS] = [
    "apply: AddOrder/Mpid",
    "apply: OrderExecuted",
    "apply: OrderCancel",
    "apply: OrderDelete",
    "apply: OrderReplace",
    "apply: Other",
];

fn print_results(file_size: u64, mode: RunMode, ingest: &IngestStats, cons: &mut ConsumerStats, wall_elapsed: std::time::Duration, symbol_cap: usize) {
    let elapsed_sec = wall_elapsed.as_secs_f64();
    let msg_rate = ingest.messages_total as f64 / elapsed_sec;
    let evt_rate = ingest.itch_parsed as f64 / elapsed_sec;
    println!("  +----------------------------- THROUGHPUT -------------------------------------+");
    println!("  messages_total      : {}", ingest.messages_total);
    println!("  itch_parsed         : {}", ingest.itch_parsed);
    println!("  unrecognized        : {}", ingest.unrecognized);
    println!("  published_to_ring   : {}", ingest.published);
    println!("  events_consumed     : {}", cons.events_consumed);
    println!("  wall elapsed        : {:.3} s", elapsed_sec);
    println!("  msg rate            : {:.2} M msg/s", msg_rate / 1e6);
    println!("  order event rate    : {:.2} M evt/s", evt_rate / 1e6);
    if file_size > 0 {
        println!("  IO throughput       : {:.1} MB/s (mmap, page-cache dependent)", file_size as f64 / (1024.0 * 1024.0) / elapsed_sec);
    }
    println!("  ingest cpu cycles   : {} ({:.3} s @ {:.1} GHz)", ingest.elapsed_cycles, ingest.elapsed_cycles as f64 / (GHZ * 1e9), GHZ);
    println!("  consumer cpu cycles : {} ({:.3} s @ {:.1} GHz)", cons.elapsed_cycles, cons.elapsed_cycles as f64 / (GHZ * 1e9), GHZ);
    if ingest.ring_backpressure_ns > 0 {
        let bp_ms = ingest.ring_backpressure_ns as f64 / GHZ / 1e6;
        println!("  ring backpressure   : {:.1} ms total claim_spin wait (consumer fell behind)", bp_ms);
    }
    if ingest.mold_gaps > 0 || ingest.mold_heartbeats > 0 {
        println!("  MoldUDP64 gaps      : {} (dropped multicast packets)", ingest.mold_gaps);
        println!("  MoldUDP64 heartbeats: {}", ingest.mold_heartbeats);
    }
    if ingest.soup_heartbeats > 0 {
        println!("  SoupBinTCP heartbeats: {}", ingest.soup_heartbeats);
    }
    println!("  NOTE: IO throughput reflects mmap'd sequential access. If the file was already");
    println!("  resident in the page cache from a prior run, this number measures memcpy/parse");
    println!("  speed, not disk IO. Cold-cache pages still pay page-fault + disk read cost.");
    println!();
    println!("  +----------------------------- LATENCY (ns) -----------------------------------+");
    print_latency_row("wire (publish->dequeue)", &mut cons.wire_samples);
    print_latency_row("apply (LOB+matching)", &mut cons.apply_samples);
    print_latency_row("end-to-end (publish->applied)", &mut cons.end_to_end_samples);
    println!();
    println!("  +----------------------------- LATENCY BY TYPE (ns) ---------------------------+");
    for i in 0..HIST_BUCKETS {
        print_latency_row(HIST_LABELS[i], &mut cons.apply_by_type[i]);
    }
    println!();
    if mode.sharded() {
        println!("  +----------------------------- SYMBOL TABLE ------------------------------------+");
        println!("  resident cap            : {}", symbol_cap);
        println!("  resident at end         : {}", cons.resident_at_end);
        println!("  unique symbols touched  : {}", cons.unique_symbols_touched);
        println!("  evictions (LRU)         : {}", cons.evictions);
        println!("  fill constructions      : {} (first-time, no eviction needed)", cons.fill_constructions_total);
        println!("  evict constructions     : {} (required an eviction to make room)", cons.evict_constructions_total);
        println!("  messages vs reloaded sym: {} (processed against a symbol evicted then reconstructed)", cons.messages_against_reloaded_symbol);
        print_latency_row("fill construction", &mut cons.fill_construction_samples);
        print_latency_row("evict construction", &mut cons.evict_construction_samples);
        println!();
    }
    if mode.matching_enabled() {
        println!("  +----------------------------- MATCHING ENGINE ---------------------------------+");
        println!("  engine adds             : {}", cons.engine_adds);
        println!("  engine fills            : {}", cons.engine_fills);
        println!("  engine resting orders   : {}", cons.engine_resting);
        println!("  engine fully filled     : {} (resting order fully consumed -- bridge pruned)", cons.engine_fully_filled);
        println!("  engine replaces         : {}", cons.engine_replaces);
        println!("  engine cancels hit      : {} (bridge found resting order)", cons.engine_cancels_hit);
        println!("  engine cancels miss     : {} (no resting order under that ITCH id)", cons.engine_cancels_miss);
        println!();
    }
    println!("  +----------------------------- RING UTILIZATION --------------------------------+");
    println!("  peak fill           : {} / {} ({:.1}%)", cons.max_ring_used, RING_CAPACITY, cons.max_ring_used as f64 / RING_CAPACITY as f64 * 100.0);
    println!("  +=============================================================================+");
    println!();
}

fn auto_symbol_cap() -> usize {
    let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let avail_kb = meminfo.lines()
        .find(|l| l.starts_with("MemAvailable:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8 * 1024 * 1024);
    let avail_mb = avail_kb / 1024;
    (avail_mb / 12).max(128).min(8192)
}

fn main() {
    install_signal_handlers();
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: aw_itch_replay_v5 <file.bin | --live-udp group:port [iface] | --live-tcp host:port | --live-stdin>");
        eprintln!("       [--legacy-single-lob] [--no-matching] [--raw] [--cap N]");
        eprintln!();
        eprintln!("  file replay:     aw_itch_replay_v5 data.bin");
        eprintln!("  live multicast:  aw_itch_replay_v5 --live-udp 224.0.0.1:20001 10.0.0.5");
        eprintln!("  live TCP:        aw_itch_replay_v5 --live-tcp 192.168.1.100:9001");
        eprintln!("  live pipe:       tcpdump -i eth0 -U -w - | aw_itch_replay_v5 --live-stdin");
        eprintln!();
        eprintln!("  --raw   : skip MoldUDP64/SoupBinTCP framing, expect raw ITCH bytes");
        eprintln!("  --cap N : override adaptive symbol cap (default: auto from /proc/meminfo)");
        eprintln!();
        eprintln!("  Ctrl+C gracefully shuts down and prints the report.");
        std::process::exit(1);
    }

    let legacy_single_lob = args.iter().any(|a| a == "--legacy-single-lob");
    let no_matching = args.iter().any(|a| a == "--no-matching");
    let has_live_udp = args.iter().any(|a| a == "--live-udp");
    let has_live_tcp = args.iter().any(|a| a == "--live-tcp");
    let has_live_stdin = args.iter().any(|a| a == "--live-stdin");
    let raw = args.iter().any(|a| a == "--raw");
    let brief = args.iter().any(|a| a == "--brief");

    let mut cap_override: Option<usize> = None;
    let mut skip_indices = std::collections::HashSet::new();
    for i in 1..args.len() {
        if args[i] == "--cap" && i + 1 < args.len() {
            cap_override = args[i + 1].parse().ok();
            skip_indices.insert(i);
            skip_indices.insert(i + 1);
        }
    }

    let flags: std::collections::HashSet<&str> = [
        "--legacy-single-lob", "--no-matching", "--live-udp",
        "--live-tcp", "--live-stdin", "--raw", "--brief",
    ].into_iter().collect();

    let positional: Vec<&String> = args[1..].iter().enumerate()
        .filter(|(i, a)| !flags.contains(a.as_str()) && !skip_indices.contains(&(i + 1)))
        .map(|(_, a)| a)
        .collect();

    let source;
    let source_arg;
    let live_iface: Option<String>;

    if has_live_udp {
        source = IngestSource::LiveUdp;
        source_arg = positional.first().map(|s| s.to_string()).unwrap_or_default();
        if source_arg.is_empty() { eprintln!("[error] --live-udp requires group:port"); std::process::exit(1); }
        live_iface = positional.get(1).map(|s| s.to_string());
    } else if has_live_tcp {
        source = IngestSource::LiveTcp;
        source_arg = positional.first().map(|s| s.to_string()).unwrap_or_default();
        if source_arg.is_empty() { eprintln!("[error] --live-tcp requires host:port"); std::process::exit(1); }
        live_iface = None;
    } else if has_live_stdin {
        source = IngestSource::LiveStdin;
        source_arg = String::from("stdin");
        live_iface = None;
    } else {
        source = IngestSource::File;
        source_arg = positional.first().map(|s| s.to_string()).unwrap_or_default();
        if source_arg.is_empty() { eprintln!("[error] no file path provided"); std::process::exit(1); }
        live_iface = None;
    }

    if legacy_single_lob && no_matching {
        eprintln!("[error] --legacy-single-lob and --no-matching are mutually exclusive");
        std::process::exit(1);
    }
    let mode = if legacy_single_lob { RunMode::LegacySingleLob }
        else if no_matching { RunMode::ShardedNoMatching }
        else { RunMode::Sharded };

    let symbol_cap = if mode.sharded() {
        cap_override.unwrap_or_else(auto_symbol_cap)
    } else { 0 };

    let live = source != IngestSource::File;
    let sample_mask: u64 = 15;

    let disruptor: &'static Disruptor<WireEvent> = Box::leak(Box::new(
        Disruptor::<WireEvent>::new(RING_CAPACITY).expect("failed to allocate disruptor ring"),
    ));
    let pub_handle = disruptor.publisher();
    let cons_handle = disruptor.consumer();

    let running = Arc::new(AtomicBool::new(false));
    let ingest_done = Arc::new(AtomicBool::new(false));
    let running_c = running.clone();
    let ingest_done_c = ingest_done.clone();

    let cons_thread = std::thread::spawn(move || {
        set_affinity(3);
        run_consumer(mode, sample_mask, symbol_cap, cons_handle, running_c, ingest_done_c, live)
    });

    set_affinity(2);
    running.store(true, Ordering::Release);
    let wall_start = Instant::now();

    let (ingest_stats, file_size) = match source {
        IngestSource::File => {
            let mapped: &'static MappedFile = Box::leak(Box::new(match MappedFile::open(&source_arg) {
                Ok(m) => m,
                Err(e) => { eprintln!("[error] mmap failed: {}", e); std::process::exit(2); }
            }));
            let fs = mapped.len as u64;
            print_header(&source_arg, fs, mode, source, sample_mask, symbol_cap, raw, brief);
            (run_ingest_file(mapped.as_slice(), pub_handle), fs)
        }
        IngestSource::LiveUdp => {
            print_header(&source_arg, 0, mode, source, sample_mask, symbol_cap, raw, brief);
            let framing = if raw { "raw ITCH" } else { "MoldUDP64" };
            eprintln!("[live] listening on UDP {} [{}] (Ctrl+C to stop)...", source_arg, framing);
            (run_ingest_udp(&source_arg, live_iface.as_deref(), raw, pub_handle), 0)
        }
        IngestSource::LiveTcp => {
            print_header(&source_arg, 0, mode, source, sample_mask, symbol_cap, raw, brief);
            let framing = if raw { "raw ITCH" } else { "SoupBinTCP" };
            eprintln!("[live] connecting to TCP {} [{}]...", source_arg, framing);
            (run_ingest_tcp(&source_arg, raw, pub_handle), 0)
        }
        IngestSource::LiveStdin => {
            print_header("stdin", 0, mode, source, sample_mask, symbol_cap, raw, brief);
            eprintln!("[live] reading ITCH from stdin (Ctrl+C or EOF to stop)...");
            (run_ingest_stdin(pub_handle), 0)
        }
    };

    ingest_done.store(true, Ordering::Release);
    let mut consumer_stats = cons_thread.join().unwrap();
    let wall_elapsed = wall_start.elapsed();
    if live { eprintln!(); }
    print_results(file_size, mode, &ingest_stats, &mut consumer_stats, wall_elapsed, symbol_cap);
}
