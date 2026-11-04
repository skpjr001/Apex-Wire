use aw_exchange::itch::parse_itch_message;
use aw_exchange::lob::LimitOrderBook;
use aw_exchange::order::MarketEvent;
use std::env;
use std::fs::File;
use std::os::unix::io::AsRawFd;

#[inline(always)]
fn rdtsc() -> u64 {
    // SAFETY: _rdtsc is always available on x86_64.
    unsafe { core::arch::x86_64::_rdtsc() }
}

fn rss_kb() -> u64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    s.split_whitespace()
        .nth(1)
        .and_then(|w| w.parse::<u64>().ok())
        .unwrap_or(0)
        * 4
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
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                fd,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: ptr and len describe the mapping created above.
        unsafe {
            libc::madvise(ptr, len, libc::MADV_SEQUENTIAL);
        }
        Ok(Self {
            ptr: ptr as *const u8,
            len,
        })
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid for len bytes.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for MappedFile {
    fn drop(&mut self) {
        // SAFETY: ptr/len describe the mapping created in open().
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.len);
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: aw_rss_diag <file.bin> [max_bytes]");
        std::process::exit(1);
    }
    let path = &args[1];
    let max_bytes: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(usize::MAX);

    let mapped = match MappedFile::open(path) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("[error] mmap failed: {}", e);
            std::process::exit(2);
        }
    };

    let buf = mapped.as_slice();
    let len = buf.len().min(max_bytes);
    println!("file={} total_bytes={} scan_bytes={}", path, buf.len(), len);
    println!("initial RSS: {} KB", rss_kb());

    let mut lob = LimitOrderBook::new(*b"REPLAY  ");
    let mut wire_samples: Vec<u64> = Vec::with_capacity(1 << 20);
    let mut apply_samples: Vec<u64> = Vec::with_capacity(1 << 20);
    let mut e2e_samples: Vec<u64> = Vec::with_capacity(1 << 20);

    let mut pos: usize = 0;
    let mut messages_total: u64 = 0;
    let mut itch_parsed: u64 = 0;

    while pos + 2 <= len {
        let msg_len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
        if msg_len == 0 {
            pos += 1;
            continue;
        }
        let total = 2 + msg_len;
        if pos + total > len {
            break;
        }
        let msg = &buf[pos + 2..pos + total];
        messages_total += 1;
        pos += total;

        let mut ev = MarketEvent::default();
        if parse_itch_message(msg, &mut ev) {
            itch_parsed += 1;
            let t0 = rdtsc();
            lob.apply_event(&ev);
            let t1 = rdtsc();
            wire_samples.push(t0);
            apply_samples.push(t1.wrapping_sub(t0));
            e2e_samples.push(t1);
        }

        if messages_total % 5_000_000 == 0 {
            println!(
                "  msg={}M parsed={}M RSS={}KB wire_cap={}M apply_cap={}M e2e_cap={}M",
                messages_total / 1_000_000,
                itch_parsed / 1_000_000,
                rss_kb(),
                wire_samples.capacity() / 1_000_000,
                apply_samples.capacity() / 1_000_000,
                e2e_samples.capacity() / 1_000_000,
            );
        }
    }

    println!();
    println!(
        "final: msg={} parsed={} RSS={}KB",
        messages_total,
        itch_parsed,
        rss_kb()
    );
    println!(
        "  wire_cap={} apply_cap={} e2e_cap={}",
        wire_samples.capacity(),
        apply_samples.capacity(),
        e2e_samples.capacity()
    );
}
