use std::env;
use std::fs::File;
use std::io::Read;
use std::collections::HashSet;
use std::process;
use std::time::Instant;

use aw_exchange::itch::parse_itch_message;
use aw_exchange::order::MarketEvent;

const USAGE: &str = "usage: aw_itch_symbol_count <file.bin>";

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("{}", USAGE);
        process::exit(1);
    }
    let path = &args[1];

    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("[error] cannot open file: {}", e);
            process::exit(2);
        }
    };

    let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);

    println!();
    println!("  +=============================================================================+");
    println!("  |          A P E X - W I R E   V 5   S Y M B O L   C O U N T                  |");
    println!("  |   Counts distinct stock_locate values across an ITCH 5.0 binary file        |");
    println!("  +=============================================================================+");
    println!();
    println!("  file              : {}", path);
    println!("  file size         : {} bytes ({:.1} MB)", file_size, file_size as f64 / (1024.0 * 1024.0));
    println!();

    let mut symbols: HashSet<u16> = HashSet::new();
    let mut messages_total: u64 = 0;
    let mut itch_parsed: u64 = 0;
    let mut buf = vec![0u8; 1 << 20];
    let mut pos: usize = 0;
    let mut valid: usize = 0;
    let mut bytes_read: u64 = 0;

    let start = Instant::now();

    loop {
        if valid - pos < 2 {
            if pos > 0 {
                buf.copy_within(pos..valid, 0);
                valid -= pos;
                pos = 0;
            }
            match file.read(&mut buf[valid..]) {
                Ok(0) => break,
                Ok(n) => {
                    valid += n;
                    bytes_read += n as u64;
                }
                Err(e) => {
                    eprintln!("[error] read failed: {}", e);
                    break;
                }
            }
        }

        if valid - pos < 2 {
            break;
        }

        let msg_len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
        if msg_len == 0 || msg_len > 200 {
            pos += 1;
            continue;
        }

        let total = 2 + msg_len;
        if valid - pos < total {
            if pos > 0 {
                buf.copy_within(pos..valid, 0);
                valid -= pos;
                pos = 0;
            }
            if buf.len() < total {
                buf.resize(total * 2, 0);
            }
            match file.read(&mut buf[valid..]) {
                Ok(0) => break,
                Ok(n) => {
                    valid += n;
                    bytes_read += n as u64;
                }
                Err(_) => break,
            }
            if valid < total {
                break;
            }
        }

        let msg = &buf[pos + 2..pos + total];
        messages_total += 1;
        pos += total;

        let mut ev = MarketEvent::default();
        if parse_itch_message(msg, &mut ev) {
            itch_parsed += 1;
            symbols.insert(ev.stock_locate);
        }
    }

    let elapsed = start.elapsed();

    println!("  +----------------------------- RESULTS --------------------------------------+");
    println!("  bytes_read          : {} ({:.1} MB)", bytes_read, bytes_read as f64 / (1024.0 * 1024.0));
    println!("  messages_total      : {}", messages_total);
    println!("  itch_parsed         : {} (order events + system/trading-action)", itch_parsed);
    println!("  distinct symbols    : {} (by stock_locate)", symbols.len());
    println!("  elapsed             : {:.3} s", elapsed.as_secs_f64());
    println!("  +=============================================================================+");
    println!();
}
