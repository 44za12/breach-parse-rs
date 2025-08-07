use bstr::ByteSlice;
use clap::{App, Arg};
use flate2::read::GzDecoder;
use flate2::read::MultiGzDecoder;
use rayon::prelude::*;
use std::fs;
use std::fs::File;
use std::io::{self, BufWriter, Read, Write};
use std::path::Path;
use walkdir::WalkDir;

use crossbeam_channel::{bounded, Sender};
use memchr::memmem::Finder;
use memchr::{memchr, memrchr};
use num_cpus;
use zstd::stream::read::Decoder;

#[derive(Debug)]
struct Config {
    keyword: String,
    output_file: String,
    breach_data_location: String,
    email: Option<String>,
}

fn parse_arguments() -> Config {
    let matches = App::new("Breach-Parse: A Parsing Tool To Quickly Search Through Breach Data")
        .version("1.0")
        .author("Aazar")
        .about("Searches through breach data efficiently")
        .arg(
            Arg::new("keyword")
                .short('k')
                .long("keyword")
                .takes_value(true)
                .required_unless_present("email")
                .help("Primary keyword to search for"),
        )
        .arg(
            Arg::new("second_keyword")
                .short('s')
                .long("second_keyword")
                .takes_value(true)
                .help("Secondary keyword to search for"),
        )
        .arg(
            Arg::new("output_file")
                .short('o')
                .long("output_file")
                .takes_value(true)
                .default_value("print")
                .help("File to output results or 'print' to output to console"),
        )
        .arg(
            Arg::new("email")
                .takes_value(true)
                .help("Email to search for directly"),
        )
        .arg(
            Arg::new("breach_data_location")
                .long("breach_data_location")
                .takes_value(true)
                .default_value("data.tmp")
                .help("Location of breach data"),
        )
        .get_matches();

    let config = Config {
        keyword: matches.value_of("keyword").unwrap_or_default().to_string(),
        output_file: matches
            .value_of("output_file")
            .unwrap_or("print")
            .to_string(),
        breach_data_location: matches
            .value_of("breach_data_location")
            .unwrap()
            .to_string(),
        email: matches.value_of("email").map(|s| s.to_string()),
    };

    config
}

fn process_email(keyword: &str, base_dir: &str) -> Vec<String> {
    let keyword_lower = keyword.to_lowercase();
    let chars: Vec<char> = keyword_lower.chars().collect();
    let mut path = format!("{}", base_dir);
    for (i, c) in chars.iter().enumerate().take(3) {
        path.push('/');
        if c.is_alphanumeric() {
            path.push(*c);
        } else {
            path.push_str("symbols");
            break;
        }

        if i < 2 {
            let gz_path = format!("{}.gz", path);
            let zst_path = format!("{}.zst", path);
            let txt_path = format!("{}.txt", path);
            if fs::metadata(&gz_path).is_ok() {
                path = gz_path;
                break;
            } else if fs::metadata(&zst_path).is_ok() {
                path = zst_path;
                break;
            } else if fs::metadata(&txt_path).is_ok() {
                path = txt_path;
                break;
            }
        } else {
            if fs::metadata(&format!("{}.gz", path)).is_ok() {
                path.push_str(".gz");
            } else if fs::metadata(&format!("{}.zst", path)).is_ok() {
                path.push_str(".zst");
            } else {
                path.push_str(".txt");
            }
        }
    }

    let file = match File::open(&path) {
        Ok(file) => file,
        Err(_) => return Vec::new(),
    };

    let mut reader: Box<dyn Read> = if path.ends_with(".gz") {
        Box::new(MultiGzDecoder::new(file))
    } else if path.ends_with(".zst") {
        match Decoder::new(file) {
            Ok(decoder) => Box::new(decoder),
            Err(_) => return Vec::new(),
        }
    } else {
        Box::new(file)
    };

    let mut buffer = Vec::new();
    if reader.read_to_end(&mut buffer).is_err() {
        return Vec::new();
    }

    buffer
        .lines()
        .par_bridge()
        .map(|line| line.to_str_lossy().into_owned())
        .filter(|line| line.to_lowercase().starts_with(&keyword_lower))
        .collect()
}

fn optimal_buffer_size(file: &File) -> usize {
    if let Ok(metadata) = file.metadata() {
        let file_size = metadata.len();

        if file_size < 64 * 1024 {
            8 * 1024
        } else if file_size < 1024 * 1024 {
            64 * 1024
        } else if file_size < 100 * 1024 * 1024 {
            512 * 1024
        } else {
            1024 * 1024
        }
    } else {
        64 * 1024
    }
}

#[inline]
fn process_chunk_bytes_seq_parallel(
    chunk: &[u8],
    finder: &Finder,
    tx: &Sender<Vec<u8>>,
    stripe_target_bytes: usize,
    stripe_headroom_bytes: usize,
) {
    if chunk.is_empty() {
        return;
    }
    let target = stripe_target_bytes;
    let headroom = stripe_headroom_bytes;
    let mut stripes: Vec<(usize, usize)> = Vec::with_capacity((chunk.len() / target.max(1)) + 1);
    let mut start = 0usize;
    while start < chunk.len() {
        let mut end = (start + target).min(chunk.len());
        if end < chunk.len() {
            let search_end = (end + headroom).min(chunk.len());
            if let Some(off) = memchr(b'\n', &chunk[end..search_end]) {
                end += off + 1;
            } else {
                end = chunk.len();
            }
        }
        stripes.push((start, end));
        start = end;
    }

    stripes
        .into_par_iter()
        .map(|(s, e)| {
            let mut out = Vec::with_capacity(128 * 1024);
            let mut slice_start = s;
            let mut last_emitted_start = usize::MAX;
            let mut last_emitted_end = s;
            while slice_start < e {
                match finder.find(&chunk[slice_start..e]) {
                    Some(rel) => {
                        let abs = slice_start + rel;
                        let line_start = memrchr(b'\n', &chunk[last_emitted_end..abs])
                            .map(|i| last_emitted_end + i + 1)
                            .unwrap_or(last_emitted_end);
                        let line_end = memchr(b'\n', &chunk[abs..e]).map(|i| abs + i).unwrap_or(e);
                        if !(line_start == last_emitted_start && line_end == last_emitted_end) {
                            out.extend_from_slice(&chunk[line_start..line_end]);
                            out.push(b'\n');
                            last_emitted_start = line_start;
                            last_emitted_end = line_end;
                        }
                        slice_start = line_end.saturating_add(1);
                    }
                    None => break,
                }
            }
            out
        })
        .filter(|out| !out.is_empty())
        .for_each(|out| {
            let _ = tx.send(out);
        });
}

fn process_file_stream(path: &Path, needle: &[u8], tx: &Sender<Vec<u8>>) {
    let _finder = Finder::new(needle);

    if let Ok(file) = File::open(path) {
        if let Ok(metadata) = file.metadata() {
            let _file_size = metadata.len();

            let buffer_size = optimal_buffer_size(&file);
            let mut reader: Box<dyn Read> =
                if path.extension().and_then(|s| s.to_str()) == Some("gz") {
                    Box::new(GzDecoder::new(file))
                } else if path.extension().and_then(|s| s.to_str()) == Some("zst") {
                    match Decoder::new(file) {
                        Ok(decoder) => Box::new(decoder),
                        Err(_) => return,
                    }
                } else {
                    Box::new(file)
                };

            let finder = Finder::new(needle);
            let mut carry: Vec<u8> = Vec::with_capacity(128 * 1024);
            let mut buf = vec![0u8; buffer_size.max(8 * 1024 * 1024)];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        let mut chunk = std::mem::take(&mut carry);
                        chunk.extend_from_slice(&buf[..n]);
                        if let Some(pos) = chunk.iter().rposition(|&b| b == b'\n') {
                            let (process_bytes, rest) = chunk.split_at(pos + 1);
                            process_chunk_bytes_seq_parallel(
                                process_bytes,
                                &finder,
                                tx,
                                4 * 1024 * 1024,
                                1 * 1024 * 1024,
                            );
                            carry = rest.to_vec();
                        } else {
                            carry = chunk;
                        }
                    }
                    Err(_) => break,
                }
            }
            if !carry.is_empty() {
                process_chunk_bytes_seq_parallel(
                    &carry,
                    &finder,
                    tx,
                    4 * 1024 * 1024,
                    1 * 1024 * 1024,
                );
            }
            return;
        }
    }

    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return,
    };
    let mut reader: Box<dyn Read> = if path.extension().and_then(|s| s.to_str()) == Some("gz") {
        Box::new(GzDecoder::new(file))
    } else if path.extension().and_then(|s| s.to_str()) == Some("zst") {
        match Decoder::new(file) {
            Ok(decoder) => Box::new(decoder),
            Err(_) => return,
        }
    } else {
        Box::new(file)
    };

    let finder = Finder::new(needle);
    let mut carry: Vec<u8> = Vec::with_capacity(128 * 1024);
    let mut buf = vec![0u8; 8 * 1024 * 1024];
    loop {
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                let mut chunk = std::mem::take(&mut carry);
                chunk.extend_from_slice(&buf[..n]);
                if let Some(pos) = chunk.iter().rposition(|&b| b == b'\n') {
                    let (process_bytes, rest) = chunk.split_at(pos + 1);
                    process_chunk_bytes_seq_parallel(
                        process_bytes,
                        &finder,
                        tx,
                        4 * 1024 * 1024,
                        1 * 1024 * 1024,
                    );
                    carry = rest.to_vec();
                } else {
                    carry = chunk;
                }
            }
            Err(_) => break,
        }
    }
    if !carry.is_empty() {
        process_chunk_bytes_seq_parallel(&carry, &finder, tx, 4 * 1024 * 1024, 1 * 1024 * 1024);
    }
}
fn main() -> io::Result<()> {
    let total_cores = num_cpus::get();
    let optimal_threads = match std::env::var("RAYON_NUM_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
    {
        Some(n) if n > 0 => n,
        _ => total_cores.max(1),
    };

    rayon::ThreadPoolBuilder::new()
        .num_threads(optimal_threads)
        .thread_name(|idx| format!("breach-parser-{}", idx))
        .build_global()
        .expect("Failed to initialize Rayon thread pool");

    println!(
        "Using {} out of {} available CPU cores",
        optimal_threads, total_cores
    );

    let config = parse_arguments();

    if !Path::new(&config.breach_data_location).is_dir() {
        println!(
            "Could not find a directory at {}",
            config.breach_data_location
        );
        std::process::exit(1);
    }

    if let Some(email) = config.email {
        let results = process_email(&email, &config.breach_data_location);
        for line in results {
            println!("{}", line);
        }
        return Ok(());
    }
    let needle = config.keyword.into_bytes();
    let walker = WalkDir::new(&config.breach_data_location)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let p = e.path();
            p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("zst")
        });
    let (tx, rx) = bounded::<Vec<u8>>(65536);

    let output_mode = config.output_file.clone();
    let writer_handle = std::thread::spawn(move || {
        if output_mode == "print" {
            let stdout = io::stdout();
            let mut handle = BufWriter::with_capacity(8 * 1024 * 1024, stdout.lock());
            while let Ok(line) = rx.recv() {
                let _ = handle.write_all(&line);
            }
            let _ = handle.flush();
        } else {
            if let Ok(file) = File::create(&output_mode) {
                let mut handle = BufWriter::with_capacity(8 * 1024 * 1024, file);
                while let Ok(line) = rx.recv() {
                    let _ = handle.write_all(&line);
                }
                let _ = handle.flush();
                println!("Results written to {}", output_mode);
            } else {
                let stdout = io::stdout();
                let mut handle = BufWriter::with_capacity(8 * 1024 * 1024, stdout.lock());
                while let Ok(line) = rx.recv() {
                    let _ = handle.write_all(&line);
                }
                let _ = handle.flush();
            }
        }
    });

    walker.par_bridge().for_each_with(tx, |s, entry| {
        process_file_stream(entry.path(), &needle, s);
    });
    let _ = writer_handle.join();

    Ok(())
}
