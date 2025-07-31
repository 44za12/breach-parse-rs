use aho_corasick::{AhoCorasick, AhoCorasickBuilder};
use bstr::ByteSlice;
use clap::{App, Arg};
use flate2::read::GzDecoder;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs::File;
use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;
use walkdir::WalkDir;
use flate2::read::MultiGzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use zstd::stream::read::Decoder;
use num_cpus;
use memmap2::Mmap;
use std::sync::Arc;

#[derive(Debug)]
struct Config {
    keyword: String,
    keyword2: Option<String>,
    output_file: String,
    breach_data_location: String,
    email: Option<String>,
}

fn parse_arguments() -> Config {
    let matches = App::new("Breach-Parse: A Parsing Tool To Quickly Search Through Breach Data")
        .version("1.0")
        .author("Aazar")
        .about("Searches through breach data efficiently")
        .arg(Arg::new("keyword")
            .short('k')
            .long("keyword")
            .takes_value(true)
            .required_unless_present("email")
            .help("Primary keyword to search for"))
        .arg(Arg::new("second_keyword")
            .short('s')
            .long("second_keyword")
            .takes_value(true)
            .help("Secondary keyword to search for"))
        .arg(Arg::new("output_file")
            .short('o')
            .long("output_file")
            .takes_value(true)
            .default_value("print")
            .help("File to output results or 'print' to output to console"))
        .arg(Arg::new("email")
            .takes_value(true)
            .help("Email to search for directly"))
        .arg(Arg::new("breach_data_location")
            .long("breach_data_location")
            .takes_value(true)
            .default_value("data.tmp")
            .help("Location of breach data"))
        .get_matches();

    let config = Config {
        keyword: matches.value_of("keyword").unwrap_or_default().to_string(),
        keyword2: matches.value_of("second_keyword").map(|s| s.to_string()),
        output_file: matches.value_of("output_file").unwrap_or("print").to_string(),
        breach_data_location: matches.value_of("breach_data_location").unwrap().to_string(),
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

fn process_file(path: &Path, ac: &AhoCorasick) -> Vec<String> {
    // Try memory mapping first for better performance
    if let Ok(file) = File::open(path) {
        if let Ok(metadata) = file.metadata() {
            let file_size = metadata.len();
            
            // Use memory mapping for files larger than 1MB
            if file_size > 1024 * 1024 {
                if let Ok(mmap) = unsafe { Mmap::map(&file) } {
                    let mmap_arc = Arc::new(mmap);
                    
                    // Process in chunks for very large files
                    if file_size > 100 * 1024 * 1024 { // 100MB+
                        return process_large_file_chunked(&mmap_arc, ac);
                    } else {
                        return process_memory_mapped_file(&mmap_arc, ac, path);
                    }
                }
            }
            
            // Get optimal buffer size before creating the reader
            let buffer_size = optimal_buffer_size(&file);
            
            // Fall back to regular file reading
            let mut reader: Box<dyn Read> = if path.extension().and_then(|s| s.to_str()) == Some("gz") {
                Box::new(GzDecoder::new(file))
            } else if path.extension().and_then(|s| s.to_str()) == Some("zst") {
                // Use multi-threaded zstd decoder
                match Decoder::new(file) {
                    Ok(decoder) => Box::new(decoder),
                    Err(_) => return Vec::new(),
                }
            } else {
                Box::new(file)
            };

            // Use optimized buffer size based on file metadata
            let mut buffer = Vec::with_capacity(buffer_size);
            if reader.read_to_end(&mut buffer).is_err() {
                return Vec::new();
            }

            return buffer
                .par_split(|&b| b == b'\n')
                .filter(|line| !line.is_empty())
                .filter(|line| {
                    let matches: HashSet<usize> = ac.find_iter(line).map(|m| m.pattern()).collect();
                    matches.len() == ac.pattern_count()
                })
                .map(|line| String::from_utf8_lossy(line).into_owned())
                .collect();
        }
    }
    
    // If we get here, we couldn't get file metadata
    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return Vec::new(),
    };

    let mut reader: Box<dyn Read> = if path.extension().and_then(|s| s.to_str()) == Some("gz") {
        Box::new(GzDecoder::new(file))
    } else if path.extension().and_then(|s| s.to_str()) == Some("zst") {
        // Use multi-threaded zstd decoder
        match Decoder::new(file) {
            Ok(decoder) => Box::new(decoder),
            Err(_) => return Vec::new(),
        }
    } else {
        Box::new(file)
    };

    // Use default buffer size
    let mut buffer = Vec::with_capacity(64 * 1024);
    if reader.read_to_end(&mut buffer).is_err() {
        return Vec::new();
    }

    buffer
        .par_split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .filter(|line| {
            let matches: HashSet<usize> = ac.find_iter(line).map(|m| m.pattern()).collect();
            matches.len() == ac.pattern_count()
        })
        .map(|line| String::from_utf8_lossy(line).into_owned())
        .collect()
}

fn process_memory_mapped_file(mmap: &Arc<Mmap>, ac: &AhoCorasick, path: &Path) -> Vec<String> {
    // For compressed files, we need to use a cursor to make the mmap readable
    use std::io::Cursor;
    
    if path.extension().and_then(|s| s.to_str()) == Some("zst") {
        // Handle zstd compressed memory-mapped files
        let cursor = Cursor::new(&**mmap);
        match Decoder::new(cursor) {
            Ok(mut decoder) => {
                let mut buffer = Vec::with_capacity(mmap.len() * 3); // zstd typically compresses 3:1
                if decoder.read_to_end(&mut buffer).is_err() {
                    return Vec::new();
                }
                return buffer
                    .par_split(|&b| b == b'\n')
                    .filter(|line| !line.is_empty())
                    .filter(|line| {
                        let matches: HashSet<usize> = ac.find_iter(line).map(|m| m.pattern()).collect();
                        matches.len() == ac.pattern_count()
                    })
                    .map(|line| String::from_utf8_lossy(line).into_owned())
                    .collect();
            },
            Err(_) => return Vec::new(),
        }
    } else if path.extension().and_then(|s| s.to_str()) == Some("gz") {
        // Handle gzip compressed memory-mapped files
        let cursor = Cursor::new(&**mmap);
        let mut decoder = GzDecoder::new(cursor);
        let mut buffer = Vec::with_capacity(mmap.len() * 3); // gzip typically compresses 3:1
        if decoder.read_to_end(&mut buffer).is_err() {
            return Vec::new();
        }
        return buffer
            .par_split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
            .filter(|line| {
                let matches: HashSet<usize> = ac.find_iter(line).map(|m| m.pattern()).collect();
                matches.len() == ac.pattern_count()
            })
            .map(|line| String::from_utf8_lossy(line).into_owned())
            .collect();
    } else {
        // Uncompressed file - use the mmap directly
        mmap
            .par_split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
            .filter(|line| {
                let matches: HashSet<usize> = ac.find_iter(line).map(|m| m.pattern()).collect();
                matches.len() == ac.pattern_count()
            })
            .map(|line| String::from_utf8_lossy(line).into_owned())
            .collect()
    }
}

fn process_large_file_chunked(mmap: &Arc<Mmap>, ac: &AhoCorasick) -> Vec<String> {
    let chunk_size = 50 * 1024 * 1024; // 50MB chunks
    let mmap_clone = Arc::clone(mmap);
    
    (0..mmap.len())
        .step_by(chunk_size)
        .par_bridge()
        .flat_map(|start| {
            let end = std::cmp::min(start + chunk_size, mmap_clone.len());
            let chunk = &mmap_clone[start..end];
            
            // Find the last newline to avoid splitting lines
            let adjusted_end = if end < mmap_clone.len() {
                chunk.iter().rposition(|&b| b == b'\n').map(|pos| start + pos + 1).unwrap_or(end)
            } else {
                end
            };
            
            let adjusted_chunk = &mmap_clone[start..adjusted_end];
            
            adjusted_chunk
                .par_split(|&b| b == b'\n')
                .filter(|line| !line.is_empty())
                .filter(|line| {
                    let matches: HashSet<usize> = ac.find_iter(line).map(|m| m.pattern()).collect();
                    matches.len() == ac.pattern_count()
                })
                .map(|line| String::from_utf8_lossy(line).into_owned())
                .collect::<Vec<String>>()
        })
        .collect()
}

fn optimal_buffer_size(file: &File) -> usize {
    if let Ok(metadata) = file.metadata() {
        let file_size = metadata.len();
        
        // Optimal buffer size based on file size
        if file_size < 64 * 1024 { // < 64KB
            8 * 1024 // 8KB buffer
        } else if file_size < 1024 * 1024 { // < 1MB
            64 * 1024 // 64KB buffer
        } else if file_size < 100 * 1024 * 1024 { // < 100MB
            512 * 1024 // 512KB buffer
        } else {
            1024 * 1024 // 1MB buffer for very large files
        }
    } else {
        64 * 1024 // Default 64KB buffer
    }
}

fn main() -> io::Result<()> {
    // Configure Rayon thread pool to use optimal number of threads
    // Leave 1-2 cores for the system to remain responsive
    let total_cores = num_cpus::get();
    let optimal_threads = std::cmp::max(1, total_cores.saturating_sub(2));
    
    // Initialize Rayon thread pool with optimal thread count
    rayon::ThreadPoolBuilder::new()
        .num_threads(optimal_threads)
        .thread_name(|idx| format!("breach-parser-{}", idx))
        .build_global()
        .expect("Failed to initialize Rayon thread pool");
    
    println!("Using {} out of {} available CPU cores", optimal_threads, total_cores);

    let config = parse_arguments();

    if !Path::new(&config.breach_data_location).is_dir() {
        println!("Could not find a directory at {}", config.breach_data_location);
        std::process::exit(1);
    }

    if let Some(email) = config.email {
        let results = process_email(&email, &config.breach_data_location);
        for line in results {
            println!("{}", line);
        }
        return Ok(())
    }

    let mut patterns = vec![config.keyword];
    if let Some(keyword2) = config.keyword2 {
        patterns.push(keyword2);
    }
    let ac = AhoCorasickBuilder::new().dfa(true).build(&patterns);

    // Collect all files first to get the total count for the progress bar
    let files: Vec<_> = WalkDir::new(&config.breach_data_location)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .collect();

    let total_files = files.len();
    let pb = ProgressBar::new(total_files as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .expect("Failed to set progress bar template")
        .progress_chars("#>-"));

    let results: Vec<String> = files
        .into_par_iter()
        .map(|entry| {
            let result = process_file(entry.path(), &ac);
            pb.inc(1);
            result
        })
        .flat_map(|result| result)
        .collect();

    pb.finish_with_message("Search completed");

    match config.output_file.as_ref() {
        "print" => {
            println!("\nResults:\n");
            for line in results {
                println!("{}", line);
            }
        },
        _ => {
            let mut file = File::create(&config.output_file)?;
            for line in results {
                writeln!(file, "{}", line)?;
            }
            println!("Results written to {}", config.output_file);
        },
    }

    Ok(())
}