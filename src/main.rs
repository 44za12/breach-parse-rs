use aho_corasick::AhoCorasick;
use clap::{App, Arg};
use flate2::read::GzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::fs::File;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use walkdir::WalkDir;
use flate2::read::MultiGzDecoder;
use zstd::stream::read::Decoder as ZstdDecoder;

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
        keyword2: matches.value_of("keyword2").map(|s| s.to_string()),
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
            if fs::metadata(&gz_path).is_ok() {
                path = gz_path;
                break;
            } else if fs::metadata(&zst_path).is_ok() {
                path = zst_path;
                break;
            }
        } else {
            if fs::metadata(&format!("{}.gz", path)).is_ok() {
                path.push_str(".gz");
            } else {
                path.push_str(".zst");
            }
        }
    }
    
    let file = File::open(&path).expect("Unable to open file");
    let buf_reader = if path.ends_with(".gz") {
        Box::new(BufReader::new(MultiGzDecoder::new(file))) as Box<dyn BufRead>
    } else {
        Box::new(BufReader::new(ZstdDecoder::new(file).unwrap())) as Box<dyn BufRead>
    };
    
    buf_reader.lines()
        .filter_map(Result::ok)
        .filter(|line| line.to_lowercase().starts_with(&keyword_lower))
        .collect()
}

fn process_file(path: &Path, ac: &AhoCorasick) -> Vec<String> {
    let file = File::open(path).expect("Unable to open file");
    let reader: Box<dyn BufRead> = if path.extension().and_then(|s| s.to_str()) == Some("gz") {
        Box::new(BufReader::new(GzDecoder::new(file)))
    } else if path.extension().and_then(|s| s.to_str()) == Some("zst") {
        Box::new(BufReader::new(ZstdDecoder::new(file).unwrap()))
    } else {
        Box::new(BufReader::new(file))
    };
    reader
        .lines()
        .filter_map(Result::ok)
        .filter(|line| ac.find_iter(line).count() == ac.pattern_count())
        .collect()
}

fn main() -> io::Result<()> {
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
    let ac = AhoCorasick::new(&patterns);

    let files: Vec<_> = WalkDir::new(&config.breach_data_location)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .collect();

    let progress_bar = ProgressBar::new(files.len() as u64);
    progress_bar.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} ({eta})")
        .unwrap()
        .progress_chars("#>-"));

    let results: Vec<_> = files.into_par_iter()
        .map(|entry| {
            progress_bar.inc(1);
            process_file(entry.path(), &ac)
        })
        .reduce(Vec::new, |mut a, b| {
            a.extend(b);
            a
        });

    progress_bar.finish_with_message("Processing complete.");

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