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
            let txt_path = format!("{}.txt", path);
            if fs::metadata(&gz_path).is_ok() {
                path = gz_path;
                break;
            } else if fs::metadata(&txt_path).is_ok() {
                path = txt_path;
                break;
            }
        } else {
            if fs::metadata(&format!("{}.gz", path)).is_ok() {
                path.push_str(".gz");
            } else {
                path.push_str(".txt");
            }
        }
    }
    
    let file = File::open(&path).expect("Unable to open file");
    let mut reader: Box<dyn Read> = if path.ends_with(".gz") {
        Box::new(MultiGzDecoder::new(file))
    } else {
        Box::new(file)
    };

    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).unwrap();

    buffer
        .lines()
        .par_bridge()
        .map(|line| line.to_str_lossy().into_owned())
        .filter(|line| line.to_lowercase().starts_with(&keyword_lower))
        .collect()
}

fn process_file(path: &Path, ac: &AhoCorasick) -> Vec<String> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return Vec::new(),
    };

    let mut reader: Box<dyn Read> = if path.extension().and_then(|s| s.to_str()) == Some("gz") {
        Box::new(GzDecoder::new(file))
    } else {
        Box::new(file)
    };

    let mut buffer = Vec::new();
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
    let ac = AhoCorasickBuilder::new().dfa(true).build(&patterns);

    let file_iterator = WalkDir::new(&config.breach_data_location)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file());

    let results: Vec<String> = file_iterator
        .par_bridge()
        .flat_map(|entry| {
            process_file(entry.path(), &ac)
        })
        .collect();

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