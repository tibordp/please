use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    hash::Hash,
    io::SeekFrom,
    mem::MaybeUninit,
    os::unix::prelude::OsStrExt,
    path::PathBuf,
    pin::Pin,
    process::{exit, ExitStatus},
};

use anyhow::{Context, Result};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
use clap_complete::{generate, Shell};
use futures::{Stream, StreamExt, TryStreamExt};
use rand::distributions::Distribution;
use regex::Regex;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt,
};
use tokio_stream::{wrappers::LinesStream, StreamMap};

use std::os::unix::process::ExitStatusExt;

use std::fmt::Write;
use std::hash::Hasher;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
#[command(multicall = true)]
struct CliMultiCall {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Default, Clone, Debug)]
enum FileOrStd {
    File(PathBuf),
    #[default]
    Std,
}

impl std::str::FromStr for FileOrStd {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        if s == "-" {
            Ok(FileOrStd::Std)
        } else {
            Ok(FileOrStd::File(PathBuf::from(s)))
        }
    }
}

impl Display for FileOrStd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileOrStd::File(path) => write!(f, "{}", path.display()),
            FileOrStd::Std => write!(f, "<std>"),
        }
    }
}

impl FileOrStd {
    pub async fn open_read(&self) -> Result<Pin<Box<dyn AsyncBufRead>>> {
        match self {
            FileOrStd::File(path) => {
                let file = tokio::fs::File::open(path)
                    .await
                    .with_context(|| format!("failed to open {self}"))?;
                Ok(Box::pin(tokio::io::BufReader::new(file)))
            }
            FileOrStd::Std => Ok(Box::pin(tokio::io::BufReader::new(tokio::io::stdin()))),
        }
    }

    pub async fn open_write(&self) -> Result<Pin<Box<dyn AsyncWrite>>> {
        match self {
            FileOrStd::File(path) => {
                let file = tokio::fs::File::create(path)
                    .await
                    .with_context(|| format!("failed to open {self}"))?;
                Ok(Box::pin(file))
            }
            FileOrStd::Std => Ok(Box::pin(tokio::io::stdout())),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum Sort {
    // Don't sort
    None,

    /// Sort in ascending order
    Asc,

    /// Sort in descending order
    Desc,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Merge two or more files (like cat, but streaming)
    Merge {
        #[clap(required = true, value_hint = ValueHint::FilePath)]
        /// The files to merge
        files: Vec<FileOrStd>,
    },

    /// Find the union of two or more files
    Union {
        #[clap(required = true, value_hint = ValueHint::FilePath)]
        /// The files to find the union of
        files: Vec<FileOrStd>,

        /// Tally by n-th field (0 for whole line)
        #[clap(short, long, default_value = "0")]
        field: usize,

        /// Delimiter regex
        #[clap(long, default_value = r"\t|,")]
        field_delimiter: regex::Regex,
    },

    /// Find the intersection of two or more files
    Intersect {
        /// The files to find the intersection of
        #[clap(required = true, value_hint = ValueHint::FilePath)]
        files: Vec<FileOrStd>,

        /// Tally by n-th field (0 for whole line)
        #[clap(short, long, default_value = "0")]
        field: usize,

        /// Delimiter regex
        #[clap(long, default_value = r"\t|,")]
        field_delimiter: regex::Regex,
    },

    /// Subtract all the files from the first file
    Subtract {
        #[clap(required = true, value_hint = ValueHint::FilePath)]
        /// The files to subtract
        files: Vec<FileOrStd>,

        /// Tally by n-th field (0 for whole line)
        #[clap(short, long, default_value = "0")]
        field: usize,

        /// Delimiter regex
        #[clap(long, default_value = r"\t|,")]
        field_delimiter: regex::Regex,
    },

    /// Zip the files together with a delimiter
    Zip {
        #[clap(required = true, value_hint = ValueHint::FilePath)]
        /// The files to zip
        files: Vec<FileOrStd>,

        /// Column separator
        #[clap(short, long, default_value = ",")]
        delimiter: String,
    },

    /// Split columns into separate files
    Unzip {
        #[clap(required = true, value_hint = ValueHint::FilePath)]
        /// The files to zip
        files: Vec<FileOrStd>,

        /// Delimiter regex
        #[clap(short, long, default_value = r"\t|,")]
        delimiter: regex::Regex,
    },

    /// Sample lines from a file
    Sample {
        /// The file to sample from
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,

        /// Probability of sampling a line
        #[clap(short, long, default_value = "0.01")]
        probability: f64,
    },

    /// Tally the # of occurrences of each line (sort | uniq -c)
    Tally {
        /// The file to tally
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,

        /// Tally by n-th field (0 for whole line)
        #[clap(short, long, default_value = "0")]
        field: usize,

        /// Delimiter regex
        #[clap(long, default_value = r"\t|,")]
        field_delimiter: regex::Regex,

        /// Sort the output by count
        #[clap(short, long, default_value = "asc")]
        sort: Sort,

        /// Column separator
        #[clap(short, long, default_value = ",")]
        delimiter: String,
    },

    /// Replaces with a regex (sed, but sane)
    Replace {
        /// The regex to replace
        #[clap(required = true)]
        regex: regex::Regex,

        /// The replacement string
        #[clap(required = true)]
        replacement: String,

        /// The file to replace in
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Similar to tally, but only print the top k lines
    Topk {
        /// The number of lines to print
        #[clap(default_value = "10")]
        k: usize,

        /// The file to tally
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,

        /// Tally by n-th field (0 for whole line)
        #[clap(short, long, default_value = "0")]
        field: usize,

        /// Delimiter regex
        #[clap(long, default_value = r"\t|,")]
        field_delimiter: regex::Regex,

        /// Column separator
        #[clap(short, long, default_value = ",")]
        delimiter: String,
    },

    /// Skip first n lines from a file
    Skip {
        /// Number of lines to skip
        n: usize,

        /// The file to skip lines from
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Take first n lines from a file
    Take {
        /// Number of lines to take
        n: usize,

        /// The file to take lines from
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Prepend a string to each line
    Prepend {
        /// The string to prepend
        string: String,

        /// The file to skip lines from
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Append a string to each line
    Append {
        /// The string to append
        string: String,

        /// The file to skip lines from
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Print each line with a format string
    Format {
        /// The string to append
        string: String,

        /// Delimiter regex
        #[clap(short, long, default_value = r"\t|,")]
        delimiter: regex::Regex,

        /// The file to skip lines from
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Execute a command for each line and append the output as a new column
    Enrich {
        /// The command to execute
        command_line: String,

        /// Delimiter regex
        #[clap(short, long, default_value = r"\t|,")]
        delimiter: regex::Regex,

        /// Output delimiter
        #[clap(short, long, default_value = ",")]
        output_delimiter: String,

        /// The file to skip lines from
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,

        /// Parallelism
        #[clap(short, long, default_value = "32")]
        parallelism: usize,

        /// Exit on error
        #[clap(long, default_value = "false")]
        exit_on_error: bool,
    },

    /// Extract regex capturing groups from each line
    Extract {
        /// The regex to extract
        #[clap()]
        regex: regex::Regex,

        /// Column separator
        #[clap(short, long, default_value = ",")]
        delimiter: String,

        /// The file to extract from
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,

        /// Print only lines that match
        #[clap(short, long)]
        matching_only: bool,
    },

    /// Print the n-th column of a file
    Field {
        /// The column to print
        n: usize,

        /// The file to print the column from
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,

        /// Delimiter regex
        #[clap(short, long, default_value = r"\t|,")]
        delimiter: regex::Regex,
    },

    /// Turn columns into rows
    Unnest {
        /// The file to print the column from
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,

        /// Delimiter regex
        #[clap(short, long, default_value = r"\t|,")]
        delimiter: regex::Regex,
    },


    /// Join the lines with a separator
    Join {
        /// The file to print the column from
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,

        /// Column separator
        #[clap(short, long, default_value = ",")]
        delimiter: String,
    },

    /// Cache the output of a command
    Cache {
        /// Cache directory
        #[clap(long, env = "CACHE_DIR", default_value = "~/.cache/please", value_hint = ValueHint::DirPath)]
        cache_dir: String,

        /// Delete the buffer
        #[clap(long)]
        delete: bool,

        /// Command to execute
        #[clap(required(true), raw(true))]
        command: Vec<String>,
    },

    /// Copy to the clipboard
    Clip {
        /// Cache directory
        #[clap(long, env = "CACHE_DIR", default_value = "~/.cache/please", value_hint = ValueHint::DirPath)]
        cache_dir: String,

        /// Name of the buffer
        #[clap(short, long)]
        name: Option<String>,

        /// Delete the buffer
        #[clap(long)]
        delete: bool,

        /// Delete all buffers
        #[clap(long)]
        delete_all: bool,
    },

    /// Paste from the clipboard
    Clop {
        /// Cache directory
        #[clap(long, env = "CACHE_DIR", default_value = "~/.cache/please", value_hint = ValueHint::DirPath)]
        cache_dir: String,

        /// Name of the buffer
        #[clap(short, long)]
        name: Option<String>,

        /// Print filename instead of contents
        #[clap(short, long)]
        print: bool,
    },

    /// Install the commands as symlinks
    Install {
        /// The directory to install to
        #[clap(short, long, default_value = ".")]
        directory: String,

        /// Overwrite existing link
        #[clap(short, long)]
        force: bool,

        /// Prefix for the commands
        #[clap(long, default_value = "")]
        prefix: String,
    },

    /// Generate shell completions
    Complete {
        /// The shell to generate completions for
        shell: Shell,

        /// Generate completions for the subcommands
        #[clap(long)]
        subcommands: bool,
    },
}

struct FieldEntry {
    line: String,
    field: Option<String>,
}
impl Hash for FieldEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.field.as_ref().unwrap_or(&self.line).hash(state);
    }
}
impl PartialEq for FieldEntry {
    fn eq(&self, other: &Self) -> bool {
        self.field.as_ref().unwrap_or(&self.line) == other.field.as_ref().unwrap_or(&other.line)
    }
}
impl Eq for FieldEntry {}

async fn merge(files: Vec<FileOrStd>) -> Result<()> {
    let mut streams = StreamMap::new();

    for (i, file) in files.into_iter().enumerate() {
        let reader = file.open_read().await?;
        streams.insert(i, LinesStream::new(reader.lines()));
    }

    while let Some((_, line)) = streams.next().await {
        println!("{}", line?);
    }

    Ok(())
}

async fn union(files: Vec<FileOrStd>, field_delimiter: Regex, field: usize) -> Result<()> {
    let mut lines = HashSet::new();
    let mut streams = StreamMap::new();

    for (i, file) in files.into_iter().enumerate() {
        let reader = file.open_read().await?;
        streams.insert(i, LinesStream::new(reader.lines()));
    }

    while let Some((_, line)) = streams.next().await {
        let line = line?;

        let ent = if field == 0 {
            FieldEntry {
                line: line.clone(),
                field: None,
            }
        } else {
            let field = field_delimiter
                .split(&line)
                .nth(field - 1)
                .map(|s| s.to_string());
            FieldEntry {
                line: line.clone(),
                field,
            }
        };

        if lines.insert(ent) {
            println!("{}", line);
        }
    }

    Ok(())
}

async fn subtract(files: Vec<FileOrStd>, field_delimiter: Regex, field: usize) -> Result<()> {
    let mut lines = HashMap::new();
    let mut streams = StreamMap::new();

    for (i, file) in files.into_iter().enumerate() {
        let reader = file.open_read().await?;
        streams.insert(i, LinesStream::new(reader.lines()));
    }

    while let Some((idx, line)) = streams.next().await {
        let line = line?;

        let ent = if field == 0 {
            FieldEntry { line, field: None }
        } else {
            let field = field_delimiter
                .split(&line)
                .nth(field - 1)
                .map(|s| s.to_string());
            FieldEntry { line, field }
        };

        if idx == 0 {
            lines.entry(ent).or_insert(true);
        } else {
            lines.insert(ent, false);
        }
    }

    for (ent, keep) in lines {
        if keep {
            println!("{}", ent.line);
        }
    }

    Ok(())
}

async fn zip(files: Vec<FileOrStd>, delimiter: String) -> Result<()> {
    let mut readers = Vec::new();

    for file in files.into_iter() {
        readers.push(file.open_read().await?.lines());
    }

    let mut columns = vec![String::new(); readers.len()];

    'outer: loop {
        for (i, reader) in readers.iter_mut().enumerate() {
            match reader.next_line().await? {
                Some(line) => {
                    columns[i] = line;
                }
                None => {
                    break 'outer;
                }
            }
        }

        println!("{}", columns.join(&delimiter));
    }

    Ok(())
}

async fn unzip(files: Vec<FileOrStd>, delimiter: Regex) -> Result<()> {
    let reader = FileOrStd::Std.open_read().await?;
    let mut writers = Vec::new();

    for file in files.into_iter() {
        writers.push(file.open_write().await?);
    }

    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {
        for (i, segment) in delimiter.split(&line).take(writers.len()).enumerate() {
            writers[i].write_all(segment.as_bytes()).await?;
            writers[i].write_all(b"\n").await?;
        }
    }

    Ok(())
}

async fn intersect(files: Vec<FileOrStd>, field_delimiter: Regex, field: usize) -> Result<()> {
    let mut lines = HashMap::new();

    let mut seen: Vec<_> = (0..files.len()).map(|_| HashSet::new()).collect();
    let mut streams = StreamMap::new();

    for (i, file) in files.into_iter().enumerate() {
        let reader = file.open_read().await?;
        streams.insert(i, LinesStream::new(reader.lines()));
    }

    while let Some((idx, line)) = streams.next().await {
        let line = line?;

        if !seen[idx].insert(line.clone()) {
            continue;
        }

        let ent = if field == 0 {
            FieldEntry {
                line: line.clone(),
                field: None,
            }
        } else {
            let field = field_delimiter
                .split(&line)
                .nth(field - 1)
                .map(|s| s.to_string());
            FieldEntry {
                line: line.clone(),
                field,
            }
        };

        let n = lines.entry(ent).or_insert(0);
        *n += 1;

        if *n == seen.len() {
            println!("{}", line);
        }
    }
    Ok(())
}

async fn tally_impl(
    file: FileOrStd,
    sort: Sort,
    delimiter: String,
    field_delimiter: Regex,
    field: usize,
    topk: Option<usize>,
) -> Result<()> {
    let mut tally = HashMap::new();

    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        let ent = if field == 0 {
            FieldEntry {
                line: line.clone(),
                field: None,
            }
        } else {
            let field = field_delimiter
                .split(&line)
                .nth(field - 1)
                .map(|s| s.to_string());
            FieldEntry {
                line: line.clone(),
                field,
            }
        };
        *tally.entry(ent).or_insert(0) += 1;
    }

    match sort {
        Sort::None => {
            for (ent, count) in tally {
                println!("{}{}{}", count, delimiter, ent.line);
            }
        }
        _ => {
            let mut lines: Vec<_> = tally.into_iter().collect();
            match sort {
                Sort::Asc => lines.sort_by_key(|(_, count)| *count),
                Sort::Desc => lines.sort_by_key(|(_, count)| std::cmp::Reverse(*count)),
                Sort::None => unreachable!(),
            }

            for (idx, (ent, count)) in lines.into_iter().enumerate() {
                if let Some(topk) = topk {
                    if idx >= topk {
                        break;
                    }
                }
                println!("{}{}{}", count, delimiter, ent.line);
            }
        }
    }

    Ok(())
}

async fn sample(file: FileOrStd, probability: f64) -> Result<()> {
    let mut rng = rand::thread_rng();

    let dist = rand::distributions::Bernoulli::new(probability)
        .map_err(|_| anyhow::anyhow!("invalid probability {}", probability))?;

    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        if dist.sample(&mut rng) {
            println!("{}", line);
        }
    }

    Ok(())
}

async fn skip(file: FileOrStd, n: usize) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    for _ in 0..n {
        lines.next_line().await?;
    }

    while let Some(line) = lines.next_line().await? {
        println!("{}", line);
    }

    Ok(())
}

async fn take(file: FileOrStd, n: usize) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    for _ in 0..n {
        if let Some(line) = lines.next_line().await? {
            println!("{}", line);
        } else {
            return Ok(());
        }
    }

    // Consume the rest of the bytes to avoid broken pipes
    let mut reader = lines.into_inner();
    loop {
        match reader.fill_buf().await? {
            [] => break,
            buf => {
                let n = buf.len();
                reader.consume(n);
            }
        }
    }

    Ok(())
}

async fn extract(
    file: FileOrStd,
    regex: Regex,
    delimiter: String,
    matching_only: bool,
) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        let Some(mtch) = regex.captures(&line) else {
            if !matching_only {
                println!();
            }
            continue;
        };

        let mut first = true;
        for m in mtch.iter().skip(1) {
            if first {
                first = false;
            } else {
                print!("{}", delimiter);
            }
            print!("{}", m.map(|s| s.as_str()).unwrap_or(""));
        }

        println!();
    }

    Ok(())
}

async fn prepend(file: FileOrStd, string: &str) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        println!("{}{}", string, line);
    }

    Ok(())
}

async fn append(file: FileOrStd, string: &str) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        println!("{}{}", line, string);
    }

    Ok(())
}

#[derive(PartialEq, Eq, Debug)]
enum Piece {
    Literal(String),
    WholeLine,
    Field(isize),
}

fn parse_format_string(s: &str) -> Result<Vec<Piece>> {
    let mut pieces = Vec::new();

    enum State {
        Normal,
        BraceOpen,
        Index,
        BraceClose,
    }

    let mut state = State::Normal;

    macro_rules! push_ch {
        ($ch:expr) => {
            let ch = $ch;
            if let Some(Piece::Literal(b)) = pieces.last_mut() {
                b.push(ch);
            } else {
                pieces.push(Piece::Literal(ch.to_string()));
            }
        };
    }

    let mut index = String::new();

    for ch in s.chars() {
        match state {
            State::Normal => {
                if ch == '{' {
                    state = State::BraceOpen;
                } else if ch == '}' {
                    state = State::BraceClose;
                } else {
                    push_ch!(ch);
                }
            }
            State::BraceClose => {
                if ch == '}' {
                    push_ch!('}');
                    state = State::Normal;
                } else {
                    return Err(anyhow::anyhow!("invalid format string"));
                }
            }
            State::BraceOpen => {
                if ch == '{' {
                    push_ch!('{');
                    state = State::Normal;
                } else if ch == '}' {
                    pieces.push(Piece::WholeLine);
                    state = State::Normal;
                } else if ch.is_ascii_digit() || ch == '-' {
                    index.push(ch);
                    state = State::Index;
                }
            }
            State::Index => {
                if ch.is_ascii_digit() {
                    index.push(ch);
                } else if ch == '}' {
                    pieces.push(Piece::Field(
                        index
                            .parse()
                            .map_err(|_| anyhow::anyhow!("invalid format string"))?,
                    ));
                    index.clear();
                    state = State::Normal;
                } else {
                    return Err(anyhow::anyhow!("invalid format string"));
                }
            }
        }
    }

    match state {
        State::Normal => Ok(pieces),
        _ => Err(anyhow::anyhow!("invalid format string")),
    }
}

async fn format(file: FileOrStd, format_string: &str, delimiter: Regex) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    let format_string = parse_format_string(format_string)?;
    let has_parts = format_string.iter().any(|x| matches!(x, Piece::Field(_)));

    while let Some(line) = lines.next_line().await? {
        let parts = has_parts.then(|| delimiter.split(&line).collect::<Vec<_>>());

        for piece in &format_string {
            match piece {
                Piece::Literal(s) => print!("{}", s),
                Piece::WholeLine | Piece::Field(0) => print!("{}", line),
                Piece::Field(n) => {
                    let parts = parts.as_ref().unwrap();

                    let index: usize = match n {
                        n if *n < 0 => parts.len() as isize + n,
                        n => *n - 1,
                    } as _;

                    if index < parts.len() {
                        print!("{}", parts[index]);
                    }
                }
            }
        }

        println!();
    }

    Ok(())
}

fn enrich_stream(
    file: FileOrStd,
    command_line: String,
    delimiter: Regex,
) -> impl Stream<Item = Result<(String, String)>> {
    async_stream::try_stream! {
        let reader = file.open_read().await?;
        let mut lines = reader.lines();

        let format_string = parse_format_string(&command_line)?;
        let has_parts = format_string.iter().any(|x| matches!(x, Piece::Field(_)));

        while let Some(line) = lines.next_line().await? {
            let parts = has_parts.then(|| delimiter.split(&line).collect::<Vec<_>>());
            let mut cmd_line = String::new();

            for piece in &format_string {
                match piece {
                    Piece::Literal(s) => {
                        let _ = write!(cmd_line, "{}", s);
                    },
                    Piece::WholeLine | Piece::Field(0) => {
                        let _ = write!(cmd_line,"{}", line);
                    },
                    Piece::Field(n) => {
                        let parts = parts.as_ref().unwrap();

                        let index: usize = match n {
                            n if *n < 0 => parts.len() as isize + n,
                            n => *n - 1,
                        } as _;

                        if index < parts.len() {
                            let _ = write!(cmd_line, "{}", parts[index]);
                        }
                    }
                }
            }

            yield (line, cmd_line);
        }
    }
}

async fn enrich(
    file: FileOrStd,
    command_line: String,
    delimiter: Regex,
    output_delimiter: String,
    parallelism: usize,
    exit_on_error: bool,
) -> Result<()> {
    let stream = enrich_stream(file, command_line, delimiter);

    let delimiter = &output_delimiter;

    stream
        .try_for_each_concurrent(parallelism, |(mut line, cmd_line)| async move {
            let child = tokio::process::Command::new("sh")
                .arg("-c")
                .arg(&cmd_line)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .spawn()?;

            let output = child.wait_with_output().await?;
            if !output.status.success() {
                if exit_on_error {
                    exit_with(output.status);
                } else {
                    eprintln!("error: command failed: {}", cmd_line);
                }
            }

            let output = String::from_utf8_lossy(&output.stdout);
            for field in output.trim().split('\n') {
                write!(line, "{}{}", delimiter, field)?;
            }

            println!("{}", line);

            Ok::<_, anyhow::Error>(())
        })
        .await?;

    Ok(())
}

async fn field(file: FileOrStd, n: usize, delimiter: Regex) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    if n == 0 {
        while let Some(line) = lines.next_line().await? {
            println!("{}", line);
        }
    } else {
        while let Some(line) = lines.next_line().await? {
            let mut parts = delimiter.split(&line);
            if let Some(part) = parts.nth(n - 1) {
                println!("{}", part);
            } else {
                println!();
            }
        }
    }

    Ok(())
}


async fn unnest(file: FileOrStd, delimiter: Regex) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        for part in delimiter.split(&line) {
            println!("{}", part);
        }
    }

    Ok(())
}

async fn replace(file: FileOrStd, regex: Regex, replacement: &str) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        let replaced = regex.replace_all(&line, replacement);
        println!("{}", replaced);
    }

    Ok(())
}

async fn join(file: FileOrStd, delimiter: String) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    let mut first = true;
    while let Some(line) = lines.next_line().await? {
        if first {
            print!("{}", line);
            first = false;
        } else {
            print!("{}{}", delimiter, line);
        }
    }

    Ok(())
}

fn get_cache_key(command: &[String]) -> Result<String, anyhow::Error> {
    let mut cache_key = xxhash_rust::xxh3::Xxh3::new();
    cache_key.write_usize(command.len());
    for i in command {
        let bytes = i.as_bytes();
        cache_key.write_usize(i.len());
        cache_key.update(bytes);
    }
    let cwd = std::env::current_dir()?;
    let as_bytes = cwd.as_os_str().as_bytes();
    cache_key.write_usize(as_bytes.len());
    cache_key.update(as_bytes);
    let mut env: Vec<_> = std::env::vars().collect();
    env.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    cache_key.write_usize(env.len());
    for (k, v) in env {
        let k_bytes = k.as_bytes();
        let v_bytes = v.as_bytes();

        cache_key.write_usize(k_bytes.len());
        cache_key.update(k_bytes);

        cache_key.write_usize(v_bytes.len());
        cache_key.update(v_bytes);
    }
    let input_hash = format!("{:032x}", cache_key.digest128());
    Ok(input_hash)
}

/// Exit with the given exit status
fn exit_with(exit_status: ExitStatus) -> ! {
    if let Some(code) = exit_status.code() {
        exit(code);
    }

    if let Some(signal) = exit_status.signal() {
        unsafe {
            libc::raise(signal);
        }
    }

    unreachable!();
}

async fn cache(command: Vec<String>, delete: bool, cache_dir: String) -> Result<()> {
    let input_hash = get_cache_key(&command)?;
    let cache_dir = PathBuf::from(&*shellexpand::tilde(&cache_dir));

    let cache_path = cache_dir.join(&input_hash);
    let tmp_cache_path = cache_dir.join(uuid::Uuid::new_v4().to_string());

    if delete {
        match tokio::fs::remove_file(&cache_path).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        return Ok(());
    }

    tokio::fs::create_dir_all(&cache_dir).await?;

    if cache_path.exists() {
        let mut cache_file = tokio::fs::File::open(&cache_path).await?;
        let mut stdout = tokio::io::stdout();
        let exit_status = unsafe {
            let mut exit_status: MaybeUninit<ExitStatus> = MaybeUninit::uninit();

            cache_file
                .read_exact(std::slice::from_raw_parts_mut(
                    exit_status.as_mut_ptr() as *mut u8,
                    std::mem::size_of::<ExitStatus>(),
                ))
                .await?;

            exit_status.assume_init()
        };

        tokio::io::copy(&mut cache_file, &mut stdout).await?;
        exit_with(exit_status);
    }

    let mut cache_file = tokio::fs::File::create(&tmp_cache_path).await?;
    cache_file
        .write_all(&[0; std::mem::size_of::<ExitStatus>()])
        .await?;

    let mut child = tokio::process::Command::new(command[0].clone())
        .args(&command[1..])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .spawn()?;

    let mut child_stdout = child.stdout.take().unwrap();

    let output_copy = {
        let cache_file = &mut cache_file;

        async move {
            let mut stdout = tokio::io::stdout();
            let mut buf = [0; 1024];
            loop {
                let n = child_stdout.read(&mut buf).await?;
                if n == 0 {
                    break;
                }

                tokio::try_join!(cache_file.write_all(&buf[..n]), stdout.write_all(&buf[..n]))?;
            }

            Ok::<_, std::io::Error>(())
        }
    };

    let (_, exit_status) = tokio::try_join!(output_copy, child.wait())?;
    cache_file.seek(SeekFrom::Start(0)).await?;

    unsafe {
        cache_file
            .write_all(std::slice::from_raw_parts(
                &exit_status as *const ExitStatus as *const u8,
                std::mem::size_of::<ExitStatus>(),
            ))
            .await?;
    }

    cache_file.sync_all().await?;
    drop(cache_file);

    tokio::fs::rename(&tmp_cache_path, &cache_path).await?;
    exit_with(exit_status)
}

async fn clip(
    cache_dir: String,
    name: Option<String>,
    delete: bool,
    delete_all: bool,
) -> Result<()> {
    let cache_dir = PathBuf::from(&*shellexpand::tilde(&cache_dir));

    let tmp_cache_path = cache_dir.join(match name {
        Some(name) => format!("clip_buf_{}", name),
        None => "clip_buf".to_string(),
    });

    if delete || delete_all {
        let mut files = Vec::new();
        if !delete_all {
            files.push(tmp_cache_path);
        } else {
            let mut dir = tokio::fs::read_dir(&cache_dir).await?;
            while let Some(entry) = dir.next_entry().await? {
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|x| x.to_str()) {
                    if name.starts_with("clip_buf") {
                        files.push(path);
                    }
                }
            }
        }
        for file in files {
            match tokio::fs::remove_file(&file).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        return Ok(());
    }

    tokio::fs::create_dir_all(&cache_dir).await?;

    let mut cache_file = tokio::fs::File::create(&tmp_cache_path).await?;
    let mut stdin = tokio::io::stdin();

    tokio::io::copy(&mut stdin, &mut cache_file).await?;

    Ok(())
}

async fn clop(cache_dir: String, name: Option<String>, print: bool) -> Result<()> {
    let cache_dir = PathBuf::from(&*shellexpand::tilde(&cache_dir));
    let cache_path = cache_dir.join(match name {
        Some(name) => format!("clip_buf_{}", name),
        None => "clip_buf".to_string(),
    });

    if print {
        match tokio::fs::try_exists(&cache_path).await {
            Ok(false) => return Err(anyhow::anyhow!("nothing in clipboard")),
            Ok(true) => println!("{}", cache_path.display()),
            Err(e) => return Err(e.into()),
        }
    } else {
        let mut cache_file = match tokio::fs::File::open(&cache_path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(anyhow::anyhow!("nothing in clipboard"));
            }
            Err(e) => return Err(e.into()),
        };

        let mut stdout = tokio::io::stdout();
        tokio::io::copy(&mut cache_file, &mut stdout).await?;
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    unsafe {
        // Reset SIGPIPE to default behavior
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }

    let command = match CliMultiCall::try_parse() {
        Ok(m) => m.command,
        Err(e) if e.kind() == clap::error::ErrorKind::InvalidSubcommand => {
            // Reparse without multicall
            Cli::parse().command
        }
        Err(e) => {
            e.exit();
        }
    };

    match command {
        Commands::Merge { files } => merge(files).await,
        Commands::Union {
            files,
            field_delimiter,
            field,
        } => union(files, field_delimiter, field).await,
        Commands::Intersect {
            files,
            field_delimiter,
            field,
        } => intersect(files, field_delimiter, field).await,
        Commands::Subtract {
            files,
            field_delimiter,
            field,
        } => subtract(files, field_delimiter, field).await,
        Commands::Zip { files, delimiter } => zip(files, delimiter).await,
        Commands::Unzip { files, delimiter } => unzip(files, delimiter).await,
        Commands::Tally {
            file,
            sort,
            delimiter,
            field_delimiter,
            field,
        } => {
            tally_impl(
                file.unwrap_or_default(),
                sort,
                delimiter,
                field_delimiter,
                field,
                None,
            )
            .await
        }
        Commands::Topk {
            file,
            delimiter,
            field_delimiter,
            field,
            k,
        } => {
            tally_impl(
                file.unwrap_or_default(),
                Sort::Desc,
                delimiter,
                field_delimiter,
                field,
                Some(k),
            )
            .await
        }
        Commands::Sample { file, probability } => {
            sample(file.unwrap_or_default(), probability).await
        }
        Commands::Skip { file, n } => skip(file.unwrap_or_default(), n).await,
        Commands::Take { file, n } => take(file.unwrap_or_default(), n).await,
        Commands::Extract {
            regex,
            delimiter,
            file,
            matching_only,
        } => extract(file.unwrap_or_default(), regex, delimiter, matching_only).await,
        Commands::Prepend { file, string } => prepend(file.unwrap_or_default(), &string).await,
        Commands::Append { file, string } => append(file.unwrap_or_default(), &string).await,
        Commands::Format {
            file,
            string,
            delimiter,
        } => format(file.unwrap_or_default(), &string, delimiter).await,
        Commands::Enrich {
            file,
            command_line,
            delimiter,
            output_delimiter,
            parallelism,
            exit_on_error,
        } => {
            enrich(
                file.unwrap_or_default(),
                command_line,
                delimiter,
                output_delimiter,
                parallelism,
                exit_on_error,
            )
            .await
        }
        Commands::Field { file, n, delimiter } => {
            field(file.unwrap_or_default(), n, delimiter).await
        }
        Commands::Unnest { file, delimiter } => {
            unnest(file.unwrap_or_default(), delimiter).await
        }
        Commands::Replace { regex, replacement, file } => {
            replace(file.unwrap_or_default(), regex, &replacement).await
        }
        Commands::Clip {
            cache_dir,
            name,
            delete,
            delete_all,
        } => clip(cache_dir, name, delete, delete_all).await,
        Commands::Clop {
            cache_dir,
            name,
            print,
        } => clop(cache_dir, name, print).await,
        Commands::Join { file, delimiter } => join(file.unwrap_or_default(), delimiter).await,
        Commands::Cache {
            command,
            cache_dir,
            delete,
        } => cache(command, delete, cache_dir).await,
        Commands::Install {
            directory,
            force,
            prefix,
        } => {
            let executable = std::env::current_exe()?;
            let directory = PathBuf::from(&*shellexpand::tilde(&directory));

            for i in <Cli as CommandFactory>::command().get_subcommands() {
                if i.get_name() == "install" || i.get_name() == "complete" {
                    continue;
                }

                let mut path = directory.clone();
                path.push(format!("{}{}", prefix, i.get_name()));

                match tokio::fs::symlink(&executable, &path).await {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        if force {
                            tokio::fs::remove_file(&path).await?;
                            tokio::fs::symlink(&executable, path).await?;
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Ok(())
        }
        Commands::Complete { shell, subcommands } => {
            generate(
                shell,
                &mut CliMultiCall::command(),
                "please",
                &mut std::io::stdout(),
            );

            if subcommands {
                for i in <Cli as CommandFactory>::command().get_subcommands_mut() {
                    let name = i.get_name().to_string();
                    generate(shell, i, name, &mut std::io::stdout());
                }
            }
            Ok(())
        }
    }?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_format_string() {
        assert_eq!(
            parse_format_string("Hello, world").unwrap(),
            vec![Piece::Literal("Hello, world".to_string())]
        );

        assert_eq!(
            parse_format_string("Hello, {0}").unwrap(),
            vec![Piece::Literal("Hello, ".to_string()), Piece::Field(0)]
        );

        assert_eq!(
            parse_format_string("Hello, {0} {1}").unwrap(),
            vec![
                Piece::Literal("Hello, ".to_string()),
                Piece::Field(0),
                Piece::Literal(" ".to_string()),
                Piece::Field(1)
            ]
        );

        assert_eq!(
            parse_format_string("Hello, {{0}}").unwrap(),
            vec![Piece::Literal("Hello, {0}".to_string())]
        );

        assert_eq!(
            parse_format_string("Hello, {} {} foo").unwrap(),
            vec![
                Piece::Literal("Hello, ".to_string()),
                Piece::WholeLine,
                Piece::Literal(" ".to_string()),
                Piece::WholeLine,
                Piece::Literal(" foo".to_string())
            ]
        );
    }
}
