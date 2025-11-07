mod cache;
mod commands;
mod format;
mod io;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, CommandFactory, Parser, Subcommand, ValueHint};
use clap_complete::{generate, Shell};

use crate::cache::{cache_command, clip, clop};
use crate::commands::{
    append, enrich, extract, field, format, group_by, intersect, jgrep, join, lookup, merge, pipe,
    pope, prepend, regexify, replace, sample, skip, sort_lines, subtract, take, tally_impl,
    transpose, union, unnest, unzip, where_filter, window, zip, Sort, SortType,
};
use crate::io::FileOrStd;

#[derive(Args, Debug)]
struct WhereConditions {
    /// Equal to value
    #[clap(long)]
    eq: Option<String>,

    /// Not equal to value
    #[clap(long)]
    ne: Option<String>,

    /// Less than value
    #[clap(long)]
    lt: Option<String>,

    /// Less than or equal to value
    #[clap(long)]
    le: Option<String>,

    /// Greater than value
    #[clap(long)]
    gt: Option<String>,

    /// Greater than or equal to value
    #[clap(long)]
    ge: Option<String>,

    /// Contains substring
    #[clap(long)]
    contains: Option<String>,

    /// Matches regex pattern
    #[clap(long)]
    matches: Option<String>,
}

#[derive(Args, Debug)]
struct GroupAggregations {
    /// Count items in each group
    #[clap(long)]
    count: bool,

    /// Count distinct values in specified field
    #[clap(long)]
    distinct_count: Option<usize>,

    /// Sum values in specified field
    #[clap(long)]
    sum: Option<usize>,

    /// Average values in specified field
    #[clap(long)]
    avg: Option<usize>,

    /// Minimum value in specified field
    #[clap(long)]
    min: Option<usize>,

    /// Maximum value in specified field
    #[clap(long)]
    max: Option<usize>,

    /// First value in specified field
    #[clap(long)]
    first: Option<usize>,

    /// Last value in specified field
    #[clap(long)]
    last: Option<usize>,

    /// Output all values from specified field as columns
    #[clap(long)]
    values: Option<usize>,

    /// Output all distinct values from specified field as columns
    #[clap(long)]
    distinct_values: Option<usize>,
}

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
        /// The files to write split columns to
        files: Vec<FileOrStd>,

        /// Delimiter regex
        #[clap(short, long, default_value = r"\t|,")]
        delimiter: regex::Regex,
    },

    /// Sample lines from a file
    Sample {
        /// The file to sample from (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,

        /// Probability of sampling a line
        #[clap(short, long, default_value = "0.01")]
        probability: f64,
    },

    /// Tally the # of occurrences of each line (sort | uniq -c)
    Tally {
        /// The file to tally (defaults to stdin)
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

        /// The file to replace in (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Similar to tally, but only print the top k lines
    Topk {
        /// The number of lines to print
        #[clap(default_value = "10")]
        k: usize,

        /// The file to tally (defaults to stdin)
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

        /// The file to skip lines from (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Take first n lines from a file
    Take {
        /// Number of lines to take
        n: usize,

        /// The file to take lines from (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Prepend a string to each line
    Prepend {
        /// The string to prepend
        string: String,

        /// The file to process (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Append a string to each line
    Append {
        /// The string to append
        string: String,

        /// The file to process (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Print each line with a format string
    Format {
        /// The format string
        string: String,

        /// Delimiter regex
        #[clap(short, long, default_value = r"\t|,")]
        delimiter: regex::Regex,

        /// The file to process (defaults to stdin)
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

        /// The file to process (defaults to stdin)
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

        /// The file to extract from (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,

        /// Print only lines that match
        #[clap(short, long)]
        matching_only: bool,
    },

    /// Print the n-th column of a file
    Field {
        /// Field selector: single (1), multiple (1,3,5), range (2-4), or combination (1,2-4,6)
        selector: String,

        /// The file to print the column from (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,

        /// Input delimiter regex
        #[clap(short, long, default_value = r"\t|,")]
        delimiter: regex::Regex,

        /// Output delimiter
        #[clap(short, long, default_value = ",")]
        output_delimiter: String,
    },

    /// Turn columns into rows
    Unnest {
        /// The file to process (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,

        /// Delimiter regex
        #[clap(short, long, default_value = r"\t|,")]
        delimiter: regex::Regex,
    },

    /// Join the lines with a separator
    Join {
        /// The file to process (defaults to stdin)
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

        /// Use relative symlinks
        #[clap(long)]
        relative: bool,

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

    /// Filter rows based on field conditions
    Where {
        /// Field to filter on (0 for whole line)
        #[clap(short, long, default_value = "0")]
        field: usize,

        #[clap(flatten)]
        conditions: WhereConditions,

        /// Delimiter regex
        #[clap(short, long, default_value = r"\t|,")]
        delimiter: regex::Regex,

        /// The file to process (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Sort lines by field
    Sort {
        /// Field to sort by (0 for whole line)
        #[clap(short, long, default_value = "0")]
        field: usize,

        /// Sort type
        #[clap(short = 't', long, default_value = "string")]
        sort_type: SortType,

        /// Sort in reverse order
        #[clap(short, long)]
        reverse: bool,

        /// Delimiter regex
        #[clap(short, long, default_value = r"\t|,")]
        delimiter: regex::Regex,

        /// The file to process (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Group by field and aggregate (defaults to dedup like sort -u)
    Group {
        /// Field to group by (0 for whole line, which is the default)
        #[clap(short, long, default_value = "0")]
        field: usize,

        #[clap(flatten)]
        aggregations: GroupAggregations,

        /// Delimiter regex
        #[clap(short, long, default_value = r"\t|,")]
        delimiter: regex::Regex,

        /// Output delimiter
        #[clap(short, long, default_value = ",")]
        output_delimiter: String,

        /// The file to process (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Monitor pipe throughput and show recent lines (like pv but for content)
    Window {
        /// Number of recent lines to display
        #[clap(short, long, default_value = "5")]
        lines: usize,

        /// Update interval in milliseconds
        #[clap(short, long, default_value = "500")]
        refresh: u64,

        /// The file to process (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Filter JSON to show only subtrees containing matching values
    Jgrep {
        /// Pattern to search for in JSON values and keys
        pattern: String,

        /// Match only keys (not values)
        #[clap(long)]
        keys_only: bool,

        /// Match only values (not keys)
        #[clap(long)]
        values_only: bool,

        /// Case insensitive matching
        #[clap(short, long)]
        ignore_case: bool,

        /// Pretty-print output with indentation
        #[clap(short, long)]
        pretty: bool,

        /// Include N levels of context above matches (0=only matching path, 1+=full content at ancestor levels)
        #[clap(short, long, default_value = "0")]
        context: usize,

        /// The file to process (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Transpose rows and columns
    Transpose {
        /// Input delimiter regex
        #[clap(short, long, default_value = r"\t|,")]
        delimiter: regex::Regex,

        /// Output delimiter
        #[clap(short, long, default_value = ",")]
        output_delimiter: String,

        /// The file to process (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Generate regex from test cases (based on grex, one test case per line)
    Regexify {
        /// Convert digits to character classes
        #[clap(short, long)]
        digits: bool,

        /// Convert words to character classes
        #[clap(short, long)]
        words: bool,

        /// Convert repetitions
        #[clap(short, long)]
        repetitions: bool,

        /// Escape non-ASCII characters
        #[clap(short, long)]
        escape_non_ascii: bool,

        /// Enable case-insensitive matching
        #[clap(short = 'i', long)]
        case_insensitive: bool,

        /// Use non-capturing groups instead of capturing groups
        #[clap(short = 'n', long)]
        non_capturing_groups: bool,

        /// Generate verbose regex
        #[clap(short = 'x', long)]
        verbose: bool,

        /// Add anchors (^ and $)
        #[clap(short, long)]
        anchors: bool,

        /// The file to process (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Lookup values from a reference file
    Lookup {
        /// Lookup file to search in
        #[clap(short = 'f', long, value_hint = ValueHint::FilePath)]
        lookup_file: FileOrStd,

        /// Field number in lookup file to match against (0 for whole line)
        #[clap(short = 'l', long, default_value = "1")]
        lookup_field: usize,

        /// Field number in input to use for matching (0 or omit for whole line)
        #[clap(short = 'i', long)]
        input_field: Option<usize>,

        /// Enrich mode: join input line with looked up line instead of replacing
        #[clap(short = 'e', long, visible_alias = "join")]
        enrich: bool,

        /// Output delimiter (only used in enrich mode)
        #[clap(short, long, default_value = ",")]
        output_delimiter: String,

        /// Delimiter regex
        #[clap(short, long, default_value = r"\t|,")]
        delimiter: regex::Regex,

        /// The input file to process (defaults to stdin)
        #[clap(value_hint = ValueHint::FilePath)]
        file: Option<FileOrStd>,
    },

    /// Write stdin to a named FIFO (blocks until pope connects)
    Pipe {
        /// Name of the pipe (optional, defaults to "default")
        #[clap(short, long)]
        name: Option<String>,
    },

    /// Read from a named FIFO to stdout (blocks until pipe connects)
    Pope {
        /// Name of the pipe (optional, defaults to "default")
        #[clap(short, long)]
        name: Option<String>,
    },
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
        Commands::Field {
            file,
            selector,
            delimiter,
            output_delimiter,
        } => {
            field(
                file.unwrap_or_default(),
                &selector,
                delimiter,
                output_delimiter,
            )
            .await
        }
        Commands::Unnest { file, delimiter } => unnest(file.unwrap_or_default(), delimiter).await,
        Commands::Replace {
            regex,
            replacement,
            file,
        } => replace(file.unwrap_or_default(), regex, &replacement).await,
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
        } => cache_command(command, delete, cache_dir).await,
        Commands::Install {
            directory,
            relative,
            force,
            prefix,
        } => {
            let executable = std::env::current_exe()?;
            let directory = PathBuf::from(&*shellexpand::tilde(&directory));

            // Determine the source path for the symlink
            let source = if relative {
                // Safety check: verify "please" exists in the directory unless force is set
                let mut please_path = directory.clone();
                please_path.push("please");
                if !please_path.exists() && !force {
                    return Err(anyhow::anyhow!(
                        "Cannot create relative symlinks: 'please' not found in {}. Use --force to override.",
                        directory.display()
                    ));
                }
                PathBuf::from("please")
            } else {
                executable.clone()
            };

            for i in <Cli as CommandFactory>::command().get_subcommands() {
                if i.get_name() == "install" || i.get_name() == "complete" {
                    continue;
                }

                let mut path = directory.clone();
                path.push(format!("{}{}", prefix, i.get_name()));

                match tokio::fs::symlink(&source, &path).await {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        if force {
                            tokio::fs::remove_file(&path).await?;
                            tokio::fs::symlink(&source, path).await?;
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
        Commands::Where {
            field,
            conditions,
            delimiter,
            file,
        } => {
            let where_conditions = crate::commands::WhereConditions {
                eq: conditions.eq,
                ne: conditions.ne,
                lt: conditions.lt,
                le: conditions.le,
                gt: conditions.gt,
                ge: conditions.ge,
                contains: conditions.contains,
                matches: conditions.matches,
            };
            where_filter(file.unwrap_or_default(), field, where_conditions, delimiter).await
        }
        Commands::Sort {
            field,
            sort_type,
            reverse,
            delimiter,
            file,
        } => {
            sort_lines(
                file.unwrap_or_default(),
                field,
                sort_type,
                reverse,
                delimiter,
            )
            .await
        }
        Commands::Group {
            field,
            aggregations,
            delimiter,
            output_delimiter,
            file,
        } => {
            let group_aggregations = crate::commands::GroupAggregations {
                count: aggregations.count,
                distinct_count: aggregations.distinct_count,
                sum: aggregations.sum,
                avg: aggregations.avg,
                min: aggregations.min,
                max: aggregations.max,
                first: aggregations.first,
                last: aggregations.last,
                values: aggregations.values,
                distinct_values: aggregations.distinct_values,
            };
            group_by(
                file.unwrap_or_default(),
                field,
                group_aggregations,
                delimiter,
                output_delimiter,
            )
            .await
        }
        Commands::Window {
            lines,
            refresh,
            file,
        } => window(file.unwrap_or_default(), lines, refresh).await,
        Commands::Jgrep {
            pattern,
            keys_only,
            values_only,
            ignore_case,
            pretty,
            context,
            file,
        } => {
            jgrep(
                file.unwrap_or_default(),
                pattern,
                keys_only,
                values_only,
                ignore_case,
                pretty,
                context,
            )
            .await
        }
        Commands::Transpose {
            delimiter,
            output_delimiter,
            file,
        } => transpose(file.unwrap_or_default(), delimiter, output_delimiter).await,
        Commands::Regexify {
            digits,
            words,
            repetitions,
            escape_non_ascii,
            case_insensitive,
            non_capturing_groups,
            verbose,
            anchors,
            file,
        } => {
            regexify(
                file.unwrap_or_default(),
                digits,
                words,
                repetitions,
                escape_non_ascii,
                case_insensitive,
                non_capturing_groups,
                verbose,
                anchors,
            )
            .await
        }
        Commands::Lookup {
            lookup_file,
            lookup_field,
            input_field,
            enrich,
            output_delimiter,
            delimiter,
            file,
        } => {
            lookup(
                file.unwrap_or_default(),
                lookup_file,
                lookup_field,
                input_field,
                enrich,
                output_delimiter,
                delimiter,
            )
            .await
        }
        Commands::Pipe { name } => pipe(name),
        Commands::Pope { name } => pope(name),
    }?;

    Ok(())
}
