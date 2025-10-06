use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    process::ExitStatus,
};

use anyhow::Result;
use async_stream::try_stream;
use clap::ValueEnum;
use futures::{Stream, StreamExt, TryStreamExt};
use rand::distributions::Distribution;
use regex::Regex;
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::Value;
use std::cmp::Ordering;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio_stream::{wrappers::LinesStream, StreamMap};

use crate::format::{format_line, parse_format_string, Piece};
use crate::io::FileOrStd;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum Sort {
    None,
    Asc,
    Desc,
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum, Debug)]
pub enum SortType {
    /// Sort as strings (lexicographic)
    String,
    /// Sort as numbers
    Numeric,
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum, Debug)]
pub enum CompareOp {
    /// Equal to
    Eq,
    /// Not equal to
    Ne,
    /// Less than
    Lt,
    /// Less than or equal to
    Le,
    /// Greater than
    Gt,
    /// Greater than or equal to
    Ge,
    /// Contains substring
    Contains,
    /// Matches regex
    Matches,
}

#[derive(Clone, Debug)]
pub struct WhereCondition {
    pub field: usize,
    pub op: CompareOp,
    pub value: String,
    pub regex: Option<Regex>,
}

impl WhereCondition {
    pub fn new(field: usize, op: CompareOp, value: String) -> Result<Self> {
        let regex = if op == CompareOp::Matches {
            Some(Regex::new(&value)?)
        } else {
            None
        };

        Ok(WhereCondition {
            field,
            op,
            value,
            regex,
        })
    }

    pub fn evaluate(&self, line: &str, parts: &[&str]) -> bool {
        let field_value = if self.field == 0 {
            line
        } else if self.field <= parts.len() {
            parts[self.field - 1]
        } else {
            ""
        };

        match self.op {
            CompareOp::Eq => field_value == self.value,
            CompareOp::Ne => field_value != self.value,
            CompareOp::Contains => field_value.contains(&self.value),
            CompareOp::Matches => {
                if let Some(ref regex) = self.regex {
                    regex.is_match(field_value)
                } else {
                    false
                }
            }
            CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge => {
                // Try numeric comparison first
                if let (Ok(field_num), Ok(value_num)) =
                    (field_value.parse::<f64>(), self.value.parse::<f64>())
                {
                    match self.op {
                        CompareOp::Lt => field_num < value_num,
                        CompareOp::Le => field_num <= value_num,
                        CompareOp::Gt => field_num > value_num,
                        CompareOp::Ge => field_num >= value_num,
                        _ => unreachable!(),
                    }
                } else {
                    // Fall back to string comparison
                    match self.op {
                        CompareOp::Lt => field_value < self.value.as_str(),
                        CompareOp::Le => field_value <= self.value.as_str(),
                        CompareOp::Gt => field_value > self.value.as_str(),
                        CompareOp::Ge => field_value >= self.value.as_str(),
                        _ => unreachable!(),
                    }
                }
            }
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum, Debug)]
pub enum AggregateFunction {
    /// Count of items
    Count,
    /// Count of distinct values
    DistinctCount,
    /// Sum of numeric values
    Sum,
    /// Average of numeric values
    Avg,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// First value encountered
    First,
    /// Last value encountered
    Last,
    /// All values as comma-separated columns
    Values,
    /// All distinct values as comma-separated columns
    DistinctValues,
}

#[derive(Debug)]
pub struct FieldEntry {
    pub line: String,
    pub field: Option<String>,
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

pub async fn merge(files: Vec<FileOrStd>) -> Result<()> {
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

pub async fn union(files: Vec<FileOrStd>, field_delimiter: Regex, field: usize) -> Result<()> {
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

pub async fn subtract(files: Vec<FileOrStd>, field_delimiter: Regex, field: usize) -> Result<()> {
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

pub async fn zip(files: Vec<FileOrStd>, delimiter: String) -> Result<()> {
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

pub async fn unzip(files: Vec<FileOrStd>, delimiter: Regex) -> Result<()> {
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

pub async fn intersect(files: Vec<FileOrStd>, field_delimiter: Regex, field: usize) -> Result<()> {
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

pub async fn tally_impl(
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

pub async fn sample(file: FileOrStd, probability: f64) -> Result<()> {
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

pub async fn skip(file: FileOrStd, n: usize) -> Result<()> {
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

pub async fn take(file: FileOrStd, n: usize) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    for _ in 0..n {
        if let Some(line) = lines.next_line().await? {
            println!("{}", line);
        } else {
            return Ok(());
        }
    }

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

pub async fn extract(
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

pub async fn prepend(file: FileOrStd, string: &str) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        println!("{}{}", string, line);
    }

    Ok(())
}

pub async fn append(file: FileOrStd, string: &str) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        println!("{}{}", line, string);
    }

    Ok(())
}

pub async fn format(file: FileOrStd, format_string: &str, delimiter: Regex) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    let format_string = parse_format_string(format_string)?;
    let has_parts = format_string.iter().any(|x| matches!(x, Piece::Field(_)));

    while let Some(line) = lines.next_line().await? {
        let parts = has_parts.then(|| delimiter.split(&line).collect::<Vec<_>>());
        let formatted = format_line(&format_string, &line, parts.as_deref());
        println!("{}", formatted);
    }

    Ok(())
}

fn enrich_stream(
    file: FileOrStd,
    command_line: String,
    delimiter: Regex,
) -> impl Stream<Item = Result<(String, String)>> {
    try_stream! {
        let reader = file.open_read().await?;
        let mut lines = reader.lines();

        let format_string = parse_format_string(&command_line)?;
        let has_parts = format_string.iter().any(|x| matches!(x, Piece::Field(_)));

        while let Some(line) = lines.next_line().await? {
            let parts = has_parts.then(|| delimiter.split(&line).collect::<Vec<_>>());
            let cmd_line = format_line(&format_string, &line, parts.as_deref());
            yield (line, cmd_line);
        }
    }
}

fn exit_with(exit_status: ExitStatus) -> ! {
    use std::os::unix::process::ExitStatusExt;
    use std::process::exit;

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

pub async fn enrich(
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
                use std::fmt::Write;
                write!(line, "{}{}", delimiter, field)?;
            }

            println!("{}", line);

            Ok::<_, anyhow::Error>(())
        })
        .await?;

    Ok(())
}

/// Parse field selector like "1", "1,3,5", "2-4", or "1,2-4,6"
fn parse_field_selector(selector: &str) -> Result<Vec<usize>> {
    let mut fields = Vec::new();

    for part in selector.split(',') {
        let part = part.trim();
        if part.contains('-') {
            // Range like "2-4"
            let range_parts: Vec<&str> = part.split('-').collect();
            if range_parts.len() != 2 {
                anyhow::bail!("Invalid range syntax: {}", part);
            }
            let start: usize = range_parts[0].trim().parse()
                .map_err(|_| anyhow::anyhow!("Invalid number: {}", range_parts[0]))?;
            let end: usize = range_parts[1].trim().parse()
                .map_err(|_| anyhow::anyhow!("Invalid number: {}", range_parts[1]))?;

            if start == 0 || end == 0 {
                anyhow::bail!("Field indices must be >= 1");
            }
            if start > end {
                anyhow::bail!("Invalid range: {} > {}", start, end);
            }

            for i in start..=end {
                fields.push(i);
            }
        } else {
            // Single field
            let n: usize = part.parse()
                .map_err(|_| anyhow::anyhow!("Invalid field number: {}", part))?;
            if n == 0 {
                anyhow::bail!("Field indices must be >= 1 (use '0' alone to print whole line)");
            }
            fields.push(n);
        }
    }

    Ok(fields)
}

pub async fn field(
    file: FileOrStd,
    selector: &str,
    delimiter: Regex,
    output_delimiter: String,
) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    // Special case: "0" means print whole line
    if selector == "0" {
        while let Some(line) = lines.next_line().await? {
            println!("{}", line);
        }
        return Ok(());
    }

    let field_indices = parse_field_selector(selector)?;

    while let Some(line) = lines.next_line().await? {
        let parts: Vec<&str> = delimiter.split(&line).collect();
        let selected: Vec<&str> = field_indices
            .iter()
            .map(|&idx| parts.get(idx - 1).copied().unwrap_or(""))
            .collect();

        println!("{}", selected.join(&output_delimiter));
    }

    Ok(())
}

pub async fn unnest(file: FileOrStd, delimiter: Regex) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        for part in delimiter.split(&line) {
            println!("{}", part);
        }
    }

    Ok(())
}

pub async fn replace(file: FileOrStd, regex: Regex, replacement: &str) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        let replaced = regex.replace_all(&line, replacement);
        println!("{}", replaced);
    }

    Ok(())
}

pub async fn join(file: FileOrStd, delimiter: String) -> Result<()> {
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

#[derive(Debug)]
pub struct WhereConditions {
    pub eq: Option<String>,
    pub ne: Option<String>,
    pub lt: Option<String>,
    pub le: Option<String>,
    pub gt: Option<String>,
    pub ge: Option<String>,
    pub contains: Option<String>,
    pub matches: Option<String>,
}

pub async fn where_filter(
    file: FileOrStd,
    field: usize,
    conditions: WhereConditions,
    delimiter: Regex,
) -> Result<()> {
    // Collect all specified conditions
    let mut condition_list = Vec::new();

    if let Some(value) = conditions.eq {
        condition_list.push(WhereCondition::new(field, CompareOp::Eq, value)?);
    }
    if let Some(value) = conditions.ne {
        condition_list.push(WhereCondition::new(field, CompareOp::Ne, value)?);
    }
    if let Some(value) = conditions.lt {
        condition_list.push(WhereCondition::new(field, CompareOp::Lt, value)?);
    }
    if let Some(value) = conditions.le {
        condition_list.push(WhereCondition::new(field, CompareOp::Le, value)?);
    }
    if let Some(value) = conditions.gt {
        condition_list.push(WhereCondition::new(field, CompareOp::Gt, value)?);
    }
    if let Some(value) = conditions.ge {
        condition_list.push(WhereCondition::new(field, CompareOp::Ge, value)?);
    }
    if let Some(value) = conditions.contains {
        condition_list.push(WhereCondition::new(field, CompareOp::Contains, value)?);
    }
    if let Some(value) = conditions.matches {
        condition_list.push(WhereCondition::new(field, CompareOp::Matches, value)?);
    }

    if condition_list.is_empty() {
        return Err(anyhow::anyhow!(
            "No conditions specified. Use --eq, --gt, --contains, etc."
        ));
    }

    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        let parts: Vec<&str> = delimiter.split(&line).collect();

        // All conditions must be true (AND logic)
        if condition_list
            .iter()
            .all(|condition| condition.evaluate(&line, &parts))
        {
            println!("{}", line);
        }
    }

    Ok(())
}

pub async fn sort_lines(
    file: FileOrStd,
    field: usize,
    sort_type: SortType,
    reverse: bool,
    delimiter: Regex,
) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();
    let mut entries = Vec::new();

    // Collect all lines
    while let Some(line) = lines.next_line().await? {
        let parts: Vec<String> = delimiter.split(&line).map(|s| s.to_string()).collect();
        entries.push((line, parts));
    }

    // Sort the entries
    entries.sort_by(|(line_a, parts_a), (line_b, parts_b)| {
        let value_a = if field == 0 {
            line_a.as_str()
        } else if field <= parts_a.len() {
            &parts_a[field - 1]
        } else {
            ""
        };

        let value_b = if field == 0 {
            line_b.as_str()
        } else if field <= parts_b.len() {
            &parts_b[field - 1]
        } else {
            ""
        };

        let cmp = match sort_type {
            SortType::String => value_a.cmp(value_b),
            SortType::Numeric => match (value_a.parse::<f64>(), value_b.parse::<f64>()) {
                (Ok(a), Ok(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
                (Ok(_), Err(_)) => Ordering::Less,
                (Err(_), Ok(_)) => Ordering::Greater,
                (Err(_), Err(_)) => value_a.cmp(value_b),
            },
        };

        if reverse {
            cmp.reverse()
        } else {
            cmp
        }
    });

    // Output sorted lines
    for (line, _) in entries {
        println!("{}", line);
    }

    Ok(())
}

#[derive(Debug)]
pub struct GroupAggregations {
    pub count: bool,
    pub distinct_count: Option<usize>,
    pub sum: Option<usize>,
    pub avg: Option<usize>,
    pub min: Option<usize>,
    pub max: Option<usize>,
    pub first: Option<usize>,
    pub last: Option<usize>,
    pub values: Option<usize>,
    pub distinct_values: Option<usize>,
}

pub async fn group_by(
    file: FileOrStd,
    group_field: usize,
    aggregations: GroupAggregations,
    delimiter: Regex,
    output_delimiter: String,
) -> Result<()> {
    let reader = file.open_read().await?;
    let mut lines = reader.lines();

    // Collect all aggregation fields we need to track
    let mut agg_fields = Vec::new();
    if aggregations.count {
        agg_fields.push((AggregateFunction::Count, None));
    }
    if let Some(field) = aggregations.distinct_count {
        agg_fields.push((AggregateFunction::DistinctCount, Some(field)));
    }
    if let Some(field) = aggregations.sum {
        agg_fields.push((AggregateFunction::Sum, Some(field)));
    }
    if let Some(field) = aggregations.avg {
        agg_fields.push((AggregateFunction::Avg, Some(field)));
    }
    if let Some(field) = aggregations.min {
        agg_fields.push((AggregateFunction::Min, Some(field)));
    }
    if let Some(field) = aggregations.max {
        agg_fields.push((AggregateFunction::Max, Some(field)));
    }
    if let Some(field) = aggregations.first {
        agg_fields.push((AggregateFunction::First, Some(field)));
    }
    if let Some(field) = aggregations.last {
        agg_fields.push((AggregateFunction::Last, Some(field)));
    }
    if let Some(field) = aggregations.values {
        agg_fields.push((AggregateFunction::Values, Some(field)));
    }
    if let Some(field) = aggregations.distinct_values {
        agg_fields.push((AggregateFunction::DistinctValues, Some(field)));
    }

    // If no aggregations specified, default to dedup (just group)
    let is_dedup_only = agg_fields.is_empty();

    // Store all lines for each group
    let mut group_lines: HashMap<String, Vec<Vec<String>>> = HashMap::new();

    // Collect and group data
    while let Some(line) = lines.next_line().await? {
        let parts: Vec<&str> = delimiter.split(&line).collect();

        let group_key = if group_field == 0 {
            line.clone()
        } else if group_field <= parts.len() {
            parts[group_field - 1].to_string()
        } else {
            String::new()
        };

        if is_dedup_only {
            // For dedup, just track unique group keys
            group_lines.entry(group_key.clone()).or_default();
        } else {
            // Store the full line parts for aggregation
            let parts_owned: Vec<String> = parts.iter().map(|s| s.to_string()).collect();
            group_lines.entry(group_key).or_default().push(parts_owned);
        }
    }

    // Output results
    if is_dedup_only {
        // Just output unique group keys (dedup behavior)
        for group_key in group_lines.keys() {
            println!("{}", group_key);
        }
    } else {
        // Calculate and output aggregations
        for (group_key, lines_in_group) in group_lines {
            let mut results = vec![group_key];

            for (agg_func, agg_field) in &agg_fields {
                let values: Vec<String> = if let Some(field) = agg_field {
                    lines_in_group
                        .iter()
                        .filter_map(|parts| {
                            if *field == 0 {
                                Some(parts.join(&output_delimiter))
                            } else if *field <= parts.len() {
                                Some(parts[*field - 1].clone())
                            } else {
                                None
                            }
                        })
                        .collect()
                } else {
                    // For count, we don't need field values
                    vec![]
                };

                let result = match agg_func {
                    AggregateFunction::Count => lines_in_group.len().to_string(),
                    AggregateFunction::DistinctCount => {
                        let distinct: HashSet<String> = values.into_iter().collect();
                        distinct.len().to_string()
                    }
                    AggregateFunction::Sum => {
                        let sum: f64 = values.iter().filter_map(|v| v.parse::<f64>().ok()).sum();
                        sum.to_string()
                    }
                    AggregateFunction::Avg => {
                        let nums: Vec<f64> = values
                            .iter()
                            .filter_map(|v| v.parse::<f64>().ok())
                            .collect();
                        if nums.is_empty() {
                            "0".to_string()
                        } else {
                            (nums.iter().sum::<f64>() / nums.len() as f64).to_string()
                        }
                    }
                    AggregateFunction::Min => values
                        .iter()
                        .filter_map(|v| v.parse::<f64>().ok())
                        .fold(f64::INFINITY, f64::min)
                        .to_string(),
                    AggregateFunction::Max => values
                        .iter()
                        .filter_map(|v| v.parse::<f64>().ok())
                        .fold(f64::NEG_INFINITY, f64::max)
                        .to_string(),
                    AggregateFunction::First => values.first().unwrap_or(&String::new()).clone(),
                    AggregateFunction::Last => values.last().unwrap_or(&String::new()).clone(),
                    AggregateFunction::Values => values.join(","),
                    AggregateFunction::DistinctValues => {
                        let distinct: HashSet<String> = values.into_iter().collect();
                        let mut distinct_vec: Vec<String> = distinct.into_iter().collect();
                        distinct_vec.sort();
                        distinct_vec.join(",")
                    }
                };

                results.push(result);
            }

            println!("{}", results.join(&output_delimiter));
        }
    }

    Ok(())
}

pub async fn window(file: FileOrStd, max_lines: usize, refresh_ms: u64) -> Result<()> {
    use crossterm::{cursor, execute, style::Print};
    use std::collections::VecDeque;
    use std::io::{stderr, IsTerminal};
    use std::time::{Duration, Instant};
    use tokio::io::BufReader;
    use tokio::time::interval;

    // Only enable display if stderr is a TTY
    let is_interactive = stderr().is_terminal();

    let reader = file.open_read().await?;
    let mut buf_reader = BufReader::new(reader);

    // Statistics tracking
    let mut total_lines = 0u64;
    let start_time = Instant::now();
    let mut last_update = Instant::now();
    let mut lines_since_last_update = 0u64;
    let mut current_rate = 0.0;

    // Circular buffer for recent lines
    let mut recent_lines: VecDeque<String> = VecDeque::with_capacity(max_lines);

    // Setup refresh timer
    let mut update_interval = interval(Duration::from_millis(refresh_ms));

    // Track display state
    let mut display_written = false;
    let mut display_height = 0usize;

    // Reserve space for our display at the bottom (only if interactive)
    if is_interactive {
        for _ in 0..(max_lines + 1) {
            eprintln!(); // Create space below the command
        }
    }

    loop {
        tokio::select! {
            // Read from input
            read_result = async {
                let mut buffer = Vec::new();
                let result: Result<Option<String>, anyhow::Error> = match buf_reader.read_until(b'\n', &mut buffer).await {
                    Ok(0) => Ok(None), // EOF
                    Ok(_) => {
                        // Remove trailing newline if present
                        if buffer.ends_with(b"\n") {
                            buffer.pop();
                        }
                        // Convert to string, replacing invalid UTF-8
                        Ok(Some(String::from_utf8_lossy(&buffer).into_owned()))
                    }
                    Err(e) => Err(e.into()),
                };
                result
            } => {
                match read_result? {
                    Some(line) => {
                        // Pass through to stdout immediately
                        println!("{}", line);

                        // Update statistics
                        total_lines += 1;
                        lines_since_last_update += 1;

                        // Store for display (handle control chars safely)
                        let safe_line = line.chars()
                            .map(|c| if c.is_control() && c != '\t' { '�' } else { c })
                            .take(80) // Limit line length for display
                            .collect::<String>();

                        recent_lines.push_back(safe_line);
                        if recent_lines.len() > max_lines {
                            recent_lines.pop_front();
                        }
                    }
                    None => {
                        // End of input - clean up display and exit
                        if display_written && is_interactive {
                            // Move down past our display area
                            execute!(stderr(), cursor::MoveDown(display_height as u16))?;
                        }
                        break;
                    }
                }
            }

            // Update display
            _ = update_interval.tick() => {
                if !is_interactive {
                    continue;
                }

                let now = Instant::now();
                let elapsed_since_last = now.duration_since(last_update).as_secs_f64();

                if elapsed_since_last > 0.0 {
                    current_rate = lines_since_last_update as f64 / elapsed_since_last;
                    lines_since_last_update = 0;
                    last_update = now;
                }

                let elapsed_total = now.duration_since(start_time);

                // Move to our display area (reserved lines at bottom)
                execute!(stderr(), cursor::MoveUp((max_lines + 1) as u16))?;

                // Calculate current display height
                display_height = 1 + recent_lines.len(); // stats line + content lines

                // Write stats line
                execute!(
                    stderr(),
                    cursor::MoveToColumn(0),
                    crossterm::terminal::Clear(crossterm::terminal::ClearType::CurrentLine),
                    Print(format!("Processing: {} lines | {:.1} lines/sec | {:02}:{:02}:{:02}",
                        total_lines,
                        current_rate,
                        elapsed_total.as_secs() / 3600,
                        (elapsed_total.as_secs() % 3600) / 60,
                        elapsed_total.as_secs() % 60
                    )),
                    cursor::MoveDown(1),
                    cursor::MoveToColumn(0)
                )?;

                // Write recent lines
                for line in &recent_lines {
                    execute!(
                        stderr(),
                        crossterm::terminal::Clear(crossterm::terminal::ClearType::CurrentLine),
                        Print(line),
                        cursor::MoveDown(1),
                        cursor::MoveToColumn(0)
                    )?;
                }

                // Clear any remaining reserved lines
                for _ in recent_lines.len()..max_lines {
                    execute!(
                        stderr(),
                        crossterm::terminal::Clear(crossterm::terminal::ClearType::CurrentLine),
                        cursor::MoveDown(1),
                        cursor::MoveToColumn(0)
                    )?;
                }

                display_written = true;
            }
        }
    }

    Ok(())
}

pub async fn jgrep(
    file: FileOrStd,
    pattern: String,
    keys_only: bool,
    values_only: bool,
    ignore_case: bool,
    pretty: bool,
) -> Result<()> {
    use tokio::task;

    // Get the underlying std reader directly
    let std_reader = match file {
        FileOrStd::Std => Box::new(std::io::stdin()) as Box<dyn std::io::Read + Send>,
        FileOrStd::File(path) => {
            let file = std::fs::File::open(path)?;
            Box::new(file) as Box<dyn std::io::Read + Send>
        }
    };

    // Process with true streaming in blocking task
    let result = task::spawn_blocking(move || -> Result<bool> {
        let buf_reader = std::io::BufReader::new(std_reader);
        stream_json_filter(
            buf_reader,
            pattern,
            keys_only,
            values_only,
            ignore_case,
            pretty,
        )
    })
    .await??;

    if !result {
        // No matches found - exit with code 1 like grep
        std::process::exit(1);
    }

    Ok(())
}

fn stream_json_filter<R: std::io::BufRead>(
    reader: R,
    pattern: String,
    keys_only: bool,
    values_only: bool,
    ignore_case: bool,
    pretty: bool,
) -> Result<bool> {
    use serde_json::Deserializer;

    let mut has_any_matches = false;
    let mut deserializer = Deserializer::from_reader(reader);

    // Process each top-level JSON value from stream using raw deserialization
    loop {
        let filter = JsonFilter {
            pattern: &pattern,
            keys_only,
            values_only,
            ignore_case,
        };

        match filter.deserialize(&mut deserializer) {
            Ok(Some(result)) => {
                has_any_matches = true;
                let output = if pretty {
                    serde_json::to_string_pretty(&result)?
                } else {
                    serde_json::to_string(&result)?
                };
                println!("{}", output);
            }
            Ok(None) => {
                // This JSON value had no matches, continue to next
            }
            Err(e) if e.is_eof() => break,
            Err(e) => return Err(anyhow::anyhow!("JSON parse error: {}", e)),
        }
    }

    Ok(has_any_matches)
}

// Token-by-token streaming JSON filter that never materializes full input structures
struct JsonFilter<'a> {
    pattern: &'a str,
    keys_only: bool,
    values_only: bool,
    ignore_case: bool,
}

impl JsonFilter<'_> {
    fn matches_pattern(&self, text: &str) -> bool {
        if self.ignore_case {
            text.to_lowercase().contains(&self.pattern.to_lowercase())
        } else {
            text.contains(self.pattern)
        }
    }

    fn should_include_key(&self, key: &str) -> bool {
        !self.values_only && self.matches_pattern(key)
    }

    fn should_include_value(&self, value: &str) -> bool {
        !self.keys_only && self.matches_pattern(value)
    }
}

impl<'de> DeserializeSeed<'de> for JsonFilter<'_> {
    type Value = Option<Value>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(FilterVisitor(self))
    }
}

struct FilterVisitor<'a>(JsonFilter<'a>);

impl<'de> Visitor<'de> for FilterVisitor<'_> {
    type Value = Option<Value>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> {
        if self.0.should_include_value(&v.to_string()) {
            Ok(Some(Value::Bool(v)))
        } else {
            Ok(None)
        }
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
        if self.0.should_include_value(&v.to_string()) {
            Ok(Some(Value::Number(v.into())))
        } else {
            Ok(None)
        }
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
        if self.0.should_include_value(&v.to_string()) {
            Ok(Some(Value::Number(v.into())))
        } else {
            Ok(None)
        }
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
        if self.0.should_include_value(&v.to_string()) {
            if let Some(n) = serde_json::Number::from_f64(v) {
                Ok(Some(Value::Number(n)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
        if self.0.should_include_value(v) {
            Ok(Some(Value::String(v.to_string())))
        } else {
            Ok(None)
        }
    }

    fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
    where
        V: MapAccess<'de>,
    {
        let mut result_map = serde_json::Map::new();
        let mut has_matches = false;

        // Process each key-value pair without loading full values into memory
        while let Some(key) = map.next_key::<String>()? {
            let key_matches = self.0.should_include_key(&key);

            if key_matches {
                // Key matches - deserialize and include the full value
                let full_value: Value = map.next_value()?;
                result_map.insert(key, full_value);
                has_matches = true;
            } else {
                // Key doesn't match - use streaming filter on the value
                let filter = JsonFilter {
                    pattern: self.0.pattern,
                    keys_only: self.0.keys_only,
                    values_only: self.0.values_only,
                    ignore_case: self.0.ignore_case,
                };

                match map.next_value_seed(filter)? {
                    Some(filtered_value) => {
                        result_map.insert(key, filtered_value);
                        has_matches = true;
                    }
                    None => {
                        // Value had no matches, skip it
                    }
                }
            }
        }

        if has_matches {
            Ok(Some(Value::Object(result_map)))
        } else {
            Ok(None)
        }
    }

    fn visit_seq<V>(self, mut seq: V) -> Result<Self::Value, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let mut result_array = Vec::new();
        let mut has_matches = false;

        // Process each array element without loading full array into memory
        while let Some(element) = seq.next_element_seed(JsonFilter {
            pattern: self.0.pattern,
            keys_only: self.0.keys_only,
            values_only: self.0.values_only,
            ignore_case: self.0.ignore_case,
        })? {
            if let Some(filtered_element) = element {
                result_array.push(filtered_element);
                has_matches = true;
            }
        }

        if has_matches {
            Ok(Some(Value::Array(result_array)))
        } else {
            Ok(None)
        }
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        if self.0.should_include_value("null") {
            Ok(Some(Value::Null))
        } else {
            Ok(None)
        }
    }
}

pub async fn transpose(
    file: FileOrStd,
    delimiter: Regex,
    output_delimiter: String,
) -> Result<()> {
    let mut reader = file.open_read().await?;
    let mut lines = Vec::new();
    let mut line = String::new();

    // Read all lines and parse into columns
    while reader.read_line(&mut line).await? > 0 {
        let trimmed = line.trim_end();
        if !trimmed.is_empty() {
            let columns: Vec<&str> = delimiter.split(trimmed).collect();
            lines.push(columns.into_iter().map(|s| s.to_string()).collect::<Vec<_>>());
        }
        line.clear();
    }

    if lines.is_empty() {
        return Ok(());
    }

    // Find the maximum number of columns
    let max_cols = lines.iter().map(|line| line.len()).max().unwrap_or(0);

    // Transpose: for each column index, collect values from all rows
    for col_idx in 0..max_cols {
        let transposed_row: Vec<String> = lines
            .iter()
            .map(|row| {
                row.get(col_idx)
                    .map(|s| s.as_str())
                    .unwrap_or("")
                    .to_string()
            })
            .collect();

        println!("{}", transposed_row.join(&output_delimiter));
    }

    Ok(())
}

pub async fn regexify(
    file: FileOrStd,
    digits: bool,
    words: bool,
    repetitions: bool,
    escape_non_ascii: bool,
    case_insensitive: bool,
    non_capturing_groups: bool,
    verbose: bool,
    anchors: bool,
) -> Result<()> {
    let mut reader = file.open_read().await?;
    let mut test_cases = Vec::new();
    let mut line = String::new();

    // Read all test cases (one per line)
    while reader.read_line(&mut line).await? > 0 {
        let trimmed = line.trim_end();
        if !trimmed.is_empty() {
            test_cases.push(trimmed.to_string());
        }
        line.clear();
    }

    if test_cases.is_empty() {
        return Ok(());
    }

    // Build regex using grex
    let mut builder = grex::RegExpBuilder::from(&test_cases);

    if digits {
        builder.with_conversion_of_digits();
    }
    if words {
        builder.with_conversion_of_words();
    }
    if repetitions {
        builder.with_conversion_of_repetitions();
    }
    if escape_non_ascii {
        builder.with_escaping_of_non_ascii_chars(false);
    }
    if case_insensitive {
        builder.with_case_insensitive_matching();
    }
    if non_capturing_groups {
        // Non-capturing is the default, so we only enable capturing groups when flag is NOT set
    } else {
        builder.with_capturing_groups();
    }
    if verbose {
        builder.with_verbose_mode();
    }
    if !anchors {
        builder.without_anchors();
    }

    let regex = builder.build();
    println!("{}", regex);

    Ok(())
}

pub async fn lookup(
    input_file: FileOrStd,
    lookup_file: FileOrStd,
    lookup_field: usize,
    input_field: Option<usize>,
    enrich: bool,
    output_delimiter: String,
    delimiter: Regex,
) -> Result<()> {
    // Read lookup file into HashMap
    let mut lookup_map: HashMap<String, String> = HashMap::new();
    let lookup_reader = lookup_file.open_read().await?;
    let mut lookup_lines = lookup_reader.lines();

    while let Some(line) = lookup_lines.next_line().await? {
        let parts: Vec<&str> = delimiter.split(&line).collect();

        if lookup_field == 0 {
            // Use whole line as key
            lookup_map.insert(line.clone(), line);
        } else if let Some(key) = parts.get(lookup_field - 1) {
            // Use specified field as key, store whole line as value
            lookup_map.insert(key.to_string(), line);
        }
    }

    // Process input file
    let input_reader = input_file.open_read().await?;
    let mut input_lines = input_reader.lines();

    while let Some(line) = input_lines.next_line().await? {
        let search_key = if let Some(field_num) = input_field {
            if field_num == 0 {
                line.clone()
            } else {
                let parts: Vec<&str> = delimiter.split(&line).collect();
                parts.get(field_num - 1)
                    .map(|s| s.to_string())
                    .unwrap_or_default()
            }
        } else {
            // No input field specified, use whole line
            line.clone()
        };

        // Lookup and print result
        if let Some(result) = lookup_map.get(&search_key) {
            if enrich {
                println!("{}{}{}", line, output_delimiter, result);
            } else {
                println!("{}", result);
            }
        } else {
            if enrich {
                println!("{}", line);
            } else {
                println!();
            }
        }
    }

    Ok(())
}
