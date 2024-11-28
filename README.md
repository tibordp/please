My personal shell toolkit

# Usage
```
Usage: please <COMMAND>

Commands:
  merge      Merge two or more files (like cat, but streaming)
  union      Find the union of two or more files
  intersect  Find the intersection of two or more files
  subtract   Subtract all the files from the first file
  zip        Zip the files together with a delimiter
  unzip      Split columns into separate files
  sample     Sample lines from a file
  tally      Tally the # of occurrences of each line (sort | uniq -c)
  replace    Replaces with a regex (sed, but sane)
  topk       Similar to tally, but only print the top k lines
  skip       Skip first n lines from a file
  take       Take first n lines from a file
  prepend    Prepend a string to each line
  append     Append a string to each line
  format     Print each line with a format string
  enrich     Execute a command for each line and append the output as a new column
  extract    Extract regex capturing groups from each line
  field      Print the n-th column of a file
  unnest     Turn columns into rows
  join       Join the lines with a separator
  cache      Cache the output of a command
  clip       Copy to the clipboard
  clop       Paste from the clipboard
  install    Install the commands as symlinks
  complete   Generate shell completions
  help       Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

# Installation
```sh
cargo install --git https://github.com/tibordp/please.git

# If you'd like to have subcommands in your PATH (NOTE: some of them clash with coreutils)
cd ~/.cargo/bin
./please install
```
