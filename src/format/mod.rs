use anyhow::Result;
use std::fmt::Write;

#[derive(PartialEq, Eq, Debug)]
pub enum Piece {
    Literal(String),
    WholeLine,
    Field(isize),
}

pub fn parse_format_string(s: &str) -> Result<Vec<Piece>> {
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

pub fn format_line(format_string: &[Piece], line: &str, parts: Option<&[&str]>) -> String {
    let mut output = String::new();

    for piece in format_string {
        match piece {
            Piece::Literal(s) => {
                let _ = write!(output, "{}", s);
            }
            Piece::WholeLine | Piece::Field(0) => {
                let _ = write!(output, "{}", line);
            }
            Piece::Field(n) => {
                if let Some(parts) = parts {
                    let index: usize = match n {
                        n if *n < 0 => parts.len() as isize + n,
                        n => *n - 1,
                    } as _;

                    if index < parts.len() {
                        let _ = write!(output, "{}", parts[index]);
                    }
                }
            }
        }
    }

    output
}
