use std::{fmt::Display, path::PathBuf, pin::Pin};

use anyhow::{Context, Result};
use tokio::io::{AsyncBufRead, AsyncWrite};

#[derive(Default, Clone, Debug)]
pub enum FileOrStd {
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
