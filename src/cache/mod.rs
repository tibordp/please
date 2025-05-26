use std::{
    hash::Hasher,
    io::SeekFrom,
    mem::MaybeUninit,
    os::unix::{prelude::OsStrExt, process::ExitStatusExt},
    path::PathBuf,
    process::{exit, ExitStatus},
};

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub fn get_cache_key(command: &[String]) -> Result<String, anyhow::Error> {
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

pub async fn cache_command(command: Vec<String>, delete: bool, cache_dir: String) -> Result<()> {
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

pub async fn clip(
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

pub async fn clop(cache_dir: String, name: Option<String>, print: bool) -> Result<()> {
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