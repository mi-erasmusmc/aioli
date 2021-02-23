use std::fs::File;
use std::io::{BufReader, Write};
use std::{env, fs};

use deadpool::managed::Object;
use deadpool_postgres::tokio_postgres::Error;
use deadpool_postgres::{ClientWrapper, Pool};
use futures::{StreamExt, TryStreamExt};
use select::document::Document;
use select::predicate::Name;
use std::process::Command;
use std::thread::current;
use tempfile::{Builder, TempDir};
use tokio::io::copy;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use walkdir::WalkDir;

pub async fn download_source_data(pool: &Pool) {
    let res = reqwest::get("https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html")
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let html = Document::from(res.as_str());

    let current_links: Vec<&str> = html
        .find(Name("a"))
        .filter_map(|n| n.attr("href"))
        .filter(|link| link.contains("faers_ascii"))
        .collect();

    let legacy_links: Vec<&str> = html
        .find(Name("a"))
        .filter_map(|n| n.attr("href"))
        .filter(|link| !link.contains("faers_ascii"))
        .filter(|link| link.contains("aers_ascii"))
        .collect();

    println!("{:?}", current_links);
    println!("{:?}", legacy_links);

    fs::create_dir_all("source_zip").unwrap();
    fs::create_dir_all("source_files").unwrap();

    let mid = current_links.len() / 2;
    let (c_left, c_right) = current_links.split_at(mid);
    let mid = legacy_links.len() / 2;
    let (l_left, l_right) = legacy_links.split_at(mid);

    let client_l = pool.get().await.unwrap();
    let client_r = pool.get().await.unwrap();

    tokio::join!(
        // process_current_links(c_left),
        // process_current_links(c_right),
        process_current_links(l_left, &client_l),
        process_current_links(l_right, &client_r)
    );
}

async fn process_current_links(links: &[&str], client: &Object<ClientWrapper, Error>) {
    for link in links {
        process_current(link, &client).await;
    }
}

async fn process_legacy_links(links: &[&str], client: &Object<ClientWrapper, Error>) {
    for link in links {
        process_current(link, &client).await;
    }
}

async fn process_current(link: &str, client: &Object<ClientWrapper, Error>) {
    let mut response = reqwest::get(link).await.unwrap();

    let mut path: String = {
        let mut fname = response
            .url()
            .path_segments()
            .and_then(|segments| segments.last())
            .and_then(|name| if name.is_empty() { None } else { Some(name) })
            .unwrap();
        println!("Downloading: '{}'", fname);
        String::from(fname)
    };

    let outfile = std::path::PathBuf::from(format!("source_zip/{}", path));
    let outfile = tokio::fs::File::create(outfile).await.unwrap();
    let mut outfile = tokio::io::BufWriter::new(outfile);

    while let Some(chunk) = response.chunk().await.unwrap() {
        outfile.write(&chunk).await.unwrap();
    }

    // Must flush tokio::io::BufWriter manually.
    // It does not flush itself automatically when dropped.
    outfile.flush().await.unwrap();

    unzip(&path, &client).await;
}

async fn unzip(name: &str, client: &Object<ClientWrapper, Error>) {
    let file = std::fs::File::open(format!("source_zip/{}", name)).unwrap();
    let reader = BufReader::new(file);
    let name = name.replace(".zip", "");
    let target_dir = format!("source_files/{}", name);
    println!("extracting into {}", target_dir);
    fs::create_dir_all(&target_dir);
    zip::ZipArchive::new(reader)
        .unwrap()
        .extract(&target_dir)
        .unwrap();

    for entry in WalkDir::new(&target_dir)
        .follow_links(true)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let f_path = entry.path().as_os_str().to_str().unwrap();
        let f_name = entry.file_name().to_string_lossy().to_lowercase();
        //  let sec = entry.metadata()?.modified()?;

        if f_name.to_lowercase().contains(".txt") {
            clean(&f_path);

            if f_name.to_lowercase().contains("demo") {
                println!("demo: {:?}", f_name);
                clean_dollar(&f_path);
                populate_db("demo", &f_path, &client).await;
            } else if f_name.to_lowercase().contains("indi") {
                println!("indi: {:?}", f_name);
                populate_db("indi", &f_path, &client).await;
            }
        }
    }
}

fn clean(path: &str) {
    println!("Cleaning {}", path);

    Command::new("sd")
        .arg("'\r'")
        .arg("''")
        .arg(path)
        .output()
        .expect("failed to execute process");
}

fn clean_dollar(path: &str) {
    println!("Cleaning {}", path);

    Command::new("sd")
        .arg("'$\n'")
        .arg("'\n'")
        .arg(path)
        .output()
        .expect("failed to execute process");
}

async fn populate_db(target: &str, path: &str, client: &Object<ClientWrapper, Error>) {
    let pwd = env::current_dir().unwrap();
    let pwd = pwd.to_str().unwrap();
    let path = format!("'{}/{}'", pwd, path);
    println!("{}", path);
    let query = format!(
        "COPY faers.{}_legacy FROM {} WITH DELIMITER E'$' CSV HEADER QUOTE E'\\b'",
        target, path
    );
    let result = client.execute(query.as_str(), &[]).await.unwrap();
    println!("done the db thing {}", result)
}
