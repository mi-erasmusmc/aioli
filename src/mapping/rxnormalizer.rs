use std::time::Instant;

use deadpool::managed::Object;
use deadpool_postgres::{ClientWrapper, Pool};
use rawsql::Loader;
use std::error::Error;
use tokio_postgres::Row;

const RXNAV_URL: &str = "https://rxnav.nlm.nih.gov/REST/rxcui.json";

pub async fn normalize(pool: &Pool, eu_only: bool) -> Result<(), Box<dyn Error>> {
    let db_client_1 = pool.get().await?;
    let db_client_2 = pool.get().await?;

    let queries = Loader::get_queries_from("./sql/rxnormalizer.sql")
        .unwrap()
        .queries;
    let mut stmt: &str = "";
    if eu_only {
        stmt = queries.get("find_eu_drugs").unwrap().as_str();
    } else {
        stmt = queries.get("find_drugs").unwrap().as_str();
    }
    let rows = db_client_1.query(stmt, &[]).await?;

    let mid = rows.len() / 2;
    let (left, right) = rows.split_at(mid);

    // Calling rxnav concurrently, we get throttled at 20 requests per second, no point in adding additional clients.
    tokio::join!(
        call_rxnorm(db_client_1, left),
        call_rxnorm(db_client_2, right)
    );
    Ok(())
}

async fn call_rxnorm(db_client: Object<ClientWrapper, tokio_postgres::Error>, rows: &[Row]) {
    let web_client = reqwest::Client::new();
    let queries = Loader::get_queries_from("./sql/rxnormalizer.sql")
        .unwrap()
        .queries;
    let update_stmt = db_client
        .prepare(queries.get("set_rxcui").unwrap().as_str())
        .await
        .unwrap();
    let total = rows.len();
    let start = Instant::now();
    for (pos, row) in rows.iter().enumerate() {
        if pos % 100 == 0 && start.elapsed().as_secs() > 1 {
            let remaining = (total - pos) as u128;
            let avg = start.elapsed().as_millis() / pos as u128;
            let rate = 1000 / avg;
            let time_remaining = (avg * remaining) / 1000;
            println!(
                "{} out of {}, {:?} seconds to go at a rate of {} requests per second",
                pos, total, time_remaining, rate
            );
        }
        let drug_name_clean: String = row.get("drug_name_clean");
        // braces and brackets cause rxnav to crash so duping them...
        let drug = drug_name_clean.replace(&['{', '}', '[', ']'][..], "");

        let res = web_client
            .get(RXNAV_URL)
            .query(&[("name", &drug), ("search", &String::from("1"))])
            .send()
            .await
            .unwrap();
        let status = res.status();
        let body = res.text().await.unwrap();
        if status.is_success() {
            let rxnorm = json::parse(&body).unwrap();
            // rxnorm ids are returned as array, we will just store them as comma separated strings in the db
            let id = rxnorm["idGroup"]["rxnormId"]
                .dump()
                .replace(&['[', ']', '\"'][..], "");

            if !id.eq("null") {
                println!("Mapped '{}' to rxcui {}", drug, id);
                db_client
                    .execute(&update_stmt, &[&id, &drug_name_clean])
                    .await
                    .unwrap();
            }
        } else {
            println!("RxNav returned error status: {}", status)
        }
    }
}
