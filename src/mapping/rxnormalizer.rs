use std::error::Error;
use std::time::Instant;
use std::{thread, time};

use deadpool::managed::Object;
use deadpool_postgres::{ClientWrapper, Pool};
use futures::future::join4;
use rawsql::Loader;
use tokio_postgres::Row;

const RXNAV_URL: &str = "https://rxnav.nlm.nih.gov/REST/rxcui.json";

pub async fn normalize(pool: &Pool, eu_only: bool) -> Result<(), Box<dyn Error>> {
    let db_client_1 = pool.get().await?;
    let db_client_2 = pool.get().await?;

    let queries = Loader::get_queries_from("./sql/rxnormalizer.sql")
        .unwrap()
        .queries;
    let stmt: &str;
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
            print_time_remaining(total, start, pos);
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

pub async fn normalize_exact(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let db_client_1 = pool.get().await?;
    let db_client_2 = pool.get().await?;
    let db_client_3 = pool.get().await?;
    let db_client_4 = pool.get().await?;
    let db_client_5 = pool.get().await?;
    let db_client_6 = pool.get().await?;
    let db_client_7 = pool.get().await?;
    let db_client_8 = pool.get().await?;

    let queries = Loader::get_queries_from("./sql/rxnormalizer.sql")
        .unwrap()
        .queries;

    let stmt_1 = queries.get("get_all_nulls_1").unwrap().as_str();
    let stmt_2 = queries.get("get_all_nulls_2").unwrap().as_str();
    let stmt_3 = queries.get("get_all_nulls_3").unwrap().as_str();
    let stmt_4 = queries
        .get("get_prod_ai_and_numeric_tail")
        .unwrap()
        .as_str();

    let rows_1 = db_client_1.query(stmt_1, &[]).await?;
    let rows_2 = db_client_1.query(stmt_2, &[]).await?;
    let rows_3 = db_client_1.query(stmt_3, &[]).await?;
    let rows_4 = db_client_1.query(stmt_4, &[]).await?;

    join4(
        split_rows_call_parallel(db_client_1, db_client_2, rows_1),
        split_rows_call_parallel(db_client_3, db_client_4, rows_2),
        split_rows_call_parallel(db_client_5, db_client_6, rows_3),
        split_rows_call_parallel(db_client_7, db_client_8, rows_4),
    )
    .await;

    Ok(())
}

async fn split_rows_call_parallel(
    db_client_1: Object<ClientWrapper, tokio_postgres::Error>,
    db_client_2: Object<ClientWrapper, tokio_postgres::Error>,
    rows: Vec<Row>,
) {
    let mid = rows.len() / 2;
    let (left, right) = rows.split_at(mid);

    // Calling rxnav concurrently, we get throttled at 20 requests per second, no point in adding additional clients.
    tokio::join!(
        call_rxnorm_exact(db_client_1, left),
        call_rxnorm_exact(db_client_2, right)
    );
}

async fn call_rxnorm_exact(db_client: Object<ClientWrapper, tokio_postgres::Error>, rows: &[Row]) {
    let web_client = reqwest::Client::new();
    let queries = Loader::get_queries_from("./sql/rxnormalizer.sql")
        .unwrap()
        .queries;
    let update_exact_stmt = db_client
        .prepare(queries.get("update_rxcui_exact").unwrap().as_str())
        .await
        .unwrap();
    let update_ingr_stmt = db_client
        .prepare(queries.get("set_ingredient").unwrap().as_str())
        .await
        .unwrap();
    let update_brand_stmt = db_client
        .prepare(queries.get("set_brand").unwrap().as_str())
        .await
        .unwrap();
    let update_scdf_stmt = db_client
        .prepare(queries.get("set_scdf").unwrap().as_str())
        .await
        .unwrap();
    let update_scdc_stmt = db_client
        .prepare(queries.get("set_scdc").unwrap().as_str())
        .await
        .unwrap();
    let total = rows.len();
    let start = Instant::now();
    for (pos, row) in rows.iter().enumerate() {
        if pos % 100 == 0 && start.elapsed().as_secs() > 1 {
            print_time_remaining(total, start, pos);
        }
        let drug_name_clean: String = row.get(0);
        // braces and brackets cause rxnav to crash so duping them...
        let drug = drug_name_clean.replace(&['{', '}', '[', ']'][..], "");

        let result = web_client
            .get(RXNAV_URL)
            .query(&[("name", &drug), ("search", &String::from("1"))])
            .send()
            .await;
        let res = match result {
            Ok(res) => res,
            Err(e) => {
                println!(
                    "Caught an error of kind {}, going to wait some seconds and try again",
                    e.to_string()
                );
                let sleepy_time = time::Duration::from_secs(5);
                thread::sleep(sleepy_time);
                web_client
                    .get(RXNAV_URL)
                    .query(&[("name", &drug), ("search", &String::from("1"))])
                    .send()
                    .await
                    .unwrap()
            }
        };
        let status = res.status();
        let body = res.text().await.unwrap();
        if status.is_success() {
            let rxnorm = json::parse(&body).unwrap();
            // rxnorm ids are returned as array, we will just store them as comma separated strings in the db
            let id = rxnorm["idGroup"]["rxnormId"]
                .dump()
                .replace(&['[', ']', '\"'][..], "");

            if !id.eq("null") && !id.contains(",") {
                let id_int = id.parse::<i32>().unwrap();
                let rows = db_client
                    .query(
                        "SELECT tty, str FROM faers.rxnconso WHERE rxcui = $1 and sab = 'RXNORM'",
                        &[&id_int],
                    )
                    .await
                    .unwrap();
                if rows.len() != 0 {
                    let ttys: Vec<&str> = rows.iter().map(|row| row.get(0)).collect();
                    let mut name: String = rows[0].get("str");
                    name = name.to_lowercase();
                    if ttys.contains(&"SCD") || ttys.contains(&"SBD") {
                        println!("Mapped '{}' to exact rxcui {}", drug, id);
                        db_client
                            .execute(&update_exact_stmt, &[&id, &drug_name_clean])
                            .await
                            .unwrap();
                    } else if ttys.contains(&"IN") || ttys.contains(&"MIN") || ttys.contains(&"PIN")
                    {
                        let mut name: String = rows[0].get("str");
                        name = name.to_lowercase();
                        println!("Mapped '{}' to ingredient {}", drug, name);
                        db_client
                            .execute(&update_ingr_stmt, &[&name, &drug_name_clean])
                            .await
                            .unwrap();
                    } else if ttys.contains(&"BN") {
                        println!("Mapped '{}' to {} {}", drug, id, name);
                        db_client
                            .execute(&update_brand_stmt, &[&name, &drug_name_clean])
                            .await
                            .unwrap();
                    } else if ttys.contains(&"SCDF") || ttys.contains(&"SBDF") {
                        println!("Mapped '{}' to {}  {}", drug, id, name);
                        db_client
                            .execute(&update_scdf_stmt, &[&id, &drug_name_clean])
                            .await
                            .unwrap();
                    } else if ttys.contains(&"SCDC") || ttys.contains(&"SBDC") {
                        println!("Mapped '{}' to {}  {}", drug, id, name);
                        db_client
                            .execute(&update_scdc_stmt, &[&id, &drug_name_clean])
                            .await
                            .unwrap();
                        // There are unclear issues handling params in this statement, so this is an ugly work around.
                        let stmt = format!(
                            "update faers.drug_mapping_exact dme
                        set rxcui = rx.rxcui
                        from faers.rxnconso rx
                        where drugname_clean = '{}'
                        and dme.rxcui is null
                        and dme.rx_dose_form is not null
                        and lower(rx.str) = lower(concat('{}', ' ', dme.rx_dose_form))
                        and rx.sab = 'RXNORM' and rx.tty = 'SCD'",
                            drug_name_clean, name
                        );
                        let result = db_client.execute(stmt.as_str(), &[]).await.unwrap();
                        println!("Converted SCDC {} to an SCD for {} rows", name, result);
                    }
                }
            }
        } else {
            println!("RxNav returned error status: {}", status)
        }
    }
}

fn print_time_remaining(total: usize, start: Instant, pos: usize) {
    let remaining = (total - pos) as u128;
    let avg = start.elapsed().as_millis() / pos as u128;
    let rate = 1000 / avg;
    let time_remaining = ((avg * remaining) / 1000) / 60;
    println!(
        "{} out of {}, {:?} minutes to go at a rate of {} requests per second",
        pos, total, time_remaining, rate
    );
}
