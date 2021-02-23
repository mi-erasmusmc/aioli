use std::collections::{BTreeMap, HashMap};

use deadpool::managed::Object;
use deadpool_postgres::tokio_postgres::Statement;
use deadpool_postgres::{ClientWrapper, Pool};
use rawsql::Loader;
use tokio_postgres::Row;

use crate::db::execute;

// sql file paths
const CONCEPT_IDS_SQL_PATH: &'static str = "sql/update_concept_ids.sql";

// rows
const CONCEPT_ID: &'static str = "concept_id";

// queries
const UPDATE_ID: &'static str = "update_single_id";
const UPDATE_NAME: &'static str = "update_for_drug_n";
const FIND_ALL_ID: &'static str = "find_all";
const FIND_BEST: &'static str = "find_best";
const FIND_SECOND_BEST: &'static str = "find_second_best";
const FIND_FINAL: &'static str = "find_final";
const FIND_FOR_STRING_MATCHING: &'static str = "find_for_string_matching";
const UPDATE_IDS: &'static str = "update_concept_ids";
const FIND_RXNORM_MULTI: &'static str = "select_multiple_rxnorm";

pub async fn populate_concept_ids(pool: &Pool) {
    println!("Updating concept ids based on rxcuis");

    let queries = Loader::get_queries_from(CONCEPT_IDS_SQL_PATH)
        .unwrap()
        .queries;
    let client = pool.get().await.unwrap();
    execute(UPDATE_IDS, &client, &queries).await;

    manual_conversions(&client).await;

    let rows = client
        .query(queries.get(FIND_RXNORM_MULTI).unwrap().as_str(), &[])
        .await
        .unwrap();
    println!("Found {} unmapped comma-separated rxcuis", rows.len());

    // Split result and concurrently do the updates on the drugs with multiple rxcuis
    let mid = rows.len() / 2;
    let (left, right) = &rows.split_at(mid);
    let client_r = pool.get().await.unwrap();
    let client_l = pool.get().await.unwrap();
    tokio::join!(
        process_rxcuis(right, client_r),
        process_rxcuis(left, client_l)
    );
    println!("Done updating the multiple rxcuis!");

    println!("Updating concept ids for rxnorm tty MIN...");
    execute("multingr_to_clinical_drug_form", &client, &queries).await;
}

async fn process_rxcuis(rows: &[Row], client: Object<ClientWrapper, tokio_postgres::Error>) {
    let queries = Loader::get_queries_from(CONCEPT_IDS_SQL_PATH)
        .unwrap()
        .queries;

    // Prepare all the statements before iterating over the list to save resources...
    let mut stmts = HashMap::new();
    stmts.insert(
        UPDATE_ID,
        client
            .prepare(queries.get(UPDATE_ID).unwrap().as_str())
            .await
            .unwrap(),
    );
    stmts.insert(
        UPDATE_NAME,
        client
            .prepare(queries.get(UPDATE_NAME).unwrap().as_str())
            .await
            .unwrap(),
    );
    stmts.insert(
        FIND_ALL_ID,
        client
            .prepare(queries.get(FIND_ALL_ID).unwrap().as_str())
            .await
            .unwrap(),
    );
    stmts.insert(
        FIND_BEST,
        client
            .prepare(queries.get(FIND_BEST).unwrap().as_str())
            .await
            .unwrap(),
    );
    stmts.insert(
        FIND_SECOND_BEST,
        client
            .prepare(queries.get(FIND_SECOND_BEST).unwrap().as_str())
            .await
            .unwrap(),
    );
    stmts.insert(
        FIND_FINAL,
        client
            .prepare(queries.get(FIND_FINAL).unwrap().as_str())
            .await
            .unwrap(),
    );
    stmts.insert(
        FIND_FOR_STRING_MATCHING,
        client
            .prepare(queries.get(FIND_FOR_STRING_MATCHING).unwrap().as_str())
            .await
            .unwrap(),
    );

    for row in rows {
        select_from_list(row.get("rxcui"), &client, &stmts).await;
    }
}

async fn select_from_list(
    rxcuis: String,
    db_client: &Object<ClientWrapper, tokio_postgres::Error>,
    stmts: &HashMap<&str, Statement>,
) {
    let rxs: Vec<&str> = rxcuis.split(',').collect();

    let rows = db_client
        .query(stmts.get(FIND_ALL_ID).unwrap(), &[&rxs])
        .await
        .unwrap();
    match rows.len() {
        1 => {
            set_concept_id(
                &rxcuis,
                db_client,
                rows[0].get(CONCEPT_ID),
                stmts.get(UPDATE_ID).unwrap(),
            )
            .await
        }
        0 => return,
        _ => {
            select_from_multiple_choice(&rxcuis, db_client, &rows, &stmts, 4).await;
            let rows = db_client
                .query(stmts.get(FIND_BEST).unwrap(), &[&rxs])
                .await
                .unwrap();
            match rows.len() {
                1 => {
                    set_concept_id(
                        &rxcuis,
                        db_client,
                        rows[0].get(CONCEPT_ID),
                        stmts.get(UPDATE_ID).unwrap(),
                    )
                    .await
                }
                0 => {
                    let rows = db_client
                        .query(stmts.get(FIND_SECOND_BEST).unwrap(), &[&rxs])
                        .await
                        .unwrap();
                    match rows.len() {
                        1 => {
                            set_concept_id(
                                &rxcuis,
                                db_client,
                                rows[0].get(CONCEPT_ID),
                                stmts.get(UPDATE_ID).unwrap(),
                            )
                            .await
                        }
                        0 => {
                            let rows = db_client
                                .query(stmts.get(FIND_FINAL).unwrap(), &[&rxs])
                                .await
                                .unwrap();
                            match rows.len() {
                                1 => {
                                    set_concept_id(
                                        &rxcuis,
                                        db_client,
                                        rows[0].get(CONCEPT_ID),
                                        stmts.get(UPDATE_ID).unwrap(),
                                    )
                                    .await
                                }
                                0 => eprintln!("Could not find a decent concept for {}!", rxcuis),
                                _ => {
                                    select_from_multiple_choice(
                                        &rxcuis, db_client, &rows, &stmts, 6,
                                    )
                                    .await
                                }
                            }
                        }
                        _ => {
                            select_from_multiple_choice(&rxcuis, db_client, &rows, &stmts, 7).await
                        }
                    }
                }
                _ => select_from_multiple_choice(&rxcuis, db_client, &rows, &stmts, 8).await,
            }
        }
    }
}

async fn set_concept_id(
    ids: &str,
    client: &Object<ClientWrapper, tokio_postgres::Error>,
    concept_id: i32,
    stmt: &Statement,
) {
    client
        .execute(stmt, &[&concept_id, &ids])
        .await
        .expect("error setting concept id");
    println!("Updating {} with {}", ids, concept_id);
}

async fn select_from_multiple_choice(
    ids: &str,
    client: &Object<ClientWrapper, tokio_postgres::Error>,
    rows: &Vec<Row>,
    stmts: &HashMap<&str, Statement>,
    range: usize,
) {
    let drugs = client
        .query(stmts.get(FIND_FOR_STRING_MATCHING).unwrap(), &[&ids])
        .await
        .unwrap();
    for drug in drugs {
        let drug_name = drug.get("name");
        let mut comparsions: BTreeMap<usize, (i32, &str)> = BTreeMap::new();
        for row in rows {
            let concept_id = row.get(CONCEPT_ID);
            let concept_name = row.get("concept_name");
            let distance = distance::damerau_levenshtein(concept_name, drug_name);
            if distance < range {
                comparsions.insert(distance, (concept_id, concept_name));
            }
        }
        let result = comparsions.iter().next();
        match result {
            Some((_distance, (id, concept_name))) => {
                println!("Updating {} with {} {}", drug_name, concept_name, id);
                client
                    .execute(stmts.get(UPDATE_NAME).unwrap(), &[&id, &drug_name])
                    .await
                    .unwrap();
            }
            None => {}
        }
    }
}

async fn manual_conversions(client: &Object<ClientWrapper, tokio_postgres::Error>) {
    let queries = Loader::get_queries_from("sql/manual_matches.sql")
        .unwrap()
        .queries;
    for (name, sql) in queries {
        let result = client.execute(sql.as_str(), &[]).await.unwrap();
        println!("Executed {} {} rows affected", name, result);
    }
}
