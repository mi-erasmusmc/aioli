use std::error::Error;

use deadpool_postgres::Pool;
use rawsql::Loader;

use crate::db::execute;

mod exact_mapping;
mod mapper;
mod roll_up;
mod rx_to_standard;
mod rxnormalizer;
mod table_creator;

pub async fn map_atc(pool: &Pool) {
    exact_mapping::map(pool).await.unwrap();
}

pub async fn create_mapping_table(pool: &Pool) -> Result<(), Box<dyn Error>> {
    table_creator::create_mapping_table(pool).await?;
    Ok(())
}

pub async fn initial_basic_mapping(pool: &Pool) -> Result<(), Box<dyn Error>> {
    mapper::basic_mapping(pool).await?;
    Ok(())
}

pub async fn rxnormalize(pool: &Pool) -> Result<(), Box<dyn Error>> {
    rxnormalizer::normalize(&pool, false).await
}

pub async fn map_rx_to_cdm_concept_id(pool: &Pool) {
    rx_to_standard::populate_concept_ids(&pool).await
}

pub async fn run_original_aeolus(pool: &Pool) {
    mapper::run_aeolus_mapping(&pool).await
}

pub async fn create_final_tables(pool: &Pool, include_atc: bool) {
    println!("Creating the big final drug mapping table... takes a minute ... or 20");
    let queries = Loader::get_queries_from("sql/combined_drug_mapping.sql")
        .unwrap()
        .queries;
    let client = pool.get().await.unwrap();
    execute("drop_scdm_table", &client, &queries).await;
    execute("create_scdm_table", &client, &queries).await;
    execute("drop_standard_case_drug", &client, &queries).await;
    if include_atc {
        execute("expand_table_atc", &client, &queries).await;
        execute("add_atc_current", &client, &queries).await;
        execute("add_atc_legacy", &client, &queries).await;
        execute("populate_atc_from_regular_mapping", &client, &queries).await;
        execute(
            "populate_atc_from_regular_mapping_standard_id",
            &client,
            &queries,
        )
        .await;
        execute("populate_atc_from_regular_mapping_infer", &client, &queries).await;
        execute("set_vocab_id_atc", &client, &queries).await;
        execute("create_standard_case_drug_atc", &client, &queries).await;
    } else {
        execute("create_standard_case_drug_original", &client, &queries).await;
    }
}

pub async fn roll_up(pool: &Pool, split_multi: bool) {
    roll_up::do_roll_up(&pool, split_multi).await;
}
