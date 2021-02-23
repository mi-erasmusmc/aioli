use deadpool_postgres::Pool;
use rawsql::Loader;

use crate::db::execute;

use std::error::Error;

mod exact_mapping;
mod mapper;
mod roll_up;
mod rx_to_standard;
mod rxnormalizer;
mod table_creator;

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

pub async fn create_final_tables(pool: &Pool) {
    println!("Creating the big final drug mapping table... takes a minute ... or 20");
    let queries = Loader::get_queries_from("sql/combined_drug_mapping.sql")
        .unwrap()
        .queries;
    let client = pool.get().await.unwrap();
    execute("drop_scdm_table", &client, &queries).await;
    execute("create_scdm_table", &client, &queries).await;
    execute("drop_standard_case_drug", &client, &queries).await;
    execute("create_standard_case_drug", &client, &queries).await;
}

pub async fn roll_up(pool: &Pool, split_multi: bool) {
    roll_up::do_roll_up(&pool, split_multi).await;
}
