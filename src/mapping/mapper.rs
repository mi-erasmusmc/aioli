use deadpool_postgres::Pool;
use rawsql::Loader;

use crate::db::{execute, execute_param};
use crate::mapping::{map_rx_to_cdm_concept_id, rxnormalize, rxnormalizer};
use std::env;
use std::error::Error;

pub async fn basic_mapping(pool: &Pool) -> Result<(), Box<dyn Error>> {
    {
        let client = pool.get().await.unwrap();
        let queries = Loader::get_queries_from("sql/initial_mapping.sql")
            .unwrap()
            .queries;

        execute_param(
            "find_match_on_rxconso",
            &client,
            &queries,
            "Initial mapping",
        ).await;
    }
    let mut result: u64 = 1;
    while result != 0 {
        result = clean_and_match(pool).await;
    }
    Ok(())
}

pub async fn run_aeolus_mapping(pool: &Pool) {
    // let mut result: u64 = 1;
    // while result != 0 {
    //     result = remove_keywords(pool).await;
    // }
    // map_rx_to_cdm_concept_id(&pool).await;
    //   drug_name_parenthsis(&pool).await;
    // eu_drug_name(&pool).await;
    // ingredient_matching(&pool).await;
    // active_ingredient_mapping(&pool).await;
    // nda_ingredient_mapping(&pool).await;
    manual_mapping(&pool).await;
}

async fn clean_and_match(pool: &Pool) -> u64 {
    let client = pool.get().await.unwrap();
    {
        let queries = Loader::get_queries_from("sql/cleaning.sql")
            .unwrap()
            .queries;

        for (name, sql) in queries {
            let result = client.execute(sql.as_str(), &[]).await.unwrap();
            println!("Executed {} {} rows affected", name, result);
        }
    }
    let queries = Loader::get_queries_from("sql/initial_mapping.sql")
        .unwrap()
        .queries;

    execute_param(
        "find_match_on_rxconso",
        &client,
        &queries,
        "Initial mapping",
    ).await
}

async fn remove_keywords(pool: &Pool) -> u64 {
    let result_1 = clean_and_match(pool).await;
    let client = pool.get().await.unwrap();
    {
        let queries = Loader::get_queries_from("sql/aeolus_regex.sql")
            .unwrap()
            .queries;

        for (name, sql) in queries {
            let result = client.execute(sql.as_str(), &[]).await.unwrap();
            println!("Executed {} {} rows affected", name, result);
        }
    }
    let queries = Loader::get_queries_from("sql/initial_mapping.sql")
        .unwrap()
        .queries;
    let result_2 = client
        .execute(
            queries.get("find_match_on_rxconso").unwrap().as_str(),
            &[&"remove nos, hcl etc."],
        )
        .await
        .unwrap();
    println!("Matched {} on RXCONSO", result_2);
    result_1 + result_2
}

async fn drug_name_parenthsis(pool: &Pool) {
    let client = pool.get().await.unwrap();
    let queries = Loader::get_queries_from("sql/aeolus.sql").unwrap().queries;
    execute("parentheses", &client, &queries).await;
}

async fn eu_drug_name(pool: &Pool) {
    let client = pool.get().await.unwrap();
    let queries = Loader::get_queries_from("sql/initial_mapping.sql")
        .unwrap()
        .queries;
    let eu_queries = Loader::get_queries_from("sql/eu_drug_name.sql")
        .unwrap()
        .queries;

    execute("eu", &client, &eu_queries).await;
    execute_param("find_match_on_rxconso", &client, &queries, "eu drug").await;

    rxnormalizer::normalize(&pool, true).await;

    map_rx_to_cdm_concept_id(&pool).await;

    execute("eu_parentheses", &client, &eu_queries).await;
    execute_param(
        "find_match_on_rxconso",
        &client,
        &queries,
        "eu drug parentheses",
    )
        .await;

    map_rx_to_cdm_concept_id(&pool).await;
}

async fn ingredient_matching(pool: &Pool) {
    println!("Starting the original AEOLUS multi and single ingredient mapping");

    let client = pool.get().await.unwrap();
    let queries = Loader::get_queries_from("sql/aeolus.sql").unwrap().queries;

    execute("1_drop_table_words", &client, &queries).await;
    execute("2_create_table_words", &client, &queries).await;
    execute("3_drop_table_multi_ingredient", &client, &queries).await;
    execute("4_create_multi_ingredient_table", &client, &queries).await;
    execute("5_drop_mapping_list", &client, &queries).await;
    execute("6_create_mapping_list", &client, &queries).await;
    execute("7_map_drugs_with_multiple_ingredient", &client, &queries).await;
    execute("8_drop_single_ingredient_table", &client, &queries).await;
    execute("9_create_single_ingredient_vocab_table", &client, &queries).await;
    execute("10_drop_drug_mapping_single_ing", &client, &queries).await;
    execute(
        "11_create_single_ingredient_drug_mapping_table",
        &client,
        &queries,
    )
        .await;
    execute(
        "12_map_drugs_with_single_ingredient_to_ingredient_concepts",
        &client,
        &queries,
    )
        .await;
    execute("13_drop_brands_table", &client, &queries).await;
    execute("14_create_brands_table", &client, &queries).await;
    execute("15_drop_source_brand_mapping_table", &client, &queries).await;
    execute("16_create_source_brand_mapping_table", &client, &queries).await;
    execute("17_map_drugs_containing_brands", &client, &queries).await;
    execute("18_update_original_drug_regex", &client, &queries).await;
}

async fn active_ingredient_mapping(pool: &Pool) {
    println!("Starting active ingredient mapping");

    let client = pool.get().await.unwrap();
    let queries = Loader::get_queries_from("sql/active_ingredient.sql")
        .unwrap()
        .queries;

    execute("drop_ai", &client, &queries).await;
    execute("create_ai", &client, &queries).await;
    execute("drop_index", &client, &queries).await;
    execute("create_index", &client, &queries).await;
    execute("update_ai", &client, &queries).await;
    execute("update_concept_ids", &client, &queries).await;
    execute("update_drug_regex_table", &client, &queries).await;
    execute("append_ambiguous_rxcuis", &client, &queries).await;

    map_rx_to_cdm_concept_id(&pool).await;
}

async fn nda_ingredient_mapping(pool: &Pool) {
    println!("Starting NDA ingredient mapping");

    let client = pool.get().await.unwrap();
    let queries = Loader::get_queries_from("sql/nda_ingredient.sql")
        .unwrap()
        .queries;

    execute("drop_nda_lookup_table", &client, &queries).await;
    execute("create_nda_lookup_table", &client, &queries).await;
    execute("drop_nda_mapping", &client, &queries).await;
    execute("create_nda_mapping", &client, &queries).await;
    execute("drop_nda_index", &client, &queries).await;
    execute("create_nda_index", &client, &queries).await;
    execute("set_nda_ingredient", &client, &queries).await;
    execute("map_nda_ingredient_to_rxnorm", &client, &queries).await;
    execute("update_concept_ids", &client, &queries).await;
    execute("update_drug_regex_table", &client, &queries).await;
    execute("append_ambiguous_rxcuis", &client, &queries).await;

    map_rx_to_cdm_concept_id(&pool).await;
}

async fn manual_mapping(pool: &Pool) {
    println!("Loading previously mapped manual mapping");
    let client = pool.get().await.unwrap();

    let queries = Loader::get_queries_from("sql/aeolus.sql")
        .unwrap()
        .queries;

    execute("drop_manual_table", &client, &queries).await;
    execute("create_manual_table", &client, &queries).await;

    // Populate the table from the csv
    let pwd = env::current_dir().unwrap();
    let pwd = pwd.to_str().unwrap();
    let path = format!("'{}/{}'", pwd, "manual_mappings/manual_map.csv");
    println!("{}", path);
    let query = format!(
        "COPY faers.manual_mappings FROM {} WITH DELIMITER E',' CSV HEADER QUOTE E'\\b'",
        path
    );

    client.execute(query.as_str(), &[]).await.unwrap();
    execute("set_from_manual_table", &client, &queries).await;
}
