use std::{thread, time};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::collections::hash_map::RandomState;
use std::error::Error;
use std::hash::Hash;
use std::time::Instant;

use csv::Reader;
use deadpool::managed::Object;
use deadpool_postgres::{ClientWrapper, Pool};
use deadpool_postgres::tokio_postgres::Row;
use rand::Rng;
use rawsql::Loader;

use crate::db::{execute, execute_param};
use crate::mapping::rxnormalizer;

pub async fn map(pool: &Pool) -> Result<(), Box<dyn Error>> {
    create_table(&pool).await?;

    clean_dose_amt(&pool).await?;
    clean_dose_form(&pool).await?;

    populate_brand_and_ingredient(&pool).await?;

    clean_drug_and_ai(&pool).await?;

    populate_brand_and_ingredient(&pool).await?;

    check_parentesiss(&pool).await?;

    clean_drug_and_ai(&pool).await?;

    populate_brand_and_ingredient(&pool).await?;

    populate_from_eu_art_57(&pool).await?;

    rxnormalizer::normalize_exact(&pool).await?;

    let queries =
        Loader::get_queries_from("sql/exact_mapping/populate_brand_and_ingredient.sql")?.queries;
    let q = queries.get("populate_brand_from_any_word_in_drug").unwrap();
    big_query_split_execution(q, pool).await;

    do_mapping(&pool).await?;

    let queries = Loader::get_queries_from("sql/exact_mapping/reinterpret.sql")?.queries;
    let client = pool.get().await.unwrap();
    execute("pin_to_in", &client, &queries).await;

    remap_rx_dose_form(&pool).await?;
    do_mapping(&pool).await?;

    Ok(())
}

async fn create_table(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let queries = Loader::get_queries_from("sql/exact_mapping/create_table.sql")?.queries;
    let client = pool.get().await?;

    execute("drop_table", &client, &queries).await;
    execute("create_table", &client, &queries).await;

    Ok(())
}

async fn clean_dose_amt(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let queries = Loader::get_queries_from("sql/exact_mapping/dose_amt.sql")?.queries;
    let client = pool.get().await?;

    execute("remove_amount_when_is_dose_form_count", &client, &queries).await;
    execute("trim_trailing_dot", &client, &queries).await;
    execute("prepend_zero", &client, &queries).await;
    execute("remove_junk", &client, &queries).await;
    execute("exchange_o_for_0", &client, &queries).await;
    execute("convert_comma_to_dot", &client, &queries).await;
    execute("trim_trailing_dot", &client, &queries).await;
    execute("remove_non_numeric_chars", &client, &queries).await;
    execute("remove_junk", &client, &queries).await;
    execute("convert_ug_to_mg", &client, &queries).await;
    execute("trim_trailing_dot", &client, &queries).await;

    Ok(())
}

// TODO: This step is both inefficient and messy, clean it up!
pub async fn clean_dose_form(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let client_1 = pool.get().await?;
    let dose_form_clean = "dose_form_clean";
    let route_clean = "route_clean";
    let columns = vec![dose_form_clean, route_clean];

    // Populate the 'clean' dose form and columns removing parenthesis dots and commas
    for target in columns.iter() {
        let query = format!("update faers.drug_mapping_exact set {tc} = concat(' ', regexp_replace({t}, '[\\(\\)\\,\\.]', '', 'gi'), ' ')", tc = target, t = target.replace("_clean", ""));
        update(query, &client_1).await;
    }

    // Get manual mapping to convert routes and dose forms unknown to rxnorm
    let mut csv = Reader::from_path("manual_mappings/route_and_form_map.csv")?;
    for record in csv.deserialize() {
        let result: (String, String) = record?;
        println!("Replacing {} with {}", result.0, result.1);
        for target in columns.iter() {
            let query = format!("update faers.drug_mapping_exact set {t} = replace({t}, ' {from} ', ' {to} ') where {t} like '% {from} %'", t = target, from = result.0, to = result.1);
            update(query, &client_1).await;
        }
    }

    // Find all distinct known words available as dose form in rxnorm
    let result = client_1.query("select distinct lower(str) as str from faers.rxnconso where tty = 'DF' and sab = 'RXNORM'", &[]).await.unwrap();
    let mut rx_dose_forms: HashSet<String> = HashSet::new();
    distinct_words_from_result_list(result, &mut rx_dose_forms);

    tokio::join!(
        remove_unknown_dose_form_terms(pool, &rx_dose_forms, dose_form_clean),
        remove_unknown_dose_form_terms(pool, &rx_dose_forms, route_clean)
    );

    println!("Doing some cleaning of blank spaces in dose_form_clean and route_clean");
    for target in columns.iter() {
        let q1 = format!(
            "update faers.drug_mapping_exact set {t} = null where {t} ~ '^[[:space:]]*$'",
            t = target
        );
        let q2 = format!("update faers.drug_mapping_exact set {t} = regexp_replace({t}, ' +$', '', 'gi') where {t} like '% '", t = target);
        let q3 = format!("update faers.drug_mapping_exact set {t} = regexp_replace({t}, '^ +', '', 'gi') where {t} like ' %'", t = target);
        let q4 = format!("update faers.drug_mapping_exact set {t} = regexp_replace({t}, '(\\S) +', '\\1 ', 'gi') where {t} ' %'", t = target);
        let q5 = format!(
            "update faers.drug_mapping_exact set {t} = null where {t} = ''",
            t = target
        );

        for q in vec![q1, q2, q3, q4, q5] {
            update(q, &client_1).await;
        }
    }

    rx_dose_form(&pool).await?;

    // find dose form in drug field of source data
    let queries = Loader::get_queries_from("sql/exact_mapping/dose_form.sql")?.queries;
    let stmt = client_1
        .prepare(queries.get("df_in_drug").unwrap().as_str())
        .await?;
    for words in (1..6).rev() {
        let altered = client_1.execute(&stmt, &[&words]).await.unwrap();
        println!("Rows alterd {}", altered)
    }

    Ok(())
}

async fn do_mapping(pool: &Pool) -> Result<(), Box<dyn Error>> {
    map_to_sbd(&pool).await?;
    map_to_scd(&pool).await?;
    map_to_sbdf(&pool).await?;
    map_to_scdf(&pool).await?;
    map_to_scdc(&pool).await?;
    Ok(())
}

async fn populate_from_eu_art_57(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let client = pool.get().await.unwrap();
    let queries = Loader::get_queries_from("sql/exact_mapping/article_57.sql")
        .unwrap()
        .queries;

    execute("single_ingredients_from_article_57", &client, &queries).await;
    execute("pin_from_article_57", &client, &queries).await;
    execute("article_57_alternative_spellings", &client, &queries).await;
    execute(
        "article_57_alternative_spellings_incl_pin",
        &client,
        &queries,
    )
        .await;
    execute("article_57_multi_ingr", &client, &queries).await;
    execute("set_prod_ai", &client, &queries).await;

    Ok(())
}

async fn check_parentesiss(pool: &&Pool) -> Result<(), Box<dyn Error>> {
    let client = pool.get().await.unwrap();
    let queries = Loader::get_queries_from("sql/exact_mapping/parenthesis.sql")
        .unwrap()
        .queries;

    for _ in 0..2 {
        for (name, _sql) in &queries {
            execute(name, &client, &queries).await;
        }
    }
    Ok(())
}

async fn clean_drug_and_ai(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let client = pool.get().await.unwrap();
    let queries = Loader::get_queries_from("sql/cleaning.sql")
        .unwrap()
        .queries;

    for _ in 0..2 {
        for (name, sql) in &queries {
            let sql = sql
                .replace("drug_mapping", "drug_mapping_exact")
                .replace("drug_name_clean", "drugname_clean");
            let result = client.execute(sql.as_str(), &[]).await.unwrap();
            println!("Executed {} on drug name {} rows affected", name, result);
            let sql = sql.replace("drugname_clean", "prod_ai_clean");
            let result = client.execute(sql.as_str(), &[]).await.unwrap();
            println!("Executed {} on prod_ai {} rows affected", name, result);
        }
    }

    let sql = queries
        .get("remove_white")
        .unwrap()
        .replace("drug_mapping", "drug_mapping_exact")
        .replace("drug_name_clean", "drugname_clean");
    let result = client.execute(sql.as_str(), &[]).await.unwrap();
    println!(
        "Executed {} on drug name {} rows affected",
        "remove_white", result
    );
    let sql = sql.replace("drugname_clean", "prod_ai_clean");
    let result = client.execute(sql.as_str(), &[]).await.unwrap();
    println!(
        "Executed {} on prod_ai {} rows affected",
        "remove_white", result
    );

    let sql = queries
        .get("trim_spaces")
        .unwrap()
        .replace("drug_mapping", "drug_mapping_exact")
        .replace("drug_name_clean", "drugname_clean");
    let result = client.execute(sql.as_str(), &[]).await.unwrap();
    println!(
        "Executed {} on drug name {} rows affected",
        "remove_white", result
    );
    let sql = sql.replace("drugname_clean", "prod_ai_clean");
    let result = client.execute(sql.as_str(), &[]).await.unwrap();
    println!(
        "Executed {} on prod_ai {} rows affected",
        "remove_white", result
    );

    Ok(())
}

async fn remove_unknown_dose_form_terms(
    pool: &Pool,
    rx_dose_forms: &HashSet<String>,
    target_column: &str,
) {
    let client = pool.get().await.unwrap();
    let q = format!(
        "select distinct lower({target}) from faers.drug_mapping_exact where {target} is not null",
        target = target_column
    );
    let result = client.query(q.as_str(), &[]).await.unwrap();
    let mut set: HashSet<String> = HashSet::new();

    distinct_words_from_result_list(result, &mut set);

    // Delete everything that rxnorm does not know (some stuff was already manually mapped earlier)
    for faers_df in set {
        if !rx_dose_forms.contains(&faers_df) {
            let query = format!("update faers.drug_mapping_exact set {target} = replace({target}, ' {df} ', ' ') where {target} like '% {df} %'", df = faers_df, target = target_column);
            let result = client.execute(query.as_str(), &[]).await;
            let count = match result {
                Ok(rows) => rows,
                Err(e) => {
                    println!(
                        "Caught an error of kind {}, going to wait 3 seconds and try again",
                        e.to_string()
                    );
                    let time = time::Duration::from_secs(3);
                    thread::sleep(time);
                    client.execute(query.as_str(), &[]).await.unwrap()
                }
            };
            println!("Removed the word {} from {} rows", faers_df, count);
        }
    }
}

fn distinct_words_from_result_list(result: Vec<Row>, set: &mut HashSet<String>) {
    for r in result {
        let str: String = r.get(0);
        let list = str.split_whitespace();
        for s in list {
            let faers_df = s.to_string().to_lowercase();
            set.insert(faers_df);
        }
    }
}

async fn populate_brand_and_ingredient(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let client = pool.get().await?;
    let queries =
        Loader::get_queries_from("sql/exact_mapping/populate_brand_and_ingredient.sql")?.queries;

    execute("populate_brand_from_drug", &client, &queries).await;
    execute("populate_ingredient_from_prod_ai", &client, &queries).await;
    execute("populate_ingredient_from_drug", &client, &queries).await;
    execute("sbd", &client, &queries).await;
    execute("scd", &client, &queries).await;
    execute("from_nda", &client, &queries).await;
    execute("from_nda_prepend_zero", &client, &queries).await;

    Ok(())
}

async fn map_to_sbd(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    let client = pool.get().await?;
    let queries = Loader::get_queries_from("sql/exact_mapping/map_to_sbd.sql")
        .unwrap()
        .queries;

    println!("Mapping to SBD (Ingredient + Strength + Dose Form + Brand Name), queries become progressively slower...");
    execute("exact", &client, &queries).await;
    execute("drug_clean", &client, &queries).await;
    execute("multi_ing", &client, &queries).await;
    execute("multi_ing_df", &client, &queries).await;
    execute("brand", &client, &queries).await;
    execute("ing_and_brand", &client, &queries).await;
    execute("brand_and_dose_form_strict", &client, &queries).await;
    execute("brand_and_dose_form_loose", &client, &queries).await;

    let q = queries.get("ing_df_brand").unwrap();
    big_query_split_execution(q, pool).await;
    let q = queries.get("ing_brand_dose_form_amount").unwrap();
    big_query_split_execution(q, pool).await;

    let duration = start.elapsed().as_secs_f32() / 60f32;
    println!("Finished mapping to SBD that took {:.2} minutes", duration);

    Ok(())
}

async fn map_to_scd(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    let client = pool.get().await?;
    let queries = Loader::get_queries_from("sql/exact_mapping/map_to_scd.sql")
        .unwrap()
        .queries;

    println!("Mapping to SCD (Ingredient + Strength + Dose Form), queries become progressively slower...");
    execute("drug_clean", &client, &queries).await;
    execute("drug_clean_extra", &client, &queries).await;
    execute("exact", &client, &queries).await;
    execute("multi_ing", &client, &queries).await;
    execute("multi_ing_df", &client, &queries).await;
    execute("ingredient", &client, &queries).await;
    execute("ing_and_dose_form", &client, &queries).await;
    execute("ing_dose_form_amount", &client, &queries).await;

    let duration = start.elapsed().as_secs_f32() / 60f32;
    println!("Finished mapping to SCD that took {:.2} minutes", duration);

    Ok(())
}

async fn map_to_sbdf(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    let client = pool.get().await?;
    let queries = Loader::get_queries_from("sql/exact_mapping/map_to_sbdf.sql")
        .unwrap()
        .queries;

    println!(
        "Mapping to SBDF (Ingredient + Dose Form + Brand), queries become progressively slower..."
    );
    execute("drug_clean", &client, &queries).await;
    execute("exact", &client, &queries).await;
    execute("loose", &client, &queries).await;
    execute("ing_brand", &client, &queries).await;

    let duration = start.elapsed().as_secs_f32() / 60f32;
    println!("Finished mapping to SBDF that took {:.2} minutes", duration);

    Ok(())
}

async fn map_to_scdf(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    let client = pool.get().await?;
    let queries = Loader::get_queries_from("sql/exact_mapping/map_to_scdf.sql")
        .unwrap()
        .queries;

    println!("Mapping to SCDF (Ingredient + Dose Form), queries become progressively slower...");
    execute("drug_clean", &client, &queries).await;
    execute("exact", &client, &queries).await;
    execute("loose", &client, &queries).await;

    let duration = start.elapsed().as_secs_f32() / 60f32;
    println!("Finished mapping to SCDF that took {:.2} minutes", duration);

    Ok(())
}

async fn map_to_scdc(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    let client = pool.get().await?;
    let queries = Loader::get_queries_from("sql/exact_mapping/map_to_scdc.sql")
        .unwrap()
        .queries;

    println!("Mapping to SCDC (Ingredient + Amount), queries become progressively slower...");
    execute("drug_clean", &client, &queries).await;
    execute("exact", &client, &queries).await;

    let duration = start.elapsed().as_secs_f32() / 60f32;
    println!("Finished mapping to SCDC in {:.2} minutes", duration);

    Ok(())
}

async fn remap_rx_dose_form(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let queries = Loader::get_queries_from("sql/exact_mapping/reinterpret.sql")?.queries;
    let client = pool.get().await?;
    let q = queries.get("remap_rx_df").unwrap();

    let stmt = client.prepare(q).await?;
    let mut csv = Reader::from_path("manual_mappings/remap_df.csv")?;
    for record in csv.deserialize() {
        let result: (String, String) = record?;
        println!(
            "Changing rx_norm_dose_form from {} to {}",
            result.0, result.1
        );
        client.execute(&stmt, &[&result.0, &result.1]).await?;
    }
    Ok(())
}

async fn rx_dose_form(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let queries = Loader::get_queries_from("sql/exact_mapping/dose_form.sql")?.queries;
    let client = pool.get().await?;

    execute("remove_df", &client, &queries).await;
    execute("set_temp_dose_form", &client, &queries).await;
    execute("deduplicate_and_order_temp_dose_form", &client, &queries).await;
    execute("set_known_rx_dose_forms", &client, &queries).await;
    execute("set_single_word_rx_dose_form", &client, &queries).await;
    execute("delete_the_word_for", &client, &queries).await;

    // Manually mapped to rxnorm dose forms
    let mut csv = Reader::from_path("manual_mappings/rx_dose_form.csv")?;
    for record in csv.deserialize() {
        let result: (String, String) = record?;
        println!(
            "Setting rx_norm_dose_form as {} from {}",
            result.1, result.0
        );
        let query_1 = format!(
            "update faers.drug_mapping_exact set rx_dose_form = '{}' where temp_dose_form = '{}'",
            result.1, result.0
        );
        update(query_1, &client).await;
    }

    // Whatever is left we will just pick the first word, and hope for the best
    execute("set_rx_dose_from_first_term", &client, &queries).await;
    // do extended release stuff
    execute("extended_release", &client, &queries).await;
    execute("extended_release_tab", &client, &queries).await;
    execute("extended_release_cap", &client, &queries).await;
    execute("extended_release_oral", &client, &queries).await;

    Ok(())
}

pub async fn update(query: String, client: &Object<ClientWrapper, tokio_postgres::Error>) {
    let count = client.execute(query.as_str(), &[]).await.unwrap();
    println!("Altered {} rows", count);
}

// Postgres can perform parallel queries on its own, but I've found this workaround to be significantly faster
// Suggestions for a 'cleaner' means of optimization, are very welcome as this whole process takes a fair amount of time.
async fn big_query_split_execution(original_query: &String, pool: &Pool) {
    let client_1 = pool.get().await.unwrap();
    let client_2 = pool.get().await.unwrap();
    let alt_query = original_query.replace(">", "=");

    println!("long lasting query coming up ... ... ");

    let start = Instant::now();

    let results = tokio::join!(
        client_1.execute(original_query.as_str(), &[]),
        client_2.execute(alt_query.as_str(), &[])
    );

    let duration = start.elapsed().as_secs_f32() / 60f32;

    println!(
        "Updated {} in the multi entry group and {} of the trailing singles it only took {:.2}",
        results.0.unwrap(),
        results.1.unwrap(),
        duration
    )
}
