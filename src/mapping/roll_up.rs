use deadpool_postgres::Pool;
use rawsql::Loader;

use crate::db::execute;

pub async fn do_roll_up(pool: &Pool, split_multi: bool) {
    println!("Converting concept ids to OMOP standard Ingredient or standard Clinical Drug Form");
    let queries = Loader::get_queries_from("sql/roll_up.sql").unwrap().queries;
    let client = pool.get().await.unwrap();

    execute("populate_standard_concept_id", &client, &queries).await;
    execute("to_standard_ingredient", &client, &queries).await;
    execute("brands_to_branded_dose_group", &client, &queries).await;
    execute("to_clinical_dose_group", &client, &queries).await;
    execute("to_standard_ingredient", &client, &queries).await;
    execute("to_clinical_drug_comp", &client, &queries).await;
    execute("to_standard_ingredient", &client, &queries).await;

    if split_multi {
        execute(
            "split_multi_ingredients_to_separate_entries",
            &client,
            &queries,
        )
        .await;
        execute("delete_multi", &client, &queries).await;
    } else {
        execute(
            "multiple_ingredients_to_clinical_drug_form",
            &client,
            &queries,
        )
        .await;
    }

    execute(
        "single_ingredient_clinical_drug_form_to_ingredient",
        &client,
        &queries,
    )
    .await;
    execute("clean_unmapped_multi", &client, &queries).await;
    execute("standardize_residue", &client, &queries).await;
    execute(
        "single_ingredient_clinical_drug_form_to_ingredient",
        &client,
        &queries,
    )
    .await;
    execute("manual_mappings_allegra", &client, &queries).await;
    execute("manual_mappings_inderal", &client, &queries).await;
    execute("manual_mappings_tylenol", &client, &queries).await;
    execute("manual_mappings_kenalog", &client, &queries).await;
    execute("manual_mappings_robitussin", &client, &queries).await;
    execute("manual_mapping_reve_vita", &client, &queries).await;
    execute("manual_mapping_optiray", &client, &queries).await;

    if split_multi {
        execute("to_standard_ingredient_incl_multi", &client, &queries).await;
        execute(
            "split_multi_ingredients_to_separate_entries",
            &client,
            &queries,
        )
        .await;
        execute("delete_multi", &client, &queries).await;
    }
}
