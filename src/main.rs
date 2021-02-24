use crate::mapping::{
    create_final_tables, create_mapping_table, initial_basic_mapping,
    map_rx_to_cdm_concept_id, roll_up, run_original_aeolus, rxnormalize,
};
use std::env;
use std::error::Error;

mod db;
mod mapping;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Starting the Aioli (a greek sauce)");

    let args: Vec<String> = env::args().collect();
    let skip_normalizer = args.contains(&"skip-normalizer".to_string());
    if skip_normalizer {
        println!("Not calling the RxNormalizer")
    }
    let split_multi = !args.contains(&"retain-multi".to_string());
    if !split_multi {
        println!("Retaining multi ingredient drugs")
    }

    println!("Initializing DB pool...");
    let pool = db::init_db_pool();

    mapping::do_exact_mapping(&pool).await?;

    // println!("Starting Drug Mapping...");
    // create_mapping_table(&pool).await?;
    // initial_basic_mapping(&pool).await?;
    //
    // if !skip_normalizer {
    //     rxnormalize(&pool).await?;
    // }
    // map_rx_to_cdm_concept_id(&pool).await;
    // run_original_aeolus(&pool).await;
    // roll_up(&pool, split_multi).await;
    // create_final_tables(&pool).await;
    Ok(())
}
