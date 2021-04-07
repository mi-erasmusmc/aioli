use std::error::Error;
use std::time::Instant;

use deadpool_postgres::Pool;
use futures::future::join;

use crate::mapping::{
    create_final_tables, create_mapping_table, initial_basic_mapping, map_rx_to_cdm_concept_id,
    roll_up, run_original_aeolus, rxnormalize,
};

mod db;
mod mapping;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Starting the Aioli (a mediterranean sauce)");
    let start = Instant::now();

    let mut settings = config::Config::default();
    settings.merge(config::File::with_name("Settings")).unwrap();

    let skip_normalizer = settings.get_bool("skip_normalizer").unwrap();
    if skip_normalizer {
        println!("Not calling the RxNormalizer")
    }
    let split_multi = !settings.get_bool("retain_multi").unwrap();
    if !split_multi {
        println!("Retaining multi ingredient drugs")
    }

    println!("Initializing DB pool...");
    let pool_1 = db::init_db_pool(&settings);

    println!("Starting Drug Mapping...");

    let map_atc = settings.get_bool("map_atc").unwrap();
    if map_atc {
        let pool_2 = db::init_db_pool(&settings);
        let result = join(
            mapping::map_atc(&pool_1),
            map_rxnorm(skip_normalizer, split_multi, &pool_2),
        )
        .await;
        // To be sure that no errors were propagated and never unwrapped
        result.0.unwrap();
        result.1.unwrap();
    } else {
        map_rxnorm(skip_normalizer, split_multi, &pool_1).await?;
    }

    create_final_tables(&pool_1, map_atc).await;

    let elapsed_secs = start.elapsed().as_secs();
    let elapsed_mins = elapsed_secs / 60;
    let hours = elapsed_secs / 3600;
    let minutes = elapsed_mins % 60;
    let seconds = elapsed_secs % 60;

    println!(
        "Your FEARS drug mapping has been Aioli-ed in {} hours {} minutes {} seconds",
        hours, minutes, seconds
    );

    Ok(())
}

async fn map_rxnorm(
    skip_normalizer: bool,
    split_multi: bool,
    pool: &Pool,
) -> Result<(), Box<dyn Error>> {
    create_mapping_table(&pool).await?;
    initial_basic_mapping(&pool).await?;

    if !skip_normalizer {
        rxnormalize(&pool).await?;
    }
    map_rx_to_cdm_concept_id(&pool).await;
    run_original_aeolus(&pool).await;
    roll_up(&pool, split_multi).await;
    Ok(())
}
