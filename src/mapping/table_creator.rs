use crate::db::execute;
use deadpool::managed::Object;
use deadpool_postgres::{ClientWrapper, Pool};
use rawsql::Loader;
use std::error::Error;

pub async fn create_mapping_table(pool: &Pool) -> Result<(), Box<dyn Error>> {
    println!("Creating the drug mapping table... takes a minute ... ");
    let client_1 = pool.get().await.unwrap();
    let client_2 = pool.get().await.unwrap();

    tokio::join!(drug_mapping(&client_1), rxnconso(&client_2));
    Ok(())
}

async fn drug_mapping(client: &Object<ClientWrapper, tokio_postgres::Error>) {
    let queries = Loader::get_queries_from("sql/create_mapping_table.sql")
        .unwrap()
        .queries;
    execute("drop_table", &client, &queries).await;
    execute("create_table", &client, &queries).await;
    execute("drop_index_dn_clean", &client, &queries).await;
    execute("index_dn_clean", &client, &queries).await;
    execute("drop_index_dn_original", &client, &queries).await;
    execute("index_dn_original", &client, &queries).await;
    execute("analyze_drug_mapping", &client, &queries).await;
}

async fn rxnconso(client: &Object<ClientWrapper, tokio_postgres::Error>) {
    let queries = Loader::get_queries_from("sql/create_mapping_table.sql")
        .unwrap()
        .queries;
    execute("drop_index_rxconso", &client, &queries).await;
    execute("index_rxnconso_str", &client, &queries).await;
    execute("analyze_rxnconso", &client, &queries).await;
}
