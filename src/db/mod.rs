use std::collections::HashMap;

use deadpool::managed::Object;
use deadpool_postgres::tokio_postgres::Error;
use deadpool_postgres::{ClientWrapper, Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::NoTls;

pub async fn execute(
    query_name: &str,
    client: &Object<ClientWrapper, Error>,
    queries: &HashMap<String, String>,
) {
    let result = client
        .execute(queries.get(query_name).unwrap().as_str(), &[])
        .await
        .expect(&msg(query_name));
    println!(
        "Executed the {} query, {} rows affected",
        query_name, result
    );
}

pub async fn execute_param(
    query_name: &str,
    client: &Object<ClientWrapper, Error>,
    queries: &HashMap<String, String>,
    param: &str,
) -> u64 {
    let result = client
        .execute(queries.get(query_name).unwrap().as_str(), &[&param])
        .await
        .expect(&msg(query_name));
    println!(
        "Executed the {} query with param {}, {} rows affected",
        query_name, param, result
    );
    result
}

pub fn init_db_pool() -> Pool {
    let mut pg_config = tokio_postgres::Config::new();
    pg_config.port(5432);
    pg_config.host("localhost");
    pg_config.user("postgres");
    pg_config.dbname("cem");
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let mgr: Manager<NoTls> = Manager::from_config(pg_config, NoTls, mgr_config);
    let pool: Pool = Pool::new(mgr, 8);
    pool
}

fn msg(query: &str) -> String {
    format!("Error executing {}", query)
}
