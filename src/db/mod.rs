use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::time::Instant;

use config::Config;
use deadpool::managed::Object;
use deadpool_postgres::tokio_postgres::Error;
use deadpool_postgres::{ClientWrapper, Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::NoTls;

pub async fn execute(
    query_name: &str,
    client: &Object<ClientWrapper, Error>,
    queries: &HashMap<String, String>,
) {
    let start = Instant::now();

    print!("Executing the {} query... ", query_name);
    io::stdout().flush().unwrap();

    let result = client
        .execute(queries.get(query_name).unwrap().as_str(), &[])
        .await
        .expect(&msg(query_name));

    let seconds = start.elapsed().as_secs_f32();

    println!("{} rows affected in {:.2}s", result, seconds);
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

pub fn init_db_pool(config: &Config) -> Pool {
    let mut pg_config = tokio_postgres::Config::new();
    pg_config.port(config.get_int("pg_port").unwrap() as u16);
    pg_config.host(&*config.get_str("pg_host").unwrap());
    pg_config.user(&*config.get_str("pg_user").unwrap());
    pg_config.dbname(&*config.get_str("pg_dbname").unwrap());
    pg_config.password(&*config.get_str("pg_password").unwrap());
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
