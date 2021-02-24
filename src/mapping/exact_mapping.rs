use crate::db::execute;
use csv::Reader;
use deadpool::managed::Object;
use deadpool_postgres::{ClientWrapper, Pool};
use rawsql::Loader;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::error::Error;

pub async fn map(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let client = pool.get().await?;
    let queries = Loader::get_queries_from("sql/exact_mapping.sql")?.queries;

    execute("drop_table", &client, &queries).await;
    execute("create_table", &client, &queries).await;
    execute("populate_brand_from_drug", &client, &queries).await;
    execute("populate_ingredient_from_prod_ai", &client, &queries).await;
    execute("populate_ingredient_from_drug", &client, &queries).await;
    execute("set_dose_amt_clean", &client, &queries).await;
    execute("prepend_zero_dose_amt", &client, &queries).await;


    //dose_form(&pool).await?;

    //
    // for route in routes {
    //     if !dose_forms.contains(&route) {
    //         println!("{}", route);
    //
    //         // let result = client
    //         //     .execute("update faers.drug_mapping_exact set route_clean = replace(route_clean, $1, '') where route_clean like '%$1%';", &[&route])
    //         //     .await.unwrap();
    //         // println!("deleted {} rows containing {}", result, route);
    //
    //         unknown_routes.insert(route);
    //     }
    // }

    //println!("{:?}", unknown_routes.len());

    //execute("create_table", &client, &queries).await;

    // {
    //     let queries = Loader::get_queries_from("sql/cleaning.sql")
    //         .unwrap()
    //         .queries;
    //
    //     for (name, sql) in queries {
    //         print!("this is the sql {}", sql);
    //         let result = client.execute(sql.as_str(), &[]).await.unwrap();
    //         println!("Executed {} {} rows affected", name, result);
    //     }

    // let queries = Loader::get_queries_from("sql/cleaning.sql")
    //     .unwrap()
    //     .queries;
    //
    // for (name, sql) in queries {
    //     let result = client.execute(sql.as_str(), &[&"prod_ai_clean"]).await.unwrap();
    //     println!("Executed {} {} rows affected", name, result);
    // }
    //  }

    Ok(())
}

pub async fn dose_form(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let client = pool.get().await?;
    let client_2 = pool.get().await?;

    // let result = client.execute("update faers.drug_mapping_exact set dose_form_clean = concat(' ', regexp_replace(dose_form, '[\\(\\)\\,\\.]', '', 'gi'), ' ')", &[]).await.unwrap();
    // println!("Rows {} alterd", result);
    //
    // let result = client.execute("update faers.drug_mapping_exact set route_clean = concat(' ', regexp_replace(route, '[\\(\\)\\,\\.]', '', 'gi'), ' ')", &[]).await.unwrap();
    // println!("Rows {} alterd", result);

    let mut csv = Reader::from_path("route_and_form_map.csv")?;

    for record in csv.deserialize() {
        let result: (String, String) = record?;
        println!("Replacing {} with {}", result.0, result.1);
        let query_1 = format!("update faers.drug_mapping_exact set dose_form_clean = replace(dose_form_clean, '{}', '{}') where dose_form_clean like '% {} %'", result.0, result.1, result.0);
        let query_2 = format!("update faers.drug_mapping_exact set route_clean = replace(route_form_clean, '{}', '{}') where route_clean like '% {} %'", result.0, result.1, result.0);
        update(query_1, &client).await;
        update(query_2, &client_2).await;
    }

    let result = client.query("select distinct str from faers.\"RxNorm_RXNCONSO\" where tty = 'DF' and sab = 'RXNORM'", &[]).await.unwrap();
    let mut rx_dose_forms: HashSet<String> = HashSet::new();

    for r in result {
        let str: String = r.get("str");
        let list = str.split_whitespace();
        for s in list {
            let string = s.to_string().to_lowercase();
            rx_dose_forms.insert(string);
        }
    }

    let result = client.query("select distinct dose_form_clean from faers.drug_mapping_exact where dose_form_clean is not null", &[]).await.unwrap();
    let mut faers_dose_forms: BTreeSet<String> = BTreeSet::new();

    for r in result {
        let str: String = r.get("dose_form_clean");
        let list = str.split_whitespace();
        for s in list {
            let string = s.to_string().to_lowercase();
            faers_dose_forms.insert(string);
        }
    }

    let mut to_delete: Vec<String> = Vec::new();

    for d in faers_dose_forms {
        if !rx_dose_forms.contains(&d) {
            let with_spaces = format!(" {} ", d);
            to_delete.push(with_spaces);

            //   update(query_2, &client_2).await;
        }
    }

    //   let query_1 = format!("update faers.drug_mapping_exact set dose_form_clean = replace(dose_form_clean, '{}', '') where dose_form_clean like '% {} %'", d, d);
    //  let query_2 = format!("update faers.drug_mapping_exact set route_clean = replace(route_clean, '{}', '') where dose_form_clean like '% {} %'", d, d);
    //   update(query_1, &client).await;

    client.execute("update faers.drug_mapping_exact set dose_form_clean = null where dose_form_clean ~ '^[[:space:]]*$'", &[]).await?;
    client.execute("update faers.drug_mapping_exact set route_clean = null where route_clean ~ '^[[:space:]]*$'", &[]).await?;

    Ok(())
}

pub async fn update(query: String, client: &Object<ClientWrapper, tokio_postgres::Error>) {
    let count = client.execute(query.as_str(), &[]).await.unwrap();
    println!("Altered {} rows", count);
    if count == 0 {
        println!(" --- ---");
    }
}
