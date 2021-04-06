-- name: exact
with cte1 as (select string_agg(distinct cast(r.rxcui as text), ',') as rxcui,
                     m.id
              from faers.rxnconso r
                       join
                   faers.drug_mapping_exact m
                   on lower(r.str) =
                      lower(concat(m.rx_ingredient, ' ', m.dose_amt_clean, ' ', m.dose_unit_clean, ' ',
                                   m.rx_dose_form, ' [',
                                   m.rx_brand_name, ']'))
              where m.rx_ingredient is not null
                and m.rx_brand_name is not null
                and m.dose_amt_clean is not null
                and m.dose_unit_clean is not null
                and m.rx_dose_form is not null
                and r.tty = 'SBD'
                and r.sab = 'RXNORM'
                and r.suppress != 'O'
              group by m.id
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte1.rxcui,
    tty   = 'SBD'
from cte1
where cte1.id = m.id;

-- name: brand
WITH cte1 AS (select distinct rx_brand_name as rxbn
              from faers.drug_mapping_exact
              where rx_brand_name is not null
                and rxcui is null),
     cte2 as (select string_agg(distinct cast(r.rxcui as text), ',') as rxcui,
                     cte1.rxbn
              from faers.rxnconso r
                       join
                   cte1
                   on lower(r.str) like
                      concat('%[', cte1.rxbn, ']')
                       and r.tty = 'SBD'
                       and r.sab = 'RXNORM'
                       and r.suppress != 'O'
              group by cte1.rxbn
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte2.rxcui,
    tty   = 'SBD'
from cte2
where cte2.rxbn = m.rx_brand_name
  and m.rxcui is null;


-- name: brand_and_dose_form_strict
WITH cte1 AS (select distinct rx_brand_name as rxbn, rx_dose_form as rdf
              from faers.drug_mapping_exact
              where rx_brand_name is not null
                and rx_dose_form is not null
                and rxcui is null),
     cte2 as (select string_agg(distinct cast(r.rxcui as text), ',') as rxcui,
                     cte1.rxbn,
                     cte1.rdf
              from faers.rxnconso r
                       join
                   cte1
                   on lower(r.str) like
                      concat('%', cte1.rdf, ' [', cte1.rxbn, ']')
                       and r.tty = 'SBD'
                       and r.sab = 'RXNORM'
                       and r.suppress != 'O'
              group by cte1.rxbn, cte1.rdf
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte2.rxcui,
    tty   = 'SBD'
from cte2
where cte2.rxbn = m.rx_brand_name
  and cte2.rdf = m.rx_dose_form
  and m.rxcui is null;

-- name: brand_and_dose_form_loose
WITH cte1 AS (select distinct rx_brand_name as rxbn, rx_dose_form as rdf
              from faers.drug_mapping_exact
              where rx_brand_name is not null
                and rx_dose_form is not null
                and rxcui is null),
     cte2 as (select string_agg(distinct cast(r.rxcui as text), ',') as rxcui,
                     cte1.rxbn,
                     cte1.rdf
              from faers.rxnconso r
                       join
                   cte1
                   on lower(r.str) like
                      concat('%', cte1.rdf, '%[', cte1.rxbn, ']')
                       and r.tty = 'SBD'
                       and r.sab = 'RXNORM'
                       and r.suppress != 'O'
              group by cte1.rxbn, cte1.rdf
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte2.rxcui,
    tty   = 'SBD'
from cte2
where cte2.rxbn = m.rx_brand_name
  and cte2.rdf = m.rx_dose_form
  and m.rxcui is null;

-- name: ing_brand_dose_form_amount
WITH cte1 AS (select rx_ingredient    as ing,
                     rx_brand_name    as rxbn,
                     rx_dose_form     as rdf,
                     dose_amt_clean   as dac,
                     sum(occurrences) as oc
              from faers.drug_mapping_exact
              where rx_brand_name is not null
                and (rx_ingredient is not null and rx_ingredient not like '%/%')
                and rx_dose_form is not null
                and dose_amt_clean is not null
                and rxcui is null
              group by rx_ingredient, rx_brand_name, rx_dose_form, dose_amt_clean
              having sum(occurrences) > 1),
     cte2 as (select string_agg(distinct cast(r.rxcui as text), ',') as rxcui,
                     cte1.rxbn,
                     cte1.rdf,
                     cte1.ing,
                     cte1.dac
              from faers.rxnconso r
                       join
                   cte1
                   on lower(r.str) like
                      concat(cte1.ing, '% ', cte1.dac, ' %', cte1.rdf, '%[', cte1.rxbn, ']')
                       and r.tty = 'SBD'
                       and r.sab = 'RXNORM'
                       and r.suppress != 'O'
              group by cte1.rxbn, cte1.rdf, cte1.ing, cte1.dac
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte2.rxcui,
    tty   = 'SBD'
from cte2
where cte2.rxbn = m.rx_brand_name
  and cte2.rdf = m.rx_dose_form
  and cte2.ing = m.rx_ingredient
  and cte2.dac = m.dose_amt_clean
  and m.rxcui is null;

-- name: ing_and_brand
WITH cte1 AS (select distinct rx_brand_name as rxbn, rx_ingredient as ing
              from faers.drug_mapping_exact
              where rx_brand_name is not null
                and (rx_ingredient is not null and rx_ingredient not like '%/%')
                and rxcui is null),
     cte2 as (select string_agg(distinct cast(r.rxcui as text), ',') as rxcui,
                     cte1.ing,
                     cte1.rxbn
              from faers.rxnconso r
                       join
                   cte1
                   on lower(r.str) like lower(concat(cte1.ing, ' %[', cte1.rxbn, ']'))
                       and r.tty = 'SBD'
                       and r.sab = 'RXNORM'
                       and r.suppress != 'O'
              group by cte1.ing, cte1.rxbn
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte2.rxcui,
    tty   = 'SBD'
from cte2
where cte2.ing = m.rx_ingredient
  and cte2.rxbn = m.rx_brand_name
  and m.rxcui is null;

-- name: ing_df_brand
WITH cte1 AS (select rx_ingredient    as ing,
                     rx_brand_name    as rxbn,
                     rx_dose_form     as rdf,
                     sum(occurrences) as oc
              from faers.drug_mapping_exact
              where rx_brand_name is not null
                and (rx_ingredient is not null and rx_ingredient not like '%/%')
                and rx_dose_form is not null
                and dose_amt_clean is not null
                and rxcui is null
              group by rx_ingredient, rx_brand_name, rx_dose_form, dose_amt_clean
              having sum(occurrences) > 1),
     cte2 as (select string_agg(distinct cast(r.rxcui as text), ',') as rxcui,
                     cte1.ing,
                     cte1.rxbn,
                     cte1.rdf,
                     string_agg(distinct r.str, ',')
              from faers.rxnconso r
                       join
                   cte1
                   on lower(r.str) like
                      lower(concat(cte1.ing, ' %', cte1.rdf, '%[', cte1.rxbn, ']'))
                       and r.tty = 'SBD'
                       and r.sab = 'RXNORM'
                       and r.suppress != 'O'
              group by cte1.ing, cte1.rxbn, cte1.rdf
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte2.rxcui,
    tty   = 'SBD'
from cte2
where cte2.ing = m.rx_ingredient
  and cte2.rxbn = m.rx_brand_name
  and cte2.rdf = m.rx_dose_form
  and m.rxcui is null;

-- name: multi_ing
WITH cte1 AS (select distinct rx_brand_name as rxbn, rx_ingredient as ing
              from faers.drug_mapping_exact
              where rx_brand_name is not null
                and (rx_ingredient is not null and rx_ingredient like '%/%')
                and rxcui is null),
     cte2 as (select distinct string_agg(DISTINCT CAST(r.rxcui AS varchar), ',') as rxcui,
                              cte1.rxbn,
                              cte1.ing
              from faers.rxnconso r
                       join cte1 on lower(r.str) like concat(replace(cte1.ing, ' ', '%'), '%', '[', cte1.rxbn, ']')
              where r.sab = 'RXNORM'
                and r.tty = 'SBD'
                and r.suppress != 'O'
              group by cte1.rxbn, cte1.ing
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte2.rxcui,
    tty   = 'SBD'
from cte2
where cte2.ing = m.rx_ingredient
  and cte2.rxbn = m.rx_brand_name
  and m.rxcui is null;

-- name: multi_ing_df
WITH cte1 AS (select distinct rx_brand_name as rxbn, rx_ingredient as ing, rx_dose_form as rdf
              from faers.drug_mapping_exact
              where rx_brand_name is not null
                and rx_dose_form is not null
                and (rx_ingredient is not null and rx_ingredient like '%/%')
                and rxcui is null),
     cte2 as (select string_agg(DISTINCT CAST(r.rxcui AS varchar), ',') as rxcui,
                     cte1.rxbn,
                     cte1.ing,
                     cte1.rdf
              from faers.rxnconso r
                       join cte1 on lower(r.str) like
                                    lower(concat(replace(cte1.ing, ' ', '%'), '%', cte1.rdf, '%[', cte1.rxbn, ']'))
              where r.sab = 'RXNORM'
                and r.tty = 'SBD'
              group by cte1.rxbn, cte1.ing, cte1.rdf
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte2.rxcui,
    tty   = 'SBD'
from cte2
where cte2.ing = m.rx_ingredient
  and cte2.rxbn = m.rx_brand_name
  and cte2.rdf = m.rx_dose_form
  and m.rxcui is null;


-- name: drug_clean
with cte1 as (select distinct drugname_clean, string_agg(distinct cast(r2.rxcui as text), ',') as cui
              from faers.drug_mapping_exact m
                       join faers.rxnconso r on m.drugname_clean = lower(r.str)
                       join faers.rxnconso r2 on r.rxcui = r2.rxcui
              where r2.tty = 'SBD'
                and r2.sab = 'RXNORM'
                and r2.suppress != 'O'
                and m.rxcui is null
                and m.rx_ingredient is null
              group by drugname_clean
              having count(distinct r2.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte1.cui,
    tty   = 'SBD'
from cte1
where cte1.drugname_clean = m.drugname_clean
  and rxcui is null;