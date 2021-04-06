-- name: exact
with cte1 as (select string_agg(distinct cast(r.rxcui as text), ',') as rxcui,
                     m.id
              from faers.rxnconso r
                       join
                   faers.drug_mapping_exact m
                   on lower(r.str) =
                      lower(concat(m.rx_ingredient, ' ', m.rx_dose_form, ' [', m.rx_brand_name, ']'))
              where m.rx_ingredient is not null
                and m.rx_dose_form is not null
                and m.rx_brand_name is not null
                and m.rxcui is null
                and r.tty = 'SBDF'
                and r.sab = 'RXNORM'
                and r.suppress != 'O'
              group by m.id, r.str
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte1.rxcui,
    tty   = 'SBDF'
from cte1
where cte1.id = m.id;

-- name: loose
WITH cte1 AS (select distinct rx_ingredient as ing, rx_dose_form as rxdf, rx_brand_name as rxbn
              from faers.drug_mapping_exact
              where rx_ingredient is not null
                and rx_dose_form is not null
                and rx_brand_name is not null
                and rxcui is null),
     cte2 as (select string_agg(distinct cast(r.rxcui as text), ',') as rxcui,
                     cte1.ing,
                     cte1.rxdf,
                     cte1.rxbn
              from faers.rxnconso r
                       join
                   cte1
                   on lower(r.str) like
                      concat(cte1.ing, '%', cte1.rxdf, '%[', rxbn, ']')
                       and r.tty = 'SBDF'
                       and r.sab = 'RXNORM'
                       and r.suppress != 'O'
              group by cte1.ing, cte1.rxdf, cte1.rxbn
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte2.rxcui,
    tty   = 'SBDF'
from cte2
where cte2.ing = m.rx_ingredient
  and cte2.rxbn = m.rx_brand_name
  and cte2.rxdf = m.rx_dose_form
  and m.rxcui is null;

-- name: ing_brand
WITH cte1 AS (select distinct rx_ingredient as ing, rx_brand_name as rxbn
              from faers.drug_mapping_exact
              where rx_ingredient is not null
                and rx_brand_name is not null
                and rxcui is null),
     cte2 as (select string_agg(distinct cast(r.rxcui as text), ',') as rxcui,
                     cte1.ing,
                     cte1.rxbn
              from faers.rxnconso r
                       join
                   cte1
                   on lower(r.str) like
                      concat(cte1.ing, '%[', rxbn, ']')
                       and r.tty = 'SBDF'
                       and r.sab = 'RXNORM'
                       and r.suppress != 'O'
              group by cte1.ing, rxbn
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte2.rxcui,
    tty   = 'SBDF'
from cte2
where cte2.ing = m.rx_ingredient
  and cte2.rxbn = m.rx_brand_name
  and m.rxcui is null;

-- name: drug_clean
with cte1 as (select distinct drugname_clean, string_agg(distinct cast(r2.rxcui as text), ',') as cui
              from faers.drug_mapping_exact m
                       join faers.rxnconso r on m.drugname_clean = lower(r.str)
                       join faers.rxnconso r2 on r.rxcui = r2.rxcui
              where r2.tty = 'SBDF'
                and r2.sab = 'RXNORM'
                and r2.suppress != 'O'
                and m.rxcui is null
                and m.rx_ingredient is null
              group by drugname_clean
              having count(distinct r2.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte1.cui,
    tty   = 'SBDF'
from cte1
where cte1.drugname_clean = m.drugname_clean
  and rxcui is null;
