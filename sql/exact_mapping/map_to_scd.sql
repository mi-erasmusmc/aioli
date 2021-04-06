-- name: exact
with cte1 as (select string_agg(distinct cast(r.rxcui as text), ',') as rxcui,
                     m.id
              from faers.rxnconso r
                       join
                   faers.drug_mapping_exact m
                   on lower(r.str) =
                      lower(concat(m.rx_ingredient, ' ', m.dose_amt_clean, ' ', m.dose_unit_clean, ' ',
                                   m.rx_dose_form))
              where m.rx_ingredient is not null
                and m.dose_amt_clean is not null
                and m.dose_unit_clean is not null
                and m.rx_dose_form is not null
                and m.rxcui is null
                and r.tty = 'SCD'
                and r.sab = 'RXNORM'
                and r.suppress != 'O'
              group by m.id, r.str
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte1.rxcui,
    tty   = 'SCD'
from cte1
where cte1.id = m.id;

-- name: ingredient
WITH cte1 AS (select distinct rx_ingredient as ing
              from faers.drug_mapping_exact
              where rx_ingredient is not null
                and rxcui is null),
     cte2 as (select string_agg(distinct cast(r.rxcui as text), ',') as rxcui,
                     cte1.ing
              from faers.rxnconso r
                       join
                   cte1
                   on lower(r.str) like
                      concat(cte1.ing, '%')
                       and r.tty = 'SCD'
                       and r.sab = 'RXNORM'
                       and r.suppress != 'O'
              group by cte1.ing
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte2.rxcui,
    tty   = 'SCD'
from cte2
where cte2.ing = m.rx_ingredient
  and m.rxcui is null;


-- name: ing_and_dose_form
WITH cte1 AS (select distinct rx_ingredient as ing, rx_dose_form as rdf
              from faers.drug_mapping_exact
              where rx_ingredient is not null
                and rx_dose_form is not null
                and rxcui is null),
     cte2 as (select string_agg(distinct cast(r.rxcui as text), ',') as rxcui,
                     cte1.ing,
                     cte1.rdf
              from faers.rxnconso r
                       join
                   cte1
                   on lower(r.str) like
                      concat(cte1.ing, '%', cte1.rdf, '%')
                       and r.tty = 'SCD'
                       and r.sab = 'RXNORM'
                       and r.suppress != 'O'
              group by cte1.ing, cte1.rdf
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte2.rxcui,
    tty   = 'SCD'
from cte2
where cte2.ing = m.rx_ingredient
  and cte2.rdf = m.rx_dose_form
  and m.rxcui is null;


-- name: ing_dose_form_amount
WITH cte1 AS (select rx_ingredient  as ing,
                     rx_dose_form   as rdf,
                     dose_amt_clean as dac
              from faers.drug_mapping_exact
              where rx_ingredient is not null
                and rx_ingredient not like '%/%'
                and rx_dose_form is not null
                and dose_amt_clean is not null
                and rxcui is null
              group by rx_ingredient, rx_dose_form, dose_amt_clean),
     cte2 as (select string_agg(distinct cast(r.rxcui as text), ',') as rxcui,
                     cte1.rdf,
                     cte1.ing,
                     cte1.dac
              from faers.rxnconso r
                       join
                   cte1
                   on lower(r.str) like
                      concat(cte1.ing, '% ', cte1.dac, ' %', cte1.rdf, '%')
                       and r.tty = 'SCD'
                       and r.sab = 'RXNORM'
                       and r.suppress != 'O'
              group by cte1.rdf, cte1.ing, cte1.dac
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte2.rxcui,
    tty   = 'SCD'
from cte2
where cte2.rdf = m.rx_dose_form
  and cte2.ing = m.rx_ingredient
  and cte2.dac = m.dose_amt_clean
  and m.rxcui is null;

-- name: multi_ing
WITH cte1 AS (select distinct rx_ingredient as ing
              from faers.drug_mapping_exact
              where (rx_ingredient is not null and rx_ingredient like '%/%')
                and rxcui is null),
     cte2 as (select distinct string_agg(DISTINCT CAST(r.rxcui AS varchar), ',') as rxcui,
                              cte1.ing
              from faers.rxnconso r
                       join cte1 on lower(r.str) like concat(replace(cte1.ing, ' ', '%'), '%')
              where r.sab = 'RXNORM'
                and r.tty = 'SCD'
                and r.suppress != 'O'
              group by cte1.ing
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte2.rxcui,
    tty   = 'SCD'
from cte2
where cte2.ing = m.rx_ingredient
  and m.rxcui is null;

-- name: multi_ing_df
WITH cte1 AS (select distinct rx_ingredient as ing, rx_dose_form as rdf
              from faers.drug_mapping_exact
              where rx_dose_form is not null
                and (rx_ingredient is not null and rx_ingredient like '%/%')
                and rxcui is null),
     cte2 as (select string_agg(DISTINCT CAST(r.rxcui AS varchar), ',') as rxcui,
                     cte1.ing,
                     cte1.rdf
              from faers.rxnconso r
                       join cte1 on lower(r.str) like
                                    lower(concat(replace(cte1.ing, ' ', '%'), '%', cte1.rdf, '%'))
              where r.sab = 'RXNORM'
                and r.tty = 'SCD'
                and r.suppress != 'O'
              group by cte1.ing, cte1.rdf
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte2.rxcui,
    tty   = 'SCD'
from cte2
where cte2.ing = m.rx_ingredient
  and cte2.rdf = m.rx_dose_form
  and m.rxcui is null;

-- name: drug_clean
with cte1 as (select distinct drugname_clean, string_agg(distinct cast(r2.rxcui as text), ',') as cui
              from faers.drug_mapping_exact m
                       join faers.rxnconso r on m.drugname_clean = lower(r.str)
                       join faers.rxnconso r2 on r.rxcui = r2.rxcui
              where r2.tty = 'SCD'
                and r2.sab = 'RXNORM'
                and r2.suppress != 'O'
                and m.rxcui is null
                and m.rx_ingredient is null
              group by drugname_clean
              having count(distinct r2.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte1.cui,
    tty   = 'SCD'
from cte1
where cte1.drugname_clean = m.drugname_clean
  and rxcui is null;

-- name: drug_clean_extra
with cte1 as (select distinct drugname_clean,
                              rx_dose_form,
                              string_agg(distinct cast(r3.rxcui as text), ',') as cui
              from faers.drug_mapping_exact m
                       join faers.rxnconso r on m.drugname_clean = lower(r.str)
                       join faers.rxnconso r2 on r.rxcui = r2.rxcui
                       join faers.rxnconso r3 on lower(r3.str) like lower(concat(r2.str, '%', rx_dose_form, '%'))
              where r2.tty = 'SCDC'
                and r2.sab = 'RXNORM'
                and r3.tty = 'SCD'
                and r3.str not like '% / %'
                and r3.suppress != 'O'
                and r3.sab = 'RXNORM'
                and m.rxcui is null
              group by drugname_clean, rx_dose_form
              having count(distinct r3.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte1.cui,
    tty   = 'SCD'
from cte1
where cte1.drugname_clean = m.drugname_clean
  and cte1.rx_dose_form = m.rx_dose_form
  and rxcui is null;
