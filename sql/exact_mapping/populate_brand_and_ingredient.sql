-- name: populate_brand_from_drug
with cte1 as (select distinct drugname_clean, lower(r2.str) as brand_name
              from faers.drug_mapping_exact m
                       join faers.rxnconso r on m.drugname_clean = lower(r.str)
                       join faers.rxnconso r2 on r.rxcui = r2.rxcui
              where r2.tty = 'BN'
                and r2.sab = 'RXNORM'
                and m.rxcui is null
                and m.rx_brand_name is null)
update faers.drug_mapping_exact m
set rx_brand_name = cte1.brand_name
from cte1
where cte1.drugname_clean = m.drugname_clean
  and m.rx_brand_name IS NULL;

-- name: populate_brand_from_drug_first_word
with cte1 as (select m.id, string_agg(distinct lower(r2.str), ',') as brand_name
              from faers.drug_mapping_exact m
                       join faers.rxnconso r on lower(r.str) = substring(drugname_clean from '[^ ]+'::text)
                       join faers.rxnconso r2 on r.rxcui = r2.rxcui
              where r2.tty = 'BN'
                and r2.sab = 'RXNORM'
                and m.rxcui is null
                and m.drugname_clean like '% %'
                and m.rx_brand_name is null
                and m.occurrences > 1
              group by m.id
              having count(distinct lower(r2.str)) = 1)
update faers.drug_mapping_exact m
set rx_brand_name = cte1.brand_name
from cte1
where cte1.id = m.id
  and m.rx_brand_name is null;

-- name: populate_brand_from_any_word_in_drug
with cte1 as (select m.id, string_agg(distinct lower(r2.str), ',') as brand_name
              from faers.drug_mapping_exact m
                       join faers.rxnconso r on lower(r.str) = ANY (string_to_array(drugname_clean, ' '))
                       join faers.rxnconso r2 on r.rxcui = r2.rxcui
              where r2.tty = 'BN'
                and r2.sab = 'RXNORM'
                and m.rxcui is null
                and m.drugname_clean like '% %'
                and m.rx_brand_name is null
                and array_length(string_to_array(r2.str, ' '), 1) = 1
                and m.occurrences > 1
              group by m.id
              having count(distinct lower(r2.str)) = 1)
update faers.drug_mapping_exact m
set rx_brand_name = cte1.brand_name
from cte1
where cte1.id = m.id
  and m.rx_brand_name is null;

-- name: populate_ingredient_from_prod_ai
with cte1 as (select distinct prod_ai_clean, lower(r2.str) as ingredient
              from faers.drug_mapping_exact m
                       join faers.rxnconso r on m.prod_ai_clean = lower(r.str)
                       join faers.rxnconso r2 on r.rxcui = r2.rxcui
              where r2.tty in ('IN', 'PIN', 'MIN')
                and r2.sab = 'RXNORM'
                and m.rxcui is null
                and m.rx_ingredient is null)
update faers.drug_mapping_exact m
set rx_ingredient = cte1.ingredient
from cte1
where cte1.prod_ai_clean = m.prod_ai_clean
  and m.rx_ingredient is null;


-- name: populate_ingredient_from_drug
with cte1 as (select distinct drugname_clean, string_agg(distinct lower(r2.str), ',') as ingredient
              from faers.drug_mapping_exact m
                       join faers.rxnconso r on m.drugname_clean = lower(r.str)
                       join faers.rxnconso r2 on r.rxcui = r2.rxcui
              where r2.tty in ('IN', 'PIN', 'MIN')
                and r2.sab = 'RXNORM'
                and m.rxcui is null
                and m.rx_ingredient is null
              group by drugname_clean
              having count(distinct lower(r2.str)) = 1)
update faers.drug_mapping_exact m
set rx_ingredient = cte1.ingredient
from cte1
where cte1.drugname_clean = m.drugname_clean
  and rx_ingredient is null;

-- name: sbd
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

-- name: scd
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

-- name: from_nda_prepend_zero
with cte1 as (select distinct string_agg(DISTINCT lower(r2.str), ',') as ingredient,
                              m.drugname_clean,
                              m.nda_num
              from faers.drug_mapping_exact m
                       join faers.nda nda on nda.appl_no = concat('0', m.nda_num)
                       join faers.rxnconso r on lower(r.str) = concat(lower(replace(nda.ingredient, ';', ' /')))
                       join faers.rxnconso r2 on r.rxcui = r2.rxcui
              where (m.drugname_clean like lower(concat('%', nda.ingredient, '%'))
                  or m.prod_ai_clean like lower(concat('%', nda.ingredient, '%'))
                  or m.drugname_clean like lower(concat('%', nda.trade_name, '%')))
                and m.rx_ingredient is null
                and m.rxcui is null
                and m.nda_num is not null
                and r2.sab = 'RXNORM'
                and r2.tty in ('IN', 'MIN', 'PIN')
                and r2.suppress != 'O'
              group by m.drugname_clean, m.nda_num
              having count(distinct lower(r2.str)) = 1)
update faers.drug_mapping_exact m
set rx_ingredient = cte1.ingredient
from cte1
where cte1.drugname_clean = m.drugname_clean
  and cte1.nda_num = m.nda_num
  and m.rx_ingredient is null;

-- name: from_nda
with cte1 as (select distinct string_agg(DISTINCT lower(r2.str), ',') as ingredient,
                              m.drugname_clean,
                              m.nda_num
              from faers.drug_mapping_exact m
                       join faers.nda nda on nda.appl_no = m.nda_num
                       join faers.rxnconso r on lower(r.str) = concat(lower(replace(nda.ingredient, ';', ' /')))
                       join faers.rxnconso r2 on r.rxcui = r2.rxcui
              where (m.drugname_clean like lower(concat('%', nda.ingredient, '%'))
                  or m.prod_ai_clean like lower(concat('%', nda.ingredient, '%'))
                  or m.drugname_clean like lower(concat('%', nda.trade_name, '%')))
                and m.rx_ingredient is null
                and m.rxcui is null
                and m.nda_num is not null
                and r2.sab = 'RXNORM'
                and r2.tty in ('IN', 'MIN', 'PIN')
                and r2.suppress != 'O'
              group by m.drugname_clean, m.nda_num
              having count(distinct lower(r2.str)) = 1)
update faers.drug_mapping_exact m
set rx_ingredient = cte1.ingredient
from cte1
where cte1.drugname_clean = m.drugname_clean
  and cte1.nda_num = m.nda_num
  and m.rx_ingredient is null;
