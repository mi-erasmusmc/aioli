-- name: single_ingredients_from_article_57
with cte as (select distinct lower(rx.str) as str, dme.drugname_clean
             from faers.article eu
                      join faers.drug_mapping_exact dme on dme.drugname_clean = lower(eu.name)
                      join faers.rxnconso rx on lower(rx.str) = lower(eu.ingredient)
             where dme.rx_ingredient is null
               and rx.sab = 'RXNORM'
               and rx.tty = 'IN')
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = cte.str
from cte
where cte.drugname_clean = dme.drugname_clean
  and dme.rx_ingredient is null;

-- name: pin_from_article_57
with cte as (select distinct lower(rx.str) as str, dme.drugname_clean
             from faers.article eu
                      join faers.drug_mapping_exact dme on dme.drugname_clean = lower(eu.name)
                      join faers.rxnconso rx on lower(rx.str) = lower(eu.ingredient)
             where dme.rx_ingredient is null
               and rx.sab = 'RXNORM'
               and rx.tty = 'PIN')
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = cte.str
from cte
where cte.drugname_clean = dme.drugname_clean
  and dme.rx_ingredient is null;

-- name: article_57_alternative_spellings
with cte as (select distinct string_agg(distinct lower(rx2.str), ',') as str, dme.drugname_clean
             from faers.article eu
                      join faers.drug_mapping_exact dme on dme.drugname_clean = lower(eu.name)
                      join faers.rxnconso rx1 on lower(rx1.str) = lower(eu.ingredient)
                      join faers.rxnconso rx2 on rx1.rxcui = rx2.rxcui
             where dme.rx_ingredient is null
               and rx2.sab = 'RXNORM'
               and rx2.tty in ('MIN', 'IN')
             group by dme.drugname_clean
             having count(distinct lower(rx2.str)) = 1)
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = cte.str
from cte
where cte.drugname_clean = dme.drugname_clean
  and dme.rx_ingredient is null;

-- name: article_57_alternative_spellings_incl_pin
with cte as (select distinct string_agg(distinct lower(rx2.str), ',') as str, dme.drugname_clean
             from faers.article eu
                      join faers.drug_mapping_exact dme on dme.drugname_clean = lower(eu.name)
                      join faers.rxnconso rx1 on lower(rx1.str) = lower(eu.ingredient)
                      join faers.rxnconso rx2 on rx1.rxcui = rx2.rxcui
             where dme.rx_ingredient is null
               and rx2.sab = 'RXNORM'
               and rx2.tty in ('IN', 'MIN', 'PIN')
             group by dme.drugname_clean
             having count(distinct lower(rx2.str)) = 1)
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = cte.str
from cte
where cte.drugname_clean = dme.drugname_clean
  and dme.rx_ingredient is null;

-- name: article_57_multi_ingr
with cte as (select distinct lower(rx1.str), lower(replace(eu.ingredient, ',', ' /')) as str, dme.drugname_clean
             from faers.article eu
                      join faers.drug_mapping_exact dme on dme.drugname_clean = lower(eu.name)
                      join faers.rxnconso rx1 on lower(rx1.str) = lower(replace(eu.ingredient, ',', ' /'))
             where dme.rx_ingredient is null
               and rx1.sab = 'RXNORM'
               and rx1.tty in ('IN', 'MIN', 'PIN')
)
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = cte.str
from cte
where cte.drugname_clean = dme.drugname_clean
  and dme.rx_ingredient is null;


-- name: set_prod_ai
with cte as (select distinct string_agg(distinct lower(eu.ingredient), ',') as str, dme.drugname_clean
             from faers.article eu
                      join faers.drug_mapping_exact dme on dme.drugname_clean = lower(eu.name)
             where dme.rx_ingredient is null
               and dme.prod_ai_clean is null
             group by dme.drugname_clean
             having count(distinct lower(eu.ingredient)) = 1
)
UPDATE faers.drug_mapping_exact dme
SET prod_ai_clean = cte.str
from cte
where cte.drugname_clean = dme.drugname_clean
  and dme.rx_ingredient is null
  and dme.prod_ai_clean is null;
