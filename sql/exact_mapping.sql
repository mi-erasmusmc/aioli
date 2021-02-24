select distinct prod_ai, r.rxcui, lower(r3.str)
from faers.drug_mapping_exact m
         join faers.rxnconso r on m.prod_ai = lower(r.str)
         join faers.rxnconso r2 on r.rxcui = r2.rxcui
         join faers.rxnrel rel on cast(r2.rxcui as text) = rel.RXCUI1
         join faers.rxnconso r3 on rel.RXCUI2 = cast(r3.rxcui as text)
where r2.tty = 'PIN'
  and r2.sab = 'RXNORM'
  and r3.tty = 'IN'
limit 2;


-- name: populate_brand_from_drug
with cte1 as (select distinct drugname_clean, lower(r2.str) as brand_name
              from faers.drug_mapping_exact m
                       join faers.rxnconso r on m.drugname_clean = lower(r.str)
                       join faers.rxnconso r2 on r.rxcui = r2.rxcui
              where r2.tty = 'BN'
                and r2.sab = 'RXNORM')
update faers.drug_mapping_exact m
set rx_brand_name = cte1.brand_name
from cte1
where cte1.drugname_clean = m.drugname_clean
  and m.rx_brand_name IS NULL;

-- name: populate_ingredient_from_prod_ai
with cte1 as (select distinct prod_ai, lower(r2.str) as ingredient
              from faers.drug_mapping_exact m
                       join faers.rxnconso r on m.prod_ai = lower(r.str)
                       join faers.rxnconso r2 on r.rxcui = r2.rxcui
              where r2.tty in ('IN', 'PIN', 'MIN')
                and r2.sab = 'RXNORM')
update faers.drug_mapping_exact m
set rx_ingredient = cte1.ingredient
from cte1
where cte1.prod_ai = m.prod_ai
  AND m.rx_ingredient IS NULL;


-- name: populate_ingredient_from_drug
with cte1 as (select distinct drugname_clean, lower(r2.str) as ingredient
              from faers.drug_mapping_exact m
                       join faers.rxnconso r on m.drugname_clean = lower(r.str)
                       join faers.rxnconso r2 on r.rxcui = r2.rxcui
              where r2.tty in ('IN', 'PIN', 'MIN')
                and r2.sab = 'RXNORM')
update faers.drug_mapping_exact m
set rx_ingredient = cte1.ingredient
from cte1
where cte1.drugname_clean = m.drugname_clean
  and rx_ingredient is null;

-- name: drop_table
DROP TABLE IF EXISTS faers.drug_mapping_exact;

-- name: create_table
CREATE TABLE faers.drug_mapping_exact AS
SELECT row_number() over () as id,
       upper(drugname)      as drugname,
       upper(prod_ai)       as prod_ai,
       lower(route)         as route,
       lower(dose_amt)      as dose_amt,
       lower(dose_form)     as dose_form,
       lower(dose_unit)     as dose_unit,
       lower(nda_num)       as nda_num,
       lower(drugname)      as drugname_clean,
       lower(prod_ai)       as prod_ai_clean,
       null                 as route_clean,
       null                 as dose_amt_clean,
       null                 as dose_form_clean,
       null                 as dose_unit_clean,
       null                 as rx_brand_name,
       null                 as rx_ingredient,
       null                 as rxcui,
       null                 as str,
       null                 as tty,
       count(*)             as occurrences
FROM faers.drug
WHERE route IS NOT NULL
   OR dose_amt IS NOT NULL
   OR dose_form IS NOT NULL
   OR dose_unit IS NOT NULL
GROUP BY drugname, prod_ai, route, dose_amt, dose_form, dose_unit, nda_num;

-- name: set_dose_amt_clean
UPDATE faers.drug_mapping_exact
SET dose_amt_clean = dose_amt;

-- name: prepend_zero_dose_amt
UPDATE faers.drug_mapping_exact
SET dose_amt_clean = concat('0', dose_amt_clean)
WHERE dose_amt_clean LIKE '.%';


UPDATE faers.drug_mapping_exact
SET route_clean = route
where route not in ('unknown', 'other');

UPDATE faers.drug_mapping_exact
SET route_clean = 'inject'
WHERE route LIKE '%intravenous%';

UPDATE faers.drug_mapping_exact
SET route_clean = 'oral'
where route_clean is null
  and dose_form like '%oral%';

UPDATE faers.drug_mapping_exact
SET dose_form = 'inhal'
    and dose_form like 'respiratory (inhalation)';



UPDATE faers.drug_mapping_exact
SET dose_form_clean = dose_form
where dose_form not in
      ('unknown', 'unspecified', 'unk', 'unknown formulation', 'formulation unknown', 'per oral nos',
       'investigational dosage form', 'other');

UPDATE faers.drug_mapping_exact
SET dose_form_clean = 'tablet'
where dose_form in
      ('tablets', 'chewable tablet', 'orodispersible tablet', 'dispersible tablet', 'tablet (extended release)')
   or dose_form like '%release tablet%'
   or dose_form like '%coated tablet%';

UPDATE faers.drug_mapping_exact
SET dose_form_clean = 'capsule'
where dose_form in ('capsules', 'capsule, hard', 'capsule, soft', 'caplet', 'slow release capsules');

UPDATE faers.drug_mapping_exact
SET dose_form_clean = 'inject'
where dose_form like '%inject%'
   or dose_form like '%infus%';

UPDATE faers.drug_mapping_exact
SET drugname_clean = drugname;

UPDATE faers.drug_mapping_exact
SET prod_ai_clean = prod_ai;

select distinct route
from faers.drug_mapping_exact;


select distinct str
from faers.rxnconso
where tty = 'DF'
  and sab = 'RXNORM';

update faers.drug_mapping_exact
set dose_form = replace(route_clean, $1, '')
where route_clean like '%$1%';






