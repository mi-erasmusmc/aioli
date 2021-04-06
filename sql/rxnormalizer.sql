-- name: find_drugs
SELECT drug_name_clean, sum(occurrences) as sum
FROM faers.drug_mapping
WHERE rxcui IS NULL
  AND concept_id IS NULL
  AND drug_name_clean IS NOT NULL
GROUP BY drug_name_clean
ORDER BY sum DESC
LIMIT 60000;

-- name: find_eu_drugs
SELECT drug_name_clean, sum(occurrences) as sum
FROM faers.drug_mapping
WHERE rxcui IS NULL
  AND drug_name_clean IS NOT NULL
  AND lower(update_method) LIKE '%eu%'
GROUP BY drug_name_clean
ORDER BY sum DESC
LIMIT 1000;

-- name: set_rxcui
UPDATE faers.drug_mapping
SET rxcui         = $1,
    update_method = 'RxNormalizer'
WHERE drug_name_clean = $2;

-- name: get_prod_ai_and_numeric_tail
SELECT drug,
       sum(oc)
from (
         (select distinct prod_ai_clean as drug, sum(occurrences) as oc
          from faers.drug_mapping_exact
          where rxcui is null
            and rx_ingredient is null
            and prod_ai_clean is not null
          group by prod_ai_clean
          order by oc desc
          limit 10000)
         union
         (select distinct drugname_clean as drug, sum(occurrences) as oc
          from faers.drug_mapping_exact
          where rxcui is null
            and drugname_clean ~ '.*[0-9].*'
            and drugname_clean is not null
          group by drugname_clean
          order by oc
          limit 10000)) x
group by drug;

-- name: get_all_nulls_1
select distinct drugname_clean, sum(occurrences) as oc
from faers.drug_mapping_exact
where rxcui is null
  and rx_ingredient is null
  and rx_brand_name is null
  and drugname_clean is not null
group by drugname_clean
order by oc desc
limit 20000 offset 0;

-- name: get_all_nulls_2
select distinct drugname_clean, sum(occurrences) as oc
from faers.drug_mapping_exact
where rxcui is null
  and rx_ingredient is null
  and rx_brand_name is null
  and drugname_clean is not null
group by drugname_clean
order by oc desc
limit 20000 offset 19999;

-- name: get_all_nulls_3
select distinct drugname_clean, sum(occurrences) as oc
from faers.drug_mapping_exact
where rxcui is null
  and rx_ingredient is null
  and rx_brand_name is null
  and drugname_clean is not null
group by drugname_clean
order by oc desc
limit 20000 offset 39998;

-- name: update_rxcui_exact
update faers.drug_mapping_exact
set rxcui = $1
where drugname_clean = $2
  and rxcui is null;

-- name: set_ingredient
update faers.drug_mapping_exact
set rx_ingredient = $1
where (drugname_clean = $2 or prod_ai_clean = $2)
  and rx_ingredient is null;

-- name: set_brand
update faers.drug_mapping_exact
set rx_brand_name = $1
where drugname_clean = $2
  and rx_brand_name is null;

-- name: set_scdf
update faers.drug_mapping_exact
set rxcui = $1
where drugname_clean = $2
  and rxcui is null
  and dose_amt_clean is null;

-- name: set_scdc
update faers.drug_mapping_exact
set rxcui = $1
where drugname_clean = $2
  and rxcui is null
  and rx_dose_form is null;