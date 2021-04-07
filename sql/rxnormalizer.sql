-- name: find_drugs
SELECT drug_name_clean, sum(occurrences) AS sum
FROM faers.drug_mapping
WHERE rxcui IS NULL
  AND concept_id IS NULL
  AND drug_name_clean IS NOT NULL
GROUP BY drug_name_clean
ORDER BY sum DESC
LIMIT 60000;

-- name: find_eu_drugs
SELECT drug_name_clean, sum(occurrences) AS sum
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
FROM (
         (SELECT DISTINCT prod_ai_clean AS drug, sum(occurrences) AS oc
          FROM faers.drug_mapping_exact
          WHERE rxcui IS NULL
            AND rx_ingredient IS NULL
            AND prod_ai_clean IS NOT NULL
          GROUP BY prod_ai_clean
          ORDER BY oc DESC
          LIMIT 10000)
         UNION
         (SELECT DISTINCT drugname_clean AS drug, sum(occurrences) AS oc
          FROM faers.drug_mapping_exact
          WHERE rxcui IS NULL
            AND drugname_clean ~ '.*[0-9].*'
            AND drugname_clean IS NOT NULL
          GROUP BY drugname_clean
          ORDER BY oc
          LIMIT 10000)) x
GROUP BY drug;

-- name: get_all_nulls_1
SELECT DISTINCT drugname_clean, sum(occurrences) AS oc
FROM faers.drug_mapping_exact
WHERE rxcui IS NULL
  AND rx_ingredient IS NULL
  AND rx_brand_name IS NULL
  AND drugname_clean IS NOT NULL
GROUP BY drugname_clean
ORDER BY oc DESC
LIMIT 20000 OFFSET 0;

-- name: get_all_nulls_2
SELECT DISTINCT drugname_clean, sum(occurrences) AS oc
FROM faers.drug_mapping_exact
WHERE rxcui IS NULL
  AND rx_ingredient IS NULL
  AND rx_brand_name IS NULL
  AND drugname_clean IS NOT NULL
GROUP BY drugname_clean
ORDER BY oc DESC
LIMIT 20000 OFFSET 19999;

-- name: get_all_nulls_3
SELECT DISTINCT drugname_clean, sum(occurrences) AS oc
FROM faers.drug_mapping_exact
WHERE rxcui IS NULL
  AND rx_ingredient IS NULL
  AND rx_brand_name IS NULL
  AND drugname_clean IS NOT NULL
GROUP BY drugname_clean
ORDER BY oc DESC
LIMIT 20000 OFFSET 39998;

-- name: update_rxcui_exact
UPDATE faers.drug_mapping_exact
SET rxcui = $1
WHERE drugname_clean = $2
  AND rxcui IS NULL;

-- name: set_ingredient
UPDATE faers.drug_mapping_exact
SET rx_ingredient = $1
WHERE (drugname_clean = $2 OR prod_ai_clean = $2)
  AND rx_ingredient IS NULL;

-- name: set_brand
UPDATE faers.drug_mapping_exact
SET rx_brand_name = $1
WHERE drugname_clean = $2
  AND rx_brand_name IS NULL;

-- name: set_scdf
UPDATE faers.drug_mapping_exact
SET rxcui = $1
WHERE drugname_clean = $2
  AND rxcui IS NULL
  AND dose_amt_clean IS NULL;

-- name: set_scdc
UPDATE faers.drug_mapping_exact
SET rxcui = $1
WHERE drugname_clean = $2
  AND rxcui IS NULL
  AND rx_dose_form IS NULL;