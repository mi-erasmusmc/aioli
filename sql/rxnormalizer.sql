-- name: find_drugs
SELECT drug_name_clean, sum(occurrences) as sum
FROM faers.drug_mapping
WHERE rxcui IS NULL
  AND concept_id IS NULL
  AND drug_name_clean IS NOT NULL
GROUP BY drug_name_clean
ORDER BY sum DESC
LIMIT 10000;


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