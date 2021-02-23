-- name: find_match_on_rxconso
WITH cte AS (SELECT a.drug_name_clean, string_agg(DISTINCT CAST(rx.rxcui AS varchar), ',') AS rxcui
             FROM faers.drug_mapping a
                      JOIN faers.rxnconso rx
                           ON lower(rx.str) = lower(a.drug_name_clean)
             GROUP BY drug_name_clean)
UPDATE faers.drug_mapping a
SET update_method = $1,
    rxcui         = cte.rxcui
FROM cte
WHERE lower(cte.drug_name_clean) = lower(a.drug_name_clean)
  AND a.rxcui IS NULL
  AND a.concept_id IS NULL;
