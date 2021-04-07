-- name: drop_ai
DROP TABLE IF EXISTS faers.drug_ai_mapping;
-- name: create_ai
CREATE TABLE faers.drug_ai_mapping AS
SELECT DISTINCT drugname              AS drug_name_original,
                lower(prod_ai)        AS prod_ai,
                cast(NULL AS INTEGER) AS concept_id,
                NULL                  AS update_method,
                NULL                  AS rxcui
FROM faers.drug a
         INNER JOIN faers.unique_all_case b ON a.primaryid = b.primaryid
WHERE b.isr IS NULL
  AND prod_ai IS NOT NULL;

-- name: drop_index
DROP INDEX IF EXISTS faers.prod_ai_ix;
-- name: create_index
CREATE INDEX prod_ai_ix ON faers.drug_ai_mapping (prod_ai);

-- name: update_ai
WITH cte AS (SELECT a.drug_name_original, string_agg(DISTINCT cast(rx.rxcui AS VARCHAR), ',') AS rxcui
             FROM faers.drug_ai_mapping a
                      JOIN faers.rxnconso rx
                           ON lower(rx.str) = lower(a.prod_ai)
             GROUP BY a.drug_name_original)
UPDATE faers.drug_ai_mapping a
SET update_method = 'drug active ingredients',
    rxcui         = cte.rxcui
FROM cte
WHERE cte.drug_name_original = a.drug_name_original;

-- name: update_concept_ids
UPDATE faers.drug_ai_mapping AS drm
SET concept_id = c.concept_id
FROM staging_vocabulary.concept c
WHERE drm.rxcui = c.concept_code
  AND c.vocabulary_id = 'RxNorm';

-- name: update_drug_regex_table
UPDATE faers.drug_mapping AS drm
SET rxcui           = ai.rxcui,
    concept_id      = ai.concept_id,
    drug_name_clean = ai.prod_ai,
    update_method   = 'drug active ingredient'
FROM faers.drug_ai_mapping AS ai
WHERE drm.drug_name_original = ai.drug_name_original
  AND drm.concept_id IS NULL
  AND (drm.rxcui IS NULL
    OR drm.rxcui NOT LIKE '%,%')
  AND ai.concept_id IS NOT NULL;

-- name: append_ambiguous_rxcuis
UPDATE faers.drug_mapping AS drm
SET rxcui         = concat(drm.rxcui, ',', ai.rxcui),
    update_method = 'drug active ingredient ambiguous'
FROM faers.drug_ai_mapping ai
WHERE drm.drug_name_original = ai.drug_name_original
  AND drm.rxcui IS NOT NULL
  AND drm.concept_id IS NULL
  AND ai.rxcui IS NOT NULL
  AND ai.rxcui != drm.rxcui;