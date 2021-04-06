-- create NDA (new drug application) number mapping table
-- (NDA num maps to ingredient(s) in the FDA orange book reference dataset)

-- note the following table should be created one time when the FDA orange book (NDA ingredient lookup) table is loaded
-- name: drop_nda_lookup_table
DROP TABLE IF EXISTS faers.nda_ingredient;
-- name: create_nda_lookup_table
CREATE TABLE faers.nda_ingredient AS
SELECT DISTINCT appl_no, ingredient, trade_name
FROM faers.nda;

-- name: drop_nda_mapping
DROP TABLE IF EXISTS faers.drug_nda_mapping;
-- name: create_nda_mapping
CREATE TABLE faers.drug_nda_mapping AS
SELECT DISTINCT drug_name_original, nda_num, nda_ingredient, concept_id, update_method, rxcui
FROM (
         SELECT DISTINCT drugname              AS drug_name_original,
                         nda_num,
                         NULL                  AS nda_ingredient,
                         cast(NULL AS INTEGER) AS concept_id,
                         NULL                  AS update_method,
                         NULL                  AS rxcui
         FROM faers.drug a
                  INNER JOIN faers.unique_all_case b
                             ON a.primaryid = b.primaryid
         WHERE b.isr IS NULL
           AND nda_num IS NOT NULL
         UNION
         SELECT DISTINCT drugname              AS drug_name_original,
                         cast(nda_num AS VARCHAR),
                         NULL                  AS nda_ingredient,
                         cast(NULL AS INTEGER) AS concept_id,
                         NULL                  AS update_method,
                         NULL                  AS rxcui
         FROM faers.drug_legacy a
                  INNER JOIN faers.unique_all_case b
                             ON cast(a.isr AS VARCHAR) = b.isr
         WHERE b.isr IS NOT NULL
           AND nda_num IS NOT NULL
     ) aa;

-- name: drop_nda_index
DROP INDEX IF EXISTS faers.nda_num_ix;
-- name: create_nda_index
CREATE INDEX nda_num_ix ON faers.drug_nda_mapping (nda_num);
-- name: set_nda_ingredient
UPDATE faers.drug_nda_mapping a
SET nda_ingredient = lower(ndai.ingredient)
FROM faers.nda_ingredient ndai
WHERE ndai.appl_no = a.nda_num;


-- name: map_nda_ingredient_to_rxnorm
WITH cte AS (SELECT a.drug_name_original, string_agg(DISTINCT cast(rx.rxcui AS VARCHAR), ',') AS rxcui
             FROM faers.drug_nda_mapping a
                      JOIN faers.rxnconso rx
                           ON lower(rx.str) = lower(a.nda_ingredient)
             GROUP BY a.drug_name_original)
UPDATE faers.drug_nda_mapping a
SET update_method = 'drug nda_num ingredients',
    rxcui         = cte.rxcui
FROM cte
WHERE cte.drug_name_original = a.drug_name_original;

-- name: update_concept_ids
UPDATE faers.drug_nda_mapping AS drm
SET concept_id = c.concept_id
FROM staging_vocabulary.concept c
WHERE drm.rxcui = c.concept_code
  AND c.vocabulary_id = 'RxNorm';

-- name: update_drug_regex_table
UPDATE faers.drug_mapping AS drm
SET rxcui           = ai.rxcui,
    concept_id      = ai.concept_id,
    drug_name_clean = ai.nda_ingredient,
    update_method   = 'drug nda_num ingredients'
FROM faers.drug_nda_mapping AS ai
WHERE drm.drug_name_original = ai.drug_name_original
  AND drm.concept_id IS NULL
  AND (drm.rxcui IS NULL
    OR drm.rxcui NOT LIKE '%,%')
  AND ai.concept_id IS NOT NULL;

-- name: append_ambiguous_rxcuis
UPDATE faers.drug_mapping AS drm
SET rxcui         = concat(drm.rxcui, ',', ai.rxcui),
    update_method = 'drug nda_num ingredients ambiguous'
FROM faers.drug_nda_mapping ai
WHERE drm.drug_name_original = ai.drug_name_original
  AND drm.rxcui IS NOT NULL
  AND drm.concept_id IS NULL
  AND ai.rxcui IS NOT NULL
  AND ai.rxcui != drm.rxcui;