-- name: drop_scdm_table
DROP TABLE IF EXISTS faers.standard_combined_drug_mapping;

-- name: drop_standard_case_drug
DROP TABLE IF EXISTS faers.standard_case_drug;

-- name: create_scdm_table
CREATE TABLE faers.standard_combined_drug_mapping AS
SELECT DISTINCT primaryid,
                isr,
                drug_seq,
                role_cod,
                drug_name_original,
                lookup_value,
                concept_id,
                update_method,
                standard_concept_id
FROM (
         SELECT DISTINCT b.primaryid,
                         b.isr,
                         drug_seq,
                         role_cod,
                         drugname                                 AS drug_name_original,
                         drm.drug_name_clean                      AS lookup_value,
                         drm.concept_id                           AS concept_id,
                         drm.update_method                        AS update_method,
                         cast(drm.standard_concept_id AS INTEGER) AS standard_concept_id
         FROM faers.drug a
                  INNER JOIN faers.unique_all_case b ON a.primaryid = b.primaryid
                  JOIN faers.drug_mapping drm ON drm.drug_name_original = a.drugname
         WHERE b.isr IS NULL
         UNION
         SELECT DISTINCT b.primaryid,
                         cast(b.isr AS VARCHAR),
                         cast(drug_seq AS VARCHAR),
                         role_cod,
                         drugname                                 AS drug_name_original,
                         drm.drug_name_clean                      AS lookup_value,
                         drm.concept_id                           AS concept_id,
                         drm.update_method                        AS update_method,
                         cast(drm.standard_concept_id AS INTEGER) AS standard_concept_id
         FROM faers.drug_legacy a
                  INNER JOIN faers.unique_all_case b ON a.isr = b.isr
                  JOIN faers.drug_mapping drm ON drm.drug_name_original = a.drugname
         WHERE b.isr IS NOT NULL) aa;

-- name: create_standard_case_drug_original
CREATE TABLE faers.standard_case_drug AS
SELECT DISTINCT a.primaryid, a.isr, a.drug_seq, a.role_cod, a.standard_concept_id
FROM faers.standard_combined_drug_mapping a
         INNER JOIN staging_vocabulary.concept c
                    ON a.standard_concept_id = c.concept_id
                        AND c.concept_class_id IN ('Ingredient', 'Clinical Drug Form')
                        AND c.standard_concept = 'S';

-- name: create_standard_case_drug_atc
CREATE TABLE faers.standard_case_drug AS
SELECT DISTINCT a.primaryid, a.isr, a.drug_seq, a.role_cod, a.atc_code AS standard_concept_id
FROM faers.standard_combined_drug_mapping a;

-- name: expand_table_atc
ALTER TABLE faers.standard_combined_drug_mapping
    ADD COLUMN atc_code       TEXT,
    ADD COLUMN atc_method     TEXT,
    ADD COLUMN atc_concept_id INTEGER;

-- name: add_atc_current
UPDATE faers.standard_combined_drug_mapping scdm
SET atc_code   = atc.atc_code,
    atc_method = atc.atc_method
FROM faers.atc_case_drug_c atc
WHERE scdm.primaryid = atc.primaryid
  AND scdm.drug_seq = atc.drug_seq
  AND scdm.role_cod = atc.role_cod;

-- name: add_atc_legacy
UPDATE faers.standard_combined_drug_mapping scdm
SET atc_code   = atc.atc_code,
    atc_method = atc.atc_method
FROM faers.atc_case_drug_l atc
WHERE scdm.isr = atc.isr
  AND scdm.drug_seq = atc.drug_seq
  AND scdm.role_cod = atc.role_cod;

-- name: populate_atc_from_regular_mapping
UPDATE faers.standard_combined_drug_mapping scdm
SET atc_code   = p.atc,
    atc_method = 'patch from concept id'
FROM faers.rxnorm_atc_patch p
WHERE p.code = scdm.concept_id
  AND scdm.atc_code IS NULL;

-- name: populate_atc_from_regular_mapping_standard_id
UPDATE faers.standard_combined_drug_mapping scdm
SET atc_code   = p.atc,
    atc_method = 'patch from standard concept id'
FROM faers.rxnorm_atc_patch p
WHERE p.code = scdm.standard_concept_id
  AND scdm.atc_code IS NULL;

-- name: populate_atc_from_regular_mapping_infer
WITH cte1 AS (SELECT DISTINCT lower(c.concept_name) AS concept_name, c.concept_id
              FROM staging_vocabulary.concept c
                       JOIN faers.standard_combined_drug_mapping scdm ON scdm.standard_concept_id = c.concept_id
              WHERE scdm.atc_code IS NULL
                AND c.concept_class_id = 'Ingredient'),
     cte2 AS (SELECT string_agg(DISTINCT p.atc, ',') AS atc, cte1.concept_id
              FROM faers.rxnorm_atc_patch p
                       JOIN cte1 ON lower(p.name) LIKE concat(cte1.concept_name, '%')
              WHERE "ingredients" = 1
              GROUP BY cte1.concept_id
              HAVING count(DISTINCT p.atc) = 1)
UPDATE faers.standard_combined_drug_mapping scdm
SET atc_code   = cte2.atc,
    atc_method = 'patch inferred from standard concept id'
FROM cte2
WHERE cte2.concept_id = scdm.concept_id
  AND scdm.atc_code IS NULL;

-- name: set_vocab_id_atc
UPDATE faers.standard_combined_drug_mapping s
SET atc_concept_id = c.concept_id
FROM staging_vocabulary.concept c
WHERE c.concept_code = s.atc_code
  AND c.vocabulary_id = 'ATC';