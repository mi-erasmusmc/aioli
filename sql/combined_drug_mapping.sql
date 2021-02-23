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
                  INNER JOIN faers.unique_all_case b ON a.isr = cast(b.isr AS INTEGER)
                  JOIN faers.drug_mapping drm ON drm.drug_name_original = a.drugname
         WHERE b.isr IS NOT NULL) aa;

-- name: create_standard_case_drug
CREATE TABLE faers.standard_case_drug AS
SELECT DISTINCT a.primaryid, a.isr, a.drug_seq, a.role_cod, a.standard_concept_id
FROM faers.standard_combined_drug_mapping a
         INNER JOIN staging_vocabulary.concept c
                    ON a.standard_concept_id = c.concept_id
                        AND c.concept_class_id IN ('Ingredient', 'Clinical Drug Form')
                        AND c.standard_concept = 'S';
