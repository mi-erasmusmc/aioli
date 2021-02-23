-- name: drop_table
DROP TABLE IF EXISTS faers.drug_mapping;

-- name: create_table
CREATE TABLE faers.drug_mapping AS
SELECT DISTINCT drug_name_original,
                drug_name_clean,
                concept_id,
                update_method,
                rxcui,
                standard_concept_id,
                sum(occurrences) as occurrences
FROM (
         SELECT drugname              AS drug_name_original,
                lower(drugname)       AS drug_name_clean,
                cast(NULL AS INTEGER) AS concept_id,
                NULL                  AS update_method,
                NULL                  AS rxcui,
                NULL                  AS standard_concept_id,
                count(*)              AS occurrences
         FROM faers.drug a
                  INNER JOIN faers.unique_all_case b
                             on a.primaryid = b.primaryid
         WHERE b.isr IS NULL
         GROUP BY drugname, lower(drugname)
         UNION
         SELECT drugname              AS drug_name_original,
                lower(drugname)       AS drug_name_clean,
                cast(NULL AS INTEGER) AS concept_id,
                NULL                  AS update_method,
                NULL                  AS rxcui,
                NULL                  AS standard_concept_id,
                count(*)              AS occurrences
         FROM faers.drug_legacy a
                  INNER JOIN faers.unique_all_case b
                             ON cast(a.isr AS VARCHAR) = b.isr
         WHERE b.isr IS NOT NULL
         GROUP BY drugname, lower(drugname)
     ) aa
GROUP BY drug_name_original, drug_name_clean, concept_id, update_method, rxcui, standard_concept_id;

-- name: drop_index_dn_clean
DROP INDEX IF EXISTS faers.drug_name_clean_ix;
-- name: index_dn_clean
CREATE INDEX drug_name_clean_ix ON faers.drug_mapping (lower(drug_name_clean));
-- name: drop_index_dn_original
DROP INDEX IF EXISTS faers.drug_name_original_ix;
-- name: index_dn_original
CREATE INDEX drug_name_original_ix ON faers.drug_mapping (lower(drug_name_original));
-- name: analyze_drug_mapping
ANALYSE VERBOSE faers.drug_mapping;

-- name: drop_index_rxconso
DROP INDEX IF EXISTS faers.rxnorm_str_id;
-- name: index_rxnconso_str
CREATE INDEX rxnorm_str_id ON faers.rxnconso (lower(str), rxcui);
-- name: analyze_rxnconso
ANALYZE VERBOSE faers.rxnconso;

