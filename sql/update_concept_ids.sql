-- name: update_concept_ids
UPDATE faers.drug_mapping as drm
SET concept_id = c.concept_id
FROM staging_vocabulary.concept c
WHERE drm.rxcui = c.concept_code
  AND c.vocabulary_id = 'RxNorm'
  AND drm.concept_id IS NULL;

-- name: select_multiple_rxnorm
SELECT DISTINCT drm.rxcui
FROM faers.drug_mapping drm
WHERE drm.rxcui LIKE '%,%'
  AND drm.concept_id IS NULL;

-- name: select_original_name
SELECT DISTINCT lower(drug_name_clean)
FROM faers.drug_mapping drm
WHERE rxcui = $1;

-- name: find_standard_for_multi
SELECT concept_id
FROM staging_vocabulary.concept
WHERE concept_code = ANY ($1)
  AND vocabulary_id = 'RxNorm'
  AND concept_class_id IN ('Clinical Drug Form', 'Ingredient')
  AND standard_concept = 'S';


-- name: multingr_to_clinical_drug_form
UPDATE faers.drug_mapping AS drm
SET concept_id    = cs.concept_id,
    update_method = 'RxNorm min to CDM Clinical Drug From'
FROM (SELECT MAX(c.concept_id) as concept_id, rr.rxcui
      FROM staging_vocabulary.concept c
               JOIN
           faers.rxnconso rr ON
                   LOWER(c.concept_name) LIKE CONCAT(rr.str, '%')
                   AND LOWER(c.concept_name) NOT LIKE CONCAT(rr.str, ' / %')
                   AND rr.tty = 'MIN'
                   AND c.concept_class_id = 'Clinical Drug Form'
                   AND c.standard_concept = 'S'
                   AND c.vocabulary_id = 'RxNorm'
                   AND c.invalid_reason IS NULL
      GROUP BY rr.rxcui) AS cs
WHERE CAST(cs.rxcui AS VARCHAR) = drm.rxcui
  AND drm.concept_id IS NULL;

-- name: update_single_id
UPDATE faers.drug_mapping
SET concept_id = $1
WHERE rxcui = $2
  AND concept_id IS NULL;

-- name: update_for_drug_n
UPDATE faers.drug_mapping
SET concept_id = $1
WHERE lower(drug_name_clean) = $2
  AND concept_id IS NULL;

-- name: find_all
SELECT concept_id, lower(concept_name) as concept_name
FROM staging_vocabulary.concept
WHERE concept_code = ANY ($1)
  AND vocabulary_id = 'RxNorm';

-- name: find_best
SELECT concept_id, lower(concept_name) as concept_name
FROM staging_vocabulary.concept
WHERE concept_code = ANY ($1)
  AND concept_class_id IN ('Clinical Drug Form', 'Ingredient')
  AND standard_concept = 'S'
  AND vocabulary_id = 'RxNorm'
  AND invalid_reason IS NULL;

-- name: find_second_best
SELECT concept_id, lower(concept_name) as concept_name
FROM staging_vocabulary.concept
WHERE concept_code = ANY ($1)
  AND (concept_class_id IN ('Clinical Drug Form', 'Ingredient') OR standard_concept = 'S')
  AND vocabulary_id = 'RxNorm'
  AND invalid_reason IS NULL;

-- name: find_final
SELECT concept_id, lower(concept_name) as concept_name
FROM staging_vocabulary.concept
WHERE concept_code = ANY ($1)
  AND vocabulary_id = 'RxNorm'
  AND invalid_reason IS NULL;

-- name: find_for_string_matching
SELECT DISTINCT lower(drug_name_clean) AS name
FROM faers.drug_mapping drm
WHERE rxcui = $1
  AND concept_id IS NULL;