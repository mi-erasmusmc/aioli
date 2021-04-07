-- name: populate_standard_concept_id
UPDATE faers.drug_mapping drm
SET standard_concept_id = concept_id;

-- name: to_standard_ingredient
WITH cte1 AS (
    SELECT DISTINCT cast(scdm.standard_concept_id AS INTEGER)
    FROM faers.drug_mapping scdm
             INNER JOIN staging_vocabulary.concept c
                        ON scdm.standard_concept_id = cast(c.concept_id AS VARCHAR)
                            AND scdm.concept_id IS NOT NULL
                            AND (c.concept_class_id NOT IN ('Clinical Drug Form', 'Ingredient', 'Dose Form')
                                OR c.standard_concept IS NULL)
),
     cte2 AS (
         SELECT concept_id_1, string_agg(DISTINCT cast(concept_id_2 AS VARCHAR), ',') AS concept_id_2
         FROM staging_vocabulary.concept_relationship cr
                  INNER JOIN staging_vocabulary.concept a
                             ON cr.concept_id_1 = a.concept_id
                                 AND cr.invalid_reason IS NULL
                                 AND a.vocabulary_id = 'RxNorm'
                  INNER JOIN staging_vocabulary.concept b
                             ON cr.concept_id_2 = b.concept_id
                                 AND b.vocabulary_id = 'RxNorm'
                                 AND b.standard_concept = 'S'
                                 AND b.concept_class_id = 'Ingredient'
                                 AND concept_id_1 IN (SELECT cte1.standard_concept_id FROM cte1)
         GROUP BY concept_id_1
     )
UPDATE faers.drug_mapping scdm
SET standard_concept_id = cte2.concept_id_2
FROM cte2
WHERE scdm.standard_concept_id = cast(cte2.concept_id_1 AS VARCHAR);

-- name: to_standard_ingredient_incl_multi
WITH cte1 AS (
    SELECT DISTINCT cast(scdm.standard_concept_id AS INTEGER)
    FROM faers.drug_mapping scdm
             INNER JOIN staging_vocabulary.concept c
                        ON scdm.standard_concept_id = cast(c.concept_id AS VARCHAR)
                            AND scdm.concept_id IS NOT NULL
                            AND (c.concept_class_id NOT IN ('Ingredient', 'Dose Form')
                                OR c.standard_concept IS NULL)
),
     cte2 AS (
         SELECT concept_id_1, string_agg(DISTINCT cast(concept_id_2 AS VARCHAR), ',') AS concept_id_2
         FROM staging_vocabulary.concept_relationship cr
                  INNER JOIN staging_vocabulary.concept a
                             ON cr.concept_id_1 = a.concept_id
                                 AND cr.invalid_reason IS NULL
                                 AND a.vocabulary_id = 'RxNorm'
                  INNER JOIN staging_vocabulary.concept b
                             ON cr.concept_id_2 = b.concept_id
                                 AND b.vocabulary_id = 'RxNorm'
                                 AND b.standard_concept = 'S'
                                 AND b.concept_class_id = 'Ingredient'
                                 AND concept_id_1 IN (SELECT cte1.standard_concept_id FROM cte1)
         GROUP BY concept_id_1
     )
UPDATE faers.drug_mapping scdm
SET standard_concept_id = cte2.concept_id_2
FROM cte2
WHERE scdm.standard_concept_id = cast(cte2.concept_id_1 AS VARCHAR);

-- name: brands_to_branded_dose_group
WITH cte1 AS (SELECT DISTINCT scdm.concept_id
              FROM faers.drug_mapping scdm
                       INNER JOIN staging_vocabulary.concept c
                                  ON scdm.standard_concept_id = cast(c.concept_id AS VARCHAR)
                                      AND scdm.concept_id IS NOT NULL
                                      AND (c.concept_class_id NOT IN ('Clinical Drug Form', 'Ingredient', 'Dose Form')
                                          OR c.standard_concept IS NULL)),
     cte2 AS (SELECT concept_id_1, max(concept_id_2) AS concept_id_2
              FROM staging_vocabulary.concept_relationship cr
                       INNER JOIN staging_vocabulary.concept a
                                  ON cr.concept_id_1 = a.concept_id
                                      AND cr.invalid_reason IS NULL
                                      AND a.vocabulary_id = 'RxNorm'
                       INNER JOIN staging_vocabulary.concept b
                                  ON cr.concept_id_2 = b.concept_id
                                      AND b.vocabulary_id = 'RxNorm'
                                      AND b.concept_class_id = 'Branded Dose Group'
                                      AND concept_id_1 IN (SELECT cte1.concept_id FROM cte1)
              GROUP BY concept_id_1)
UPDATE faers.drug_mapping scdm
SET standard_concept_id = cte2.concept_id_2
FROM cte2
WHERE scdm.standard_concept_id = cast(cte2.concept_id_1 AS VARCHAR);

-- name: to_clinical_dose_group
WITH cte1 AS (SELECT DISTINCT scdm.standard_concept_id
              FROM faers.drug_mapping scdm
                       INNER JOIN staging_vocabulary.concept c
                                  ON scdm.standard_concept_id = cast(c.concept_id AS VARCHAR)
                                      AND scdm.concept_id IS NOT NULL
                                      AND (c.concept_class_id NOT IN ('Clinical Drug Form', 'Ingredient', 'Dose Form')
                                          OR c.standard_concept IS NULL)),
     cte2 AS (SELECT concept_id_1, max(concept_id_2) AS concept_id_2
              FROM staging_vocabulary.concept_relationship cr
                       INNER JOIN staging_vocabulary.concept a
                                  ON cr.concept_id_1 = a.concept_id
                                      AND cr.invalid_reason IS NULL
                                      AND a.vocabulary_id = 'RxNorm'
                       INNER JOIN staging_vocabulary.concept b
                                  ON cr.concept_id_2 = b.concept_id
                                      AND b.vocabulary_id = 'RxNorm'
                                      AND b.concept_class_id = 'Clinical Dose Group'
                                      AND cast(concept_id_1 AS VARCHAR) IN (SELECT cte1.standard_concept_id FROM cte1)
              GROUP BY concept_id_1)
UPDATE faers.drug_mapping scdm
SET standard_concept_id = cte2.concept_id_2
FROM cte2
WHERE scdm.standard_concept_id = cast(cte2.concept_id_1 AS VARCHAR);

-- name: to_clinical_drug_comp
WITH cte1 AS (SELECT DISTINCT scdm.standard_concept_id
              FROM faers.drug_mapping scdm
                       INNER JOIN staging_vocabulary.concept c
                                  ON scdm.standard_concept_id = cast(c.concept_id AS VARCHAR)
                                      AND scdm.concept_id IS NOT NULL
                                      AND (c.concept_class_id NOT IN ('Clinical Drug Form', 'Ingredient', 'Dose Form')
                                          OR c.standard_concept IS NULL)),
     cte2 AS (SELECT concept_id_1, max(concept_id_2) AS concept_id_2
              FROM staging_vocabulary.concept_relationship cr
                       INNER JOIN staging_vocabulary.concept a
                                  ON cr.concept_id_1 = a.concept_id
                                      AND cr.invalid_reason IS NULL
                                      AND a.vocabulary_id = 'RxNorm'
                       INNER JOIN staging_vocabulary.concept b
                                  ON cr.concept_id_2 = b.concept_id
                                      AND b.vocabulary_id = 'RxNorm'
                                      AND b.concept_class_id = 'Clinical Drug Comp'
                                      AND cast(concept_id_1 AS VARCHAR) IN (SELECT cte1.standard_concept_id FROM cte1)
              GROUP BY concept_id_1
     )
UPDATE faers.drug_mapping scdm
SET standard_concept_id = cte2.concept_id_2
FROM cte2
WHERE scdm.standard_concept_id = cast(cte2.concept_id_1 AS VARCHAR);

-- name: multiple_ingredients_to_clinical_drug_form
WITH cte1 AS (SELECT DISTINCT scdm.standard_concept_id,
                              count(DISTINCT c.concept_name)                               AS ingredient_count,
                              concat('%', string_agg(DISTINCT c.concept_name, '%/%'), '%') AS cdf
              FROM faers.drug_mapping scdm
                       INNER JOIN staging_vocabulary.concept c
                                  ON scdm.standard_concept_id LIKE concat('%', cast(c.concept_id AS VARCHAR), '%')
              WHERE scdm.standard_concept_id LIKE '%,%'
                AND c.concept_class_id = 'Ingredient'
              GROUP BY scdm.standard_concept_id),
     cte2 AS (SELECT max(c.concept_id) AS id, cte1.standard_concept_id AS standadard_id
              FROM staging_vocabulary.concept c
                       JOIN cte1 ON c.concept_name LIKE cte1.cdf
              WHERE concept_class_id = 'Clinical Drug Form'
                AND array_length(regexp_split_to_array(c.concept_name, '/'), 1) = cte1.ingredient_count
                AND standard_concept = 'S'
              GROUP BY cte1.standard_concept_id)
UPDATE faers.drug_mapping drm
SET standard_concept_id = cast(cte2.id AS VARCHAR)
FROM cte2
WHERE cte2.standadard_id = drm.standard_concept_id;


-- name: split_multi_ingredients_to_separate_entries
INSERT INTO faers.drug_mapping (SELECT DISTINCT scdm.drug_name_original,
                                                scdm.drug_name_clean,
                                                scdm.concept_id,
                                                scdm.update_method,
                                                scdm.rxcui,
                                                scdm.standard_concept_id,
                                                scdm.occurrences
                                FROM staging_vocabulary.concept c
                                         JOIN faers.drug_mapping scdm
                                              ON cast(c.concept_id AS VARCHAR) = ANY
                                                 (string_to_array(scdm.standard_concept_id, ','))
                                WHERE scdm.standard_concept_id LIKE '%,%'
                                  AND c.concept_class_id = 'Ingredient'
                                  AND c.standard_concept = 'S'
                                  AND c.invalid_reason IS NULL);


-- name: single_ingredient_clinical_drug_form_to_ingredient
WITH cte1 AS (SELECT DISTINCT cast(scdm.standard_concept_id AS INTEGER)
              FROM faers.drug_mapping scdm
                       INNER JOIN staging_vocabulary.concept c
                                  ON scdm.standard_concept_id = cast(c.concept_id AS VARCHAR)
                                      AND scdm.concept_id IS NOT NULL
                                      AND c.concept_class_id = 'Clinical Drug Form'
                                      AND c.concept_name NOT LIKE '%/%'
),
     cte2 AS (SELECT concept_id_1, string_agg(DISTINCT cast(concept_id_2 AS VARCHAR), ',') AS concept_id_2
              FROM staging_vocabulary.concept_relationship cr
                       INNER JOIN staging_vocabulary.concept a
                                  ON cr.concept_id_1 = a.concept_id
                                      AND cr.invalid_reason IS NULL
                                      AND a.vocabulary_id = 'RxNorm'
                       INNER JOIN staging_vocabulary.concept b
                                  ON cr.concept_id_2 = b.concept_id
                                      AND b.vocabulary_id = 'RxNorm'
                                      AND b.standard_concept = 'S'
                                      AND b.concept_class_id = 'Ingredient'
                                      AND concept_id_1 IN (SELECT cte1.standard_concept_id FROM cte1)
              GROUP BY concept_id_1
     )
UPDATE faers.drug_mapping scdm
SET standard_concept_id = cte2.concept_id_2
FROM cte2
WHERE scdm.standard_concept_id = cast(cte2.concept_id_1 AS VARCHAR);

-- name: clean_unmapped_multi
UPDATE faers.drug_mapping drm
SET standard_concept_id = concept_id
WHERE standard_concept_id LIKE '%,%';

-- name: delete_multi
DELETE
FROM faers.drug_mapping drm
WHERE standard_concept_id LIKE '%,%';

-- name: standardize_residue
WITH cte1 AS (SELECT DISTINCT scdm.standard_concept_id
              FROM faers.drug_mapping scdm
                       INNER JOIN staging_vocabulary.concept c
                                  ON scdm.standard_concept_id = cast(c.concept_id AS VARCHAR)
                                      AND scdm.concept_id IS NOT NULL
                                      AND c.standard_concept IS NULL
                                      AND c.concept_class_id = 'Clinical Drug Form'),
     cte2 AS (
         SELECT concept_id_1, max(concept_id_2) AS concept_id_2
         FROM staging_vocabulary.concept_relationship cr
                  INNER JOIN staging_vocabulary.concept a
                             ON cr.concept_id_1 = a.concept_id
                                 AND cr.invalid_reason IS NULL
                                 AND a.vocabulary_id = 'RxNorm'
                  INNER JOIN staging_vocabulary.concept b
                             ON cr.concept_id_2 = b.concept_id
                                 AND b.vocabulary_id = 'RxNorm'
                                 AND b.standard_concept = 'S'
                                 AND b.concept_class_id = 'Clinical Drug Form'
                                 AND cast(concept_id_1 AS VARCHAR) IN (SELECT cte1.standard_concept_id FROM cte1)
         GROUP BY concept_id_1)
UPDATE faers.drug_mapping scdm
SET standard_concept_id = cte2.concept_id_2
FROM cte2
WHERE scdm.standard_concept_id = cast(cte2.concept_id_1 AS VARCHAR);


-- name: manual_mappings_kenalog
UPDATE faers.drug_mapping
SET standard_concept_id = '903963'
WHERE concept_id = 19026370
   OR standard_concept_id = '19026370';

-- name: manual_mappings_tylenol
UPDATE faers.drug_mapping
SET standard_concept_id = '1125315'
WHERE concept_id = 19052129
   OR standard_concept_id = '19052129';

-- name: manual_mappings_inderal
UPDATE faers.drug_mapping
SET standard_concept_id = '1353766'
WHERE concept_id = 19014310
   OR standard_concept_id = '19014310';

-- name: manual_mappings_allegra
UPDATE faers.drug_mapping
SET standard_concept_id = '40129571'
WHERE concept_id IN (19084668, 40223264, 19088750)
   OR standard_concept_id IN ('19084668', '40223264', '19088750');

-- name: manual_mappings_robitussin
UPDATE faers.drug_mapping
SET standard_concept_id = '40141015'
WHERE concept_id IN (40230101)
   OR standard_concept_id IN ('40230101');

-- name: manual_mapping_reve_vita
UPDATE faers.drug_mapping
SET standard_concept_id = '40156013'
WHERE concept_id IN (40168125)
   OR standard_concept_id IN ('40168125');

-- name: manual_mapping_optiray
UPDATE faers.drug_mapping
SET standard_concept_id = '19069131'
WHERE concept_id IN (19125391)
   OR standard_concept_id IN ('19125391');







