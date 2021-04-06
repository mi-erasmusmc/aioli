-- name: pin_to_in
WITH cte1 AS (
    SELECT DISTINCT c.concept_id, dme.rx_ingredient
    FROM faers.drug_mapping_exact dme
             INNER JOIN staging_vocabulary.concept c
                        ON dme.rx_ingredient = lower(c.concept_name)
    WHERE c.concept_class_id = 'Precise Ingredient'
      AND dme.rx_ingredient IS NOT NULL
      AND dme.rxcui IS NULL),
     cte2 AS (
         SELECT cte1.rx_ingredient, string_agg(DISTINCT lower(b.concept_name), ',') AS ingredient
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
                  INNER JOIN cte1 ON cte1.concept_id = concept_id_1
         GROUP BY cte1.rx_ingredient
         HAVING count(DISTINCT lower(a.concept_name)) = 1)
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = cte2.ingredient
FROM cte2
WHERE cte2.rx_ingredient = dme.rx_ingredient
  AND rxcui IS NULL;

-- name: remap_rx_df
UPDATE faers.drug_mapping_exact
SET rx_dose_form = $2
WHERE rx_dose_form = $1;
