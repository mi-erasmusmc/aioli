SET SEARCH_PATH = faers;

ALTER TABLE drug_mapping_exact_java
    DROP COLUMN IF EXISTS final_id;

ALTER TABLE drug_mapping_exact_java
    ADD final_id INT;

WITH cte1 AS (SELECT DISTINCT rxcui FROM drug_mapping_exact_java WHERE rx_ingredient IS NULL),
     cte2 AS (SELECT DISTINCT cte1.rxcui, lower(string_agg(DISTINCT r_in.str, ' / ' ORDER BY r_in.str)) AS ingr
              FROM cte1
                       JOIN rxnorm.rxnrel rel ON rel.rxcui1 = cte1.rxcui
                       JOIN rxnorm.rxnconso r_in ON rel.rxcui2 = r_in.rxcui
                  AND r_in.tty IN ('IN', 'MIN')
                  AND r_in.sab = 'RXNORM'
              GROUP BY cte1.rxcui),
     cte3 AS (SELECT DISTINCT cte2.rxcui AS rxcui, r.str AS ingredient
              FROM rxnorm.rxnconso r
                       JOIN cte2 ON lower(r.str) = cte2.ingr AND r.tty IN ('IN', 'MIN')
                  AND r.sab = 'RXNORM')
UPDATE drug_mapping_exact_java dme
SET rx_ingredient = cte3.ingredient
FROM cte3
WHERE dme.rxcui = cte3.rxcui
  AND dme.rx_ingredient IS NULL;

WITH cte1 AS (SELECT DISTINCT rx_ingredient FROM drug_mapping_exact_java),
     cte2 AS (SELECT DISTINCT cte1.rx_ingredient, c.concept_id
              FROM cte1
                       JOIN staging_vocabulary.concept c
                            ON lower(c.concept_name) = lower(cte1.rx_ingredient)
                                AND c.vocabulary_id = 'RxNorm'
                                AND c.invalid_reason IS NULL
                                AND c.standard_concept = 'S')
UPDATE faers.drug_mapping_exact_java AS drm
SET final_id = cte2.concept_id
FROM cte2
WHERE drm.rx_ingredient = cte2.rx_ingredient;

UPDATE faers.drug_mapping_exact_java AS drm
SET final_id = cs.concept_id,
    atc_str  = cs.str
FROM (SELECT max(c.concept_id) AS concept_id, rr.str
      FROM staging_vocabulary.concept c
               JOIN
           rxnorm.rxnconso rr ON
                       lower(c.concept_name) LIKE concat(rr.str, '%')
                   AND lower(c.concept_name) NOT LIKE concat(rr.str, ' / %')
                   AND rr.tty = 'MIN'
                   AND c.concept_class_id = 'Clinical Drug Form'
                   AND c.standard_concept = 'S'
                   AND c.vocabulary_id = 'RxNorm'
                   AND c.invalid_reason IS NULL
      GROUP BY rr.str) AS cs
WHERE cs.str = drm.rx_ingredient
  AND drm.final_id IS NULL;