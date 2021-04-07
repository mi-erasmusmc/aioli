-- name: single_ingredients_from_article_57
WITH cte AS (SELECT DISTINCT lower(rx.str) AS str, dme.drugname_clean
             FROM faers.article eu
                      JOIN faers.drug_mapping_exact dme ON dme.drugname_clean = lower(eu.name)
                      JOIN faers.rxnconso rx ON lower(rx.str) = lower(eu.ingredient)
             WHERE dme.rx_ingredient IS NULL
               AND rx.sab = 'RXNORM'
               AND rx.tty = 'IN')
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = cte.str
FROM cte
WHERE cte.drugname_clean = dme.drugname_clean
  AND dme.rx_ingredient IS NULL;

-- name: pin_from_article_57
WITH cte AS (SELECT DISTINCT lower(rx.str) AS str, dme.drugname_clean
             FROM faers.article eu
                      JOIN faers.drug_mapping_exact dme ON dme.drugname_clean = lower(eu.name)
                      JOIN faers.rxnconso rx ON lower(rx.str) = lower(eu.ingredient)
             WHERE dme.rx_ingredient IS NULL
               AND rx.sab = 'RXNORM'
               AND rx.tty = 'PIN')
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = cte.str
FROM cte
WHERE cte.drugname_clean = dme.drugname_clean
  AND dme.rx_ingredient IS NULL;

-- name: article_57_alternative_spellings
WITH cte AS (SELECT DISTINCT string_agg(DISTINCT lower(rx2.str), ',') AS str, dme.drugname_clean
             FROM faers.article eu
                      JOIN faers.drug_mapping_exact dme ON dme.drugname_clean = lower(eu.name)
                      JOIN faers.rxnconso rx1 ON lower(rx1.str) = lower(eu.ingredient)
                      JOIN faers.rxnconso rx2 ON rx1.rxcui = rx2.rxcui
             WHERE dme.rx_ingredient IS NULL
               AND rx2.sab = 'RXNORM'
               AND rx2.tty IN ('MIN', 'IN')
             GROUP BY dme.drugname_clean
             HAVING count(DISTINCT lower(rx2.str)) = 1)
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = cte.str
FROM cte
WHERE cte.drugname_clean = dme.drugname_clean
  AND dme.rx_ingredient IS NULL;

-- name: article_57_alternative_spellings_incl_pin
WITH cte AS (SELECT DISTINCT string_agg(DISTINCT lower(rx2.str), ',') AS str, dme.drugname_clean
             FROM faers.article eu
                      JOIN faers.drug_mapping_exact dme ON dme.drugname_clean = lower(eu.name)
                      JOIN faers.rxnconso rx1 ON lower(rx1.str) = lower(eu.ingredient)
                      JOIN faers.rxnconso rx2 ON rx1.rxcui = rx2.rxcui
             WHERE dme.rx_ingredient IS NULL
               AND rx2.sab = 'RXNORM'
               AND rx2.tty IN ('IN', 'MIN', 'PIN')
             GROUP BY dme.drugname_clean
             HAVING count(DISTINCT lower(rx2.str)) = 1)
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = cte.str
FROM cte
WHERE cte.drugname_clean = dme.drugname_clean
  AND dme.rx_ingredient IS NULL;

-- name: article_57_multi_ingr
WITH cte AS (SELECT DISTINCT lower(rx1.str), lower(replace(eu.ingredient, ',', ' /')) AS str, dme.drugname_clean
             FROM faers.article eu
                      JOIN faers.drug_mapping_exact dme ON dme.drugname_clean = lower(eu.name)
                      JOIN faers.rxnconso rx1 ON lower(rx1.str) = lower(replace(eu.ingredient, ',', ' /'))
             WHERE dme.rx_ingredient IS NULL
               AND rx1.sab = 'RXNORM'
               AND rx1.tty IN ('IN', 'MIN', 'PIN')
)
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = cte.str
FROM cte
WHERE cte.drugname_clean = dme.drugname_clean
  AND dme.rx_ingredient IS NULL;


-- name: set_prod_ai
WITH cte AS (SELECT DISTINCT string_agg(DISTINCT lower(eu.ingredient), ',') AS str, dme.drugname_clean
             FROM faers.article eu
                      JOIN faers.drug_mapping_exact dme ON dme.drugname_clean = lower(eu.name)
             WHERE dme.rx_ingredient IS NULL
               AND dme.prod_ai_clean IS NULL
             GROUP BY dme.drugname_clean
             HAVING count(DISTINCT lower(eu.ingredient)) = 1
)
UPDATE faers.drug_mapping_exact dme
SET prod_ai_clean = cte.str
FROM cte
WHERE cte.drugname_clean = dme.drugname_clean
  AND dme.rx_ingredient IS NULL
  AND dme.prod_ai_clean IS NULL;
