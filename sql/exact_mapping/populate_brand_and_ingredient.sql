-- name: populate_brand_from_drug
WITH cte1 AS (SELECT DISTINCT drugname_clean, lower(r2.str) AS brand_name
              FROM faers.drug_mapping_exact m
                       JOIN faers.rxnconso r ON m.drugname_clean = lower(r.str)
                       JOIN faers.rxnconso r2 ON r.rxcui = r2.rxcui
              WHERE r2.tty = 'BN'
                AND r2.sab = 'RXNORM'
                AND m.rxcui IS NULL
                AND m.rx_brand_name IS NULL)
UPDATE faers.drug_mapping_exact m
SET rx_brand_name = cte1.brand_name
FROM cte1
WHERE cte1.drugname_clean = m.drugname_clean
  AND m.rx_brand_name IS NULL;

-- name: populate_brand_from_drug_first_word
WITH cte1 AS (SELECT m.id, string_agg(DISTINCT lower(r2.str), ',') AS brand_name
              FROM faers.drug_mapping_exact m
                       JOIN faers.rxnconso r ON lower(r.str) = substring(drugname_clean FROM '[^ ]+'::TEXT)
                       JOIN faers.rxnconso r2 ON r.rxcui = r2.rxcui
              WHERE r2.tty = 'BN'
                AND r2.sab = 'RXNORM'
                AND m.rxcui IS NULL
                AND m.drugname_clean LIKE '% %'
                AND m.rx_brand_name IS NULL
                AND m.occurrences > 1
              GROUP BY m.id
              HAVING count(DISTINCT lower(r2.str)) = 1)
UPDATE faers.drug_mapping_exact m
SET rx_brand_name = cte1.brand_name
FROM cte1
WHERE cte1.id = m.id
  AND m.rx_brand_name IS NULL;

-- name: populate_brand_from_any_word_in_drug
WITH cte1 AS (SELECT m.id, string_agg(DISTINCT lower(r2.str), ',') AS brand_name
              FROM faers.drug_mapping_exact m
                       JOIN faers.rxnconso r ON lower(r.str) = ANY (string_to_array(drugname_clean, ' '))
                       JOIN faers.rxnconso r2 ON r.rxcui = r2.rxcui
              WHERE r2.tty = 'BN'
                AND r2.sab = 'RXNORM'
                AND m.rxcui IS NULL
                AND m.drugname_clean LIKE '% %'
                AND m.rx_brand_name IS NULL
                AND array_length(string_to_array(r2.str, ' '), 1) = 1
                AND m.occurrences > 1
              GROUP BY m.id
              HAVING count(DISTINCT lower(r2.str)) = 1)
UPDATE faers.drug_mapping_exact m
SET rx_brand_name = cte1.brand_name
FROM cte1
WHERE cte1.id = m.id
  AND m.rx_brand_name IS NULL;

-- name: populate_ingredient_from_prod_ai
WITH cte1 AS (SELECT DISTINCT prod_ai_clean, lower(r2.str) AS ingredient
              FROM faers.drug_mapping_exact m
                       JOIN faers.rxnconso r ON m.prod_ai_clean = lower(r.str)
                       JOIN faers.rxnconso r2 ON r.rxcui = r2.rxcui
              WHERE r2.tty IN ('IN', 'PIN', 'MIN')
                AND r2.sab = 'RXNORM'
                AND m.rxcui IS NULL
                AND m.rx_ingredient IS NULL)
UPDATE faers.drug_mapping_exact m
SET rx_ingredient = cte1.ingredient
FROM cte1
WHERE cte1.prod_ai_clean = m.prod_ai_clean
  AND m.rx_ingredient IS NULL;


-- name: populate_ingredient_from_drug
WITH cte1 AS (SELECT DISTINCT drugname_clean, string_agg(DISTINCT lower(r2.str), ',') AS ingredient
              FROM faers.drug_mapping_exact m
                       JOIN faers.rxnconso r ON m.drugname_clean = lower(r.str)
                       JOIN faers.rxnconso r2 ON r.rxcui = r2.rxcui
              WHERE r2.tty IN ('IN', 'PIN', 'MIN')
                AND r2.sab = 'RXNORM'
                AND m.rxcui IS NULL
                AND m.rx_ingredient IS NULL
              GROUP BY drugname_clean
              HAVING count(DISTINCT lower(r2.str)) = 1)
UPDATE faers.drug_mapping_exact m
SET rx_ingredient = cte1.ingredient
FROM cte1
WHERE cte1.drugname_clean = m.drugname_clean
  AND rx_ingredient IS NULL;

-- name: sbd
WITH cte1 AS (SELECT DISTINCT drugname_clean, string_agg(DISTINCT cast(r2.rxcui AS TEXT), ',') AS cui
              FROM faers.drug_mapping_exact m
                       JOIN faers.rxnconso r ON m.drugname_clean = lower(r.str)
                       JOIN faers.rxnconso r2 ON r.rxcui = r2.rxcui
              WHERE r2.tty = 'SBD'
                AND r2.sab = 'RXNORM'
                AND r2.suppress != 'O'
                AND m.rxcui IS NULL
                AND m.rx_ingredient IS NULL
              GROUP BY drugname_clean
              HAVING count(DISTINCT r2.rxcui) = 1)
UPDATE faers.drug_mapping_exact m
SET rxcui = cte1.cui,
    tty   = 'SBD'
FROM cte1
WHERE cte1.drugname_clean = m.drugname_clean
  AND rxcui IS NULL;

-- name: scd
WITH cte1 AS (SELECT DISTINCT drugname_clean, string_agg(DISTINCT cast(r2.rxcui AS TEXT), ',') AS cui
              FROM faers.drug_mapping_exact m
                       JOIN faers.rxnconso r ON m.drugname_clean = lower(r.str)
                       JOIN faers.rxnconso r2 ON r.rxcui = r2.rxcui
              WHERE r2.tty = 'SCD'
                AND r2.sab = 'RXNORM'
                AND r2.suppress != 'O'
                AND m.rxcui IS NULL
                AND m.rx_ingredient IS NULL
              GROUP BY drugname_clean
              HAVING count(DISTINCT r2.rxcui) = 1)
UPDATE faers.drug_mapping_exact m
SET rxcui = cte1.cui,
    tty   = 'SCD'
FROM cte1
WHERE cte1.drugname_clean = m.drugname_clean
  AND rxcui IS NULL;

-- name: from_nda_prepend_zero
WITH cte1 AS (SELECT DISTINCT string_agg(DISTINCT lower(r2.str), ',') AS ingredient,
                              m.drugname_clean,
                              m.nda_num
              FROM faers.drug_mapping_exact m
                       JOIN faers.nda nda ON nda.appl_no = concat('0', m.nda_num)
                       JOIN faers.rxnconso r ON lower(r.str) = concat(lower(replace(nda.ingredient, ';', ' /')))
                       JOIN faers.rxnconso r2 ON r.rxcui = r2.rxcui
              WHERE (m.drugname_clean LIKE lower(concat('%', nda.ingredient, '%'))
                  OR m.prod_ai_clean LIKE lower(concat('%', nda.ingredient, '%'))
                  OR m.drugname_clean LIKE lower(concat('%', nda.trade_name, '%')))
                AND m.rx_ingredient IS NULL
                AND m.rxcui IS NULL
                AND m.nda_num IS NOT NULL
                AND r2.sab = 'RXNORM'
                AND r2.tty IN ('IN', 'MIN', 'PIN')
                AND r2.suppress != 'O'
              GROUP BY m.drugname_clean, m.nda_num
              HAVING count(DISTINCT lower(r2.str)) = 1)
UPDATE faers.drug_mapping_exact m
SET rx_ingredient = cte1.ingredient
FROM cte1
WHERE cte1.drugname_clean = m.drugname_clean
  AND cte1.nda_num = m.nda_num
  AND m.rx_ingredient IS NULL;

-- name: from_nda
WITH cte1 AS (SELECT DISTINCT string_agg(DISTINCT lower(r2.str), ',') AS ingredient,
                              m.drugname_clean,
                              m.nda_num
              FROM faers.drug_mapping_exact m
                       JOIN faers.nda nda ON nda.appl_no = m.nda_num
                       JOIN faers.rxnconso r ON lower(r.str) = concat(lower(replace(nda.ingredient, ';', ' /')))
                       JOIN faers.rxnconso r2 ON r.rxcui = r2.rxcui
              WHERE (m.drugname_clean LIKE lower(concat('%', nda.ingredient, '%'))
                  OR m.prod_ai_clean LIKE lower(concat('%', nda.ingredient, '%'))
                  OR m.drugname_clean LIKE lower(concat('%', nda.trade_name, '%')))
                AND m.rx_ingredient IS NULL
                AND m.rxcui IS NULL
                AND m.nda_num IS NOT NULL
                AND r2.sab = 'RXNORM'
                AND r2.tty IN ('IN', 'MIN', 'PIN')
                AND r2.suppress != 'O'
              GROUP BY m.drugname_clean, m.nda_num
              HAVING count(DISTINCT lower(r2.str)) = 1)
UPDATE faers.drug_mapping_exact m
SET rx_ingredient = cte1.ingredient
FROM cte1
WHERE cte1.drugname_clean = m.drugname_clean
  AND cte1.nda_num = m.nda_num
  AND m.rx_ingredient IS NULL;
