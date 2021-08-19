-- name: exact
WITH cte2 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS TEXT), ',') AS rxcui, m.id
              FROM faers.rxnconso r
                       JOIN
                   faers.drug_mapping_exact_java m
                   ON lower(r.str) =
                      lower(concat(m.rx_ingredient, ' ', m.dose_amt_clean, ' ', m.dose_unit_clean, ' ',
                                   m.rx_dose_form, ' [',
                                   m.rx_brand_name, ']'))
              WHERE m.rx_ingredient IS NOT NULL
                AND m.rx_brand_name IS NOT NULL
                AND m.dose_amt_clean IS NOT NULL
                AND m.dose_unit_clean IS NOT NULL
                AND m.rx_dose_form IS NOT NULL
                AND m.rxcui IS NULL
                AND r.tty = 'SBD'
                AND r.sab = 'RXNORM'
                AND r.suppress != 'O'
              GROUP BY m.id
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte2.rxcui AS INT),
    tty   = 'SBD'
FROM cte2
WHERE cte2.id = m.id;



-- name: drug_clean
WITH cte1 AS (SELECT DISTINCT drugname_clean, string_agg(DISTINCT cast(r2.rxcui AS TEXT), ',') AS cui
              FROM faers.drug_mapping_exact_java m
                       JOIN faers.rxnconso r ON m.drugname_clean = lower(r.str)
                       JOIN faers.rxnconso r2 ON r.rxcui = r2.rxcui
              WHERE r2.tty = 'SBD'
                AND r2.sab = 'RXNORM'
                AND r2.suppress != 'O'
                AND m.rxcui IS NULL
                AND m.rx_ingredient IS NULL
              GROUP BY drugname_clean
              HAVING count(DISTINCT r2.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte1.cui AS INT),
    tty   = 'SBD'
FROM cte1
WHERE cte1.drugname_clean = m.drugname_clean
  AND rxcui IS NULL;



-- name: multi_ing
WITH cte1 AS (SELECT DISTINCT rx_brand_name AS rxbn, rx_ingredient AS ing
              FROM faers.drug_mapping_exact_java
              WHERE rx_brand_name IS NOT NULL
                AND (rx_ingredient IS NOT NULL AND rx_ingredient LIKE '%/%')
                AND rxcui IS NULL),
     cte2 AS (SELECT DISTINCT string_agg(DISTINCT cast(r.rxcui AS VARCHAR), ',') AS rxcui,
                              cte1.rxbn,
                              cte1.ing
              FROM faers.rxnconso r
                       JOIN cte1 ON lower(r.str) LIKE concat(replace(cte1.ing, ' ', '%'), '%', '[', cte1.rxbn, ']')
              WHERE r.sab = 'RXNORM'
                AND r.tty = 'SBD'
                AND r.suppress != 'O'
              GROUP BY cte1.rxbn, cte1.ing
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte2.rxcui AS INT),
    tty   = 'SBD'
FROM cte2
WHERE cte2.ing = m.rx_ingredient
  AND cte2.rxbn = m.rx_brand_name
  AND m.rxcui IS NULL;



-- name: multi_ing_df
WITH cte1 AS (SELECT DISTINCT rx_brand_name AS rxbn, rx_ingredient AS ing, rx_dose_form AS rdf
              FROM faers.drug_mapping_exact_java
              WHERE rx_brand_name IS NOT NULL
                AND rx_dose_form IS NOT NULL
                AND (rx_ingredient IS NOT NULL AND rx_ingredient LIKE '%/%')
                AND rxcui IS NULL),
     cte2 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS VARCHAR), ',') AS rxcui,
                     cte1.rxbn,
                     cte1.ing,
                     cte1.rdf
              FROM faers.rxnconso r
                       JOIN cte1 ON lower(r.str) LIKE
                                    lower(concat(replace(cte1.ing, ' ', '%'), '%', cte1.rdf, '%[', cte1.rxbn, ']'))
              WHERE r.sab = 'RXNORM'
                AND r.tty = 'SBD'
              GROUP BY cte1.rxbn, cte1.ing, cte1.rdf
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte2.rxcui AS INT),
    tty   = 'SBD'
FROM cte2
WHERE cte2.ing = m.rx_ingredient
  AND cte2.rxbn = m.rx_brand_name
  AND cte2.rdf = m.rx_dose_form
  AND m.rxcui IS NULL;



-- name: brand
WITH cte1 AS (SELECT DISTINCT rx_brand_name AS rxbn
              FROM faers.drug_mapping_exact_java
              WHERE rx_brand_name IS NOT NULL
                AND rxcui IS NULL),
     cte2 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS TEXT), ',') AS rxcui,
                     cte1.rxbn
              FROM faers.rxnconso r
                       JOIN
                   cte1
                   ON lower(r.str) LIKE
                      concat('%[', cte1.rxbn, ']')
                       AND r.tty = 'SBD'
                       AND r.sab = 'RXNORM'
                       AND r.suppress != 'O'
              GROUP BY cte1.rxbn
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte2.rxcui AS INT),
    tty   = 'SBD'
FROM cte2
WHERE cte2.rxbn = m.rx_brand_name
  AND m.rxcui IS NULL;



-- name: ing_and_brand
WITH cte1 AS (SELECT DISTINCT rx_brand_name AS rxbn, rx_ingredient AS ing
              FROM faers.drug_mapping_exact_java
              WHERE rx_brand_name IS NOT NULL
                AND (rx_ingredient IS NOT NULL AND rx_ingredient NOT LIKE '%/%')
                AND rxcui IS NULL),
     cte2 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS TEXT), ',') AS rxcui,
                     cte1.ing,
                     cte1.rxbn
              FROM faers.rxnconso r
                       JOIN
                   cte1
                   ON lower(r.str) LIKE lower(concat(cte1.ing, ' %[', cte1.rxbn, ']'))
                       AND r.tty = 'SBD'
                       AND r.sab = 'RXNORM'
                       AND r.suppress != 'O'
              GROUP BY cte1.ing, cte1.rxbn
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte2.rxcui AS INT),
    tty   = 'SBD'
FROM cte2
WHERE cte2.ing = m.rx_ingredient
  AND cte2.rxbn = m.rx_brand_name
  AND m.rxcui IS NULL;



-- name: brand_and_dose_form_strict
WITH cte1 AS (SELECT DISTINCT rx_brand_name AS rxbn, rx_dose_form AS rdf
              FROM faers.drug_mapping_exact_java
              WHERE rx_brand_name IS NOT NULL
                AND rx_dose_form IS NOT NULL
                AND rxcui IS NULL),
     cte2 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS TEXT), ',') AS rxcui,
                     cte1.rxbn,
                     cte1.rdf
              FROM faers.rxnconso r
                       JOIN
                   cte1
                   ON lower(r.str) LIKE
                      concat('%', cte1.rdf, ' [', cte1.rxbn, ']')
                       AND r.tty = 'SBD'
                       AND r.sab = 'RXNORM'
                       AND r.suppress != 'O'
              GROUP BY cte1.rxbn, cte1.rdf
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte2.rxcui AS INT),
    tty   = 'SBD'
FROM cte2
WHERE cte2.rxbn = m.rx_brand_name
  AND cte2.rdf = m.rx_dose_form
  AND m.rxcui IS NULL;



-- name: brand_and_dose_form_loose
WITH cte1 AS (SELECT DISTINCT rx_brand_name AS rxbn, rx_dose_form AS rdf
              FROM faers.drug_mapping_exact_java
              WHERE rx_brand_name IS NOT NULL
                AND rx_dose_form IS NOT NULL
                AND rxcui IS NULL),
     cte2 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS TEXT), ',') AS rxcui,
                     cte1.rxbn,
                     cte1.rdf
              FROM faers.rxnconso r
                       JOIN
                   cte1
                   ON lower(r.str) LIKE
                      concat('%', cte1.rdf, '%[', cte1.rxbn, ']')
                       AND r.tty = 'SBD'
                       AND r.sab = 'RXNORM'
                       AND r.suppress != 'O'
              GROUP BY cte1.rxbn, cte1.rdf
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte2.rxcui AS INT),
    tty   = 'SBD'
FROM cte2
WHERE cte2.rxbn = m.rx_brand_name
  AND cte2.rdf = m.rx_dose_form
  AND m.rxcui IS NULL;



-- name: ing_df_brand
WITH cte1 AS (SELECT rx_ingredient AS ing,
                     rx_brand_name AS rxbn,
                     rx_dose_form  AS rdf
              FROM faers.drug_mapping_exact_java
              WHERE rx_brand_name IS NOT NULL
                AND (rx_ingredient IS NOT NULL AND rx_ingredient NOT LIKE '%/%')
                AND rx_dose_form IS NOT NULL
                AND dose_amt_clean IS NOT NULL
                AND rxcui IS NULL
              GROUP BY rx_ingredient, rx_brand_name, rx_dose_form, dose_amt_clean),
     cte2 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS TEXT), ',') AS rxcui,
                     cte1.ing,
                     cte1.rxbn,
                     cte1.rdf,
                     string_agg(DISTINCT r.str, ',')
              FROM faers.rxnconso r
                       JOIN
                   cte1
                   ON lower(r.str) LIKE
                      lower(concat(cte1.ing, ' %', cte1.rdf, '%[', cte1.rxbn, ']'))
                       AND r.tty = 'SBD'
                       AND r.sab = 'RXNORM'
                       AND r.suppress != 'O'
              GROUP BY cte1.ing, cte1.rxbn, cte1.rdf
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte2.rxcui AS INT),
    tty   = 'SBD'
FROM cte2
WHERE cte2.ing = m.rx_ingredient
  AND cte2.rxbn = m.rx_brand_name
  AND cte2.rdf = m.rx_dose_form
  AND m.rxcui IS NULL;




-- name: ing_brand_dose_form_amount
WITH cte1 AS (SELECT rx_ingredient  AS ing,
                     rx_brand_name  AS rxbn,
                     rx_dose_form   AS rdf,
                     dose_amt_clean AS dac
              FROM faers.drug_mapping_exact_java
              WHERE rx_brand_name IS NOT NULL
                AND (rx_ingredient IS NOT NULL AND rx_ingredient NOT LIKE '%/%')
                AND rx_dose_form IS NOT NULL
                AND dose_amt_clean IS NOT NULL
                AND rxcui IS NULL
              GROUP BY rx_ingredient, rx_brand_name, rx_dose_form, dose_amt_clean),
     cte2 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS TEXT), ',') AS rxcui,
                     cte1.rxbn,
                     cte1.rdf,
                     cte1.ing,
                     cte1.dac
              FROM faers.rxnconso r
                       JOIN
                   cte1
                   ON lower(r.str) LIKE
                      concat(cte1.ing, '% ', cte1.dac, ' %', cte1.rdf, '%[', cte1.rxbn, ']')
                       AND r.tty = 'SBD'
                       AND r.sab = 'RXNORM'
                       AND r.suppress != 'O'
              GROUP BY cte1.rxbn, cte1.rdf, cte1.ing, cte1.dac
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte2.rxcui AS INT),
    tty   = 'SBD'
FROM cte2
WHERE cte2.rxbn = m.rx_brand_name
  AND cte2.rdf = m.rx_dose_form
  AND cte2.ing = m.rx_ingredient
  AND cte2.dac = m.dose_amt_clean
  AND m.rxcui IS NULL;


