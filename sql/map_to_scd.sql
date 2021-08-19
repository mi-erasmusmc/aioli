-- name: exact
WITH cte1 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS TEXT), ',') AS rxcui,
                     m.id
              FROM faers.rxnconso r
                       JOIN
                   faers.drug_mapping_exact_java m
                   ON lower(r.str) =
                      lower(concat(m.rx_ingredient, ' ', m.dose_amt_clean, ' ', m.dose_unit_clean, ' ',
                                   m.rx_dose_form))
              WHERE m.rx_ingredient IS NOT NULL
                AND m.dose_amt_clean IS NOT NULL
                AND m.dose_unit_clean IS NOT NULL
                AND m.rx_dose_form IS NOT NULL
                AND m.rxcui IS NULL
                AND r.tty = 'SCD'
                AND r.sab = 'RXNORM'
                AND r.suppress != 'O'
              GROUP BY m.id, r.str
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte1.rxcui AS INT),
    tty   = 'SCD'
FROM cte1
WHERE cte1.id = m.id;


-- name: ingredient
WITH cte1 AS (SELECT rx_ingredient AS ing
              FROM faers.drug_mapping_exact_java
              WHERE rx_ingredient IS NOT NULL
                AND rxcui IS NULL
              GROUP BY rx_ingredient),
     cte2 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS TEXT), ',') AS rxcui,
                     cte1.ing
              FROM faers.rxnconso r
                       JOIN
                   cte1
                   ON lower(r.str) LIKE
                      concat(cte1.ing, '%')
                       AND r.tty = 'SCD'
                       AND r.sab = 'RXNORM'
                       AND r.suppress != 'O'
              GROUP BY cte1.ing
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte2.rxcui AS INT),
    tty   = 'SCD'
FROM cte2
WHERE cte2.ing = m.rx_ingredient
  AND m.rxcui IS NULL;


-- name: ing_and_dose_form
WITH cte1 AS (SELECT rx_ingredient AS ing, rx_dose_form AS rdf
              FROM faers.drug_mapping_exact_java
              WHERE rx_ingredient IS NOT NULL
                AND rx_dose_form IS NOT NULL
                AND rxcui IS NULL
              GROUP BY rx_ingredient, rx_dose_form),
     cte2 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS TEXT), ',') AS rxcui,
                     cte1.ing,
                     cte1.rdf
              FROM faers.rxnconso r
                       JOIN
                   cte1
                   ON lower(r.str) LIKE
                      concat(cte1.ing, '%', cte1.rdf, '%')
                       AND r.tty = 'SCD'
                       AND r.sab = 'RXNORM'
                       AND r.suppress != 'O'
              GROUP BY cte1.ing, cte1.rdf
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte2.rxcui AS INT),
    tty   = 'SCD'
FROM cte2
WHERE cte2.ing = m.rx_ingredient
  AND cte2.rdf = m.rx_dose_form
  AND m.rxcui IS NULL;


-- name: ing_dose_form_amount
WITH cte1 AS (SELECT rx_ingredient  AS ing,
                     rx_dose_form   AS rdf,
                     dose_amt_clean AS dac
              FROM faers.drug_mapping_exact_java
              WHERE rx_ingredient IS NOT NULL
                AND rx_ingredient NOT LIKE '%/%'
                AND rx_dose_form IS NOT NULL
                AND dose_amt_clean IS NOT NULL
                AND rxcui IS NULL
              GROUP BY rx_ingredient, rx_dose_form, dose_amt_clean),
     cte2 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS TEXT), ',') AS rxcui,
                     cte1.rdf,
                     cte1.ing,
                     cte1.dac
              FROM faers.rxnconso r
                       JOIN
                   cte1
                   ON lower(r.str) LIKE
                      concat(cte1.ing, '% ', cte1.dac, ' %', cte1.rdf, '%')
                       AND r.tty = 'SCD'
                       AND r.sab = 'RXNORM'
                       AND r.suppress != 'O'
              GROUP BY cte1.rdf, cte1.ing, cte1.dac
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte2.rxcui AS INT),
    tty   = 'SCD'
FROM cte2
WHERE cte2.rdf = m.rx_dose_form
  AND cte2.ing = m.rx_ingredient
  AND cte2.dac = m.dose_amt_clean
  AND m.rxcui IS NULL;


-- name: multi_ing
WITH cte1 AS (SELECT rx_ingredient AS ing
              FROM faers.drug_mapping_exact_java
              WHERE (rx_ingredient IS NOT NULL AND rx_ingredient LIKE '%/%')
                AND rxcui IS NULL
              GROUP BY rx_ingredient),
     cte2 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS VARCHAR), ',') AS rxcui,
                     cte1.ing
              FROM faers.rxnconso r
                       JOIN cte1 ON lower(r.str) LIKE concat(replace(cte1.ing, ' ', '%'), '%')
              WHERE r.sab = 'RXNORM'
                AND r.tty = 'SCD'
                AND r.suppress != 'O'
              GROUP BY cte1.ing
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte2.rxcui AS INT),
    tty   = 'SCD'
FROM cte2
WHERE cte2.ing = m.rx_ingredient
  AND m.rxcui IS NULL;


-- name: multi_ing_df
WITH cte1 AS (SELECT rx_ingredient AS ing, rx_dose_form AS rdf
              FROM faers.drug_mapping_exact_java
              WHERE rx_dose_form IS NOT NULL
                AND (rx_ingredient IS NOT NULL AND rx_ingredient LIKE '%/%')
                AND rxcui IS NULL
              GROUP BY rx_ingredient, rx_dose_form),
     cte2 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS VARCHAR), ',') AS rxcui,
                     cte1.ing,
                     cte1.rdf
              FROM faers.rxnconso r
                       JOIN cte1 ON lower(r.str) LIKE
                                    lower(concat(replace(cte1.ing, ' ', '%'), '%', cte1.rdf, '%'))
              WHERE r.sab = 'RXNORM'
                AND r.tty = 'SCD'
                AND r.suppress != 'O'
              GROUP BY cte1.ing, cte1.rdf
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte2.rxcui AS INT),
    tty   = 'SCD'
FROM cte2
WHERE cte2.ing = m.rx_ingredient
  AND cte2.rdf = m.rx_dose_form
  AND m.rxcui IS NULL;


-- name: drug_clean
WITH cte1 AS (SELECT drugname_clean, string_agg(DISTINCT cast(r2.rxcui AS TEXT), ',') AS cui
              FROM faers.drug_mapping_exact_java m
                       JOIN faers.rxnconso r ON m.drugname_clean = lower(r.str)
                       JOIN faers.rxnconso r2 ON r.rxcui = r2.rxcui
              WHERE r2.tty = 'SCD'
                AND r2.sab = 'RXNORM'
                AND r2.suppress != 'O'
                AND m.rxcui IS NULL
                AND m.rx_ingredient IS NULL
              GROUP BY drugname_clean
              HAVING count(DISTINCT r2.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte1.cui AS INT),
    tty   = 'SCD'
FROM cte1
WHERE cte1.drugname_clean = m.drugname_clean
  AND rxcui IS NULL;


-- name: drug_clean_extra
WITH cte1 AS (SELECT drugname_clean,
                     rx_dose_form,
                     string_agg(DISTINCT cast(r3.rxcui AS TEXT), ',') AS cui
              FROM faers.drug_mapping_exact_java m
                       JOIN faers.rxnconso r ON m.drugname_clean = lower(r.str)
                       JOIN faers.rxnconso r2 ON r.rxcui = r2.rxcui
                       JOIN faers.rxnconso r3 ON lower(r3.str) LIKE lower(concat(r2.str, '%', rx_dose_form, '%'))
              WHERE r2.tty = 'SCDC'
                AND r2.sab = 'RXNORM'
                AND r3.tty = 'SCD'
                AND r3.str NOT LIKE '% / %'
                AND r3.suppress != 'O'
                AND r3.sab = 'RXNORM'
                AND m.rxcui IS NULL
              GROUP BY drugname_clean, rx_dose_form
              HAVING count(DISTINCT r3.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte1.cui AS INT),
    tty   = 'SCD'
FROM cte1
WHERE cte1.drugname_clean = m.drugname_clean
  AND cte1.rx_dose_form = m.rx_dose_form
  AND rxcui IS NULL;
