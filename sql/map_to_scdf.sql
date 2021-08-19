-- name: exact
WITH cte1 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS TEXT), ',') AS rxcui,
                     m.id
              FROM rxnorm.rxnconso r
                       JOIN
                   faers.drug_mapping_exact_java m
                   ON lower(r.str) =
                      lower(concat(m.rx_ingredient, ' ', m.rx_dose_form))
              WHERE m.rx_ingredient IS NOT NULL
                AND m.rx_dose_form IS NOT NULL
                AND m.rxcui IS NULL
                AND r.tty = 'SCDF'
                AND r.sab = 'RXNORM'
                AND r.suppress != 'O'
              GROUP BY m.id, r.str
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte1.rxcui AS INT),
    tty   = 'SCDF'
FROM cte1
WHERE cte1.id = m.id;


-- name: loose
WITH cte1 AS (SELECT DISTINCT rx_ingredient AS ing, rx_dose_form AS rxdf
              FROM faers.drug_mapping_exact_java
              WHERE rx_ingredient IS NOT NULL
                AND rx_dose_form IS NOT NULL
                AND rxcui IS NULL),
     cte2 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS TEXT), ',') AS rxcui,
                     cte1.ing,
                     cte1.rxdf
              FROM rxnorm.rxnconso r
                       JOIN
                   cte1
                   ON lower(r.str) LIKE
                      concat(cte1.ing, '%', cte1.rxdf, '%')
                       AND r.tty = 'SCD'
                       AND r.sab = 'RXNORM'
                       AND r.suppress != 'O'
              GROUP BY cte1.ing, cte1.rxdf
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte2.rxcui AS INT),
    tty   = 'SCD'
FROM cte2
WHERE cte2.ing = m.rx_ingredient
  AND cte2.rxdf = m.rx_dose_form
  AND m.rxcui IS NULL;

-- name: drug_clean
WITH cte1 AS (SELECT drugname_clean, string_agg(DISTINCT cast(r2.rxcui AS TEXT), ',') AS cui
              FROM faers.drug_mapping_exact_java m
                       JOIN rxnorm.rxnconso r ON m.drugname_clean = lower(r.str)
                       JOIN rxnorm.rxnconso r2 ON r.rxcui = r2.rxcui
              WHERE r2.tty = 'SCDF'
                AND r2.sab = 'RXNORM'
                AND r2.suppress != 'O'
                AND m.rxcui IS NULL
                AND m.rx_ingredient IS NULL
              GROUP BY drugname_clean
              HAVING count(DISTINCT r2.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte1.cui AS INT),
    tty   = 'SCDF'
FROM cte1
WHERE cte1.drugname_clean = m.drugname_clean
  AND rxcui IS NULL;
