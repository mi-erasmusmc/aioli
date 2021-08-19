-- name: exact
WITH cte1 AS (SELECT string_agg(DISTINCT cast(r.rxcui AS TEXT), ',') AS rxcui,
                     m.id
              FROM faers.rxnconso r
                       JOIN
                   faers.drug_mapping_exact_java m
                   ON lower(r.str) =
                      lower(concat(m.rx_ingredient, ' ', m.dose_amt_clean, ' ', m.dose_unit_clean))
              WHERE m.rx_ingredient IS NOT NULL
                AND m.dose_unit_clean IS NOT NULL
                AND m.dose_amt_clean IS NOT NULL
                AND m.rxcui IS NULL
                AND r.tty = 'SCDC'
                AND r.sab = 'RXNORM'
                AND r.suppress != 'O'
              GROUP BY m.id, r.str
              HAVING count(DISTINCT r.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte1.rxcui AS INT),
    tty   = 'SCDC'
FROM cte1
WHERE cte1.id = m.id;

-- name: drug_clean
WITH cte1 AS (SELECT drugname_clean,
                     string_agg(DISTINCT cast(r2.rxcui AS TEXT), ',') AS cui
              FROM faers.drug_mapping_exact_java m
                       JOIN faers.rxnconso r ON lower(m.drugname_clean) = lower(r.str)
                       JOIN faers.rxnconso r2 ON r.rxcui = r2.rxcui
              WHERE r2.tty = 'SCDC'
                AND r2.sab = 'RXNORM'
                AND m.rxcui IS NULL
              GROUP BY drugname_clean
              HAVING count(DISTINCT r2.rxcui) = 1)
UPDATE faers.drug_mapping_exact_java m
SET rxcui = cast(cte1.cui AS INT),
    tty   = 'SCDC'
FROM cte1
WHERE cte1.drugname_clean = m.drugname_clean
  AND rxcui IS NULL;
