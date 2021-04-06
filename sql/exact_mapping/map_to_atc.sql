-- name: set_atc_string_1
UPDATE faers.drug_mapping_exact dme
SET atc_str = lower(replace(str,
                            concat(' [', regexp_replace(str, '.* \[(.*)\]', '\1', 'gi'), ']'),
                            ''))
WHERE atc_str IS NULL
  AND str IS NOT NULL;

-- name: set_atc_string_2
UPDATE faers.drug_mapping_exact dme
SET atc_str = rx_ingredient
WHERE atc_str IS NULL
  AND rx_ingredient IS NOT NULL;

-- name: set_rxcui_for_ing
UPDATE faers.drug_mapping_exact dme
SET rxcui = rx_ingredient
FROM faers.rxnconso rx
WHERE lower(dme.rx_ingredient) = lower(rx.str)
  AND atc_str IS NULL
  AND dme.rxcui IS NULL
  AND rx.sab = 'RXNORM'
  AND rx.tty IN ('IN', 'PIN', 'MIN')
  AND rx_ingredient IS NOT NULL;

-- name: exact_from_patch
UPDATE faers.drug_mapping_exact dme
SET atc_code   = p.atc,
    atc_method = 'patch exact'
FROM faers.rxnorm_atc_patch p
WHERE lower(p.name) = dme.atc_str
  AND dme.atc_code IS NULL;

-- name: infer_from_patch
WITH cte1 AS (SELECT DISTINCT atc_str AS str_final
              FROM faers.drug_mapping_exact
              WHERE atc_code IS NULL
                AND atc_str IS NOT NULL),
     cte2 AS (SELECT string_agg(DISTINCT p.atc, ',') AS atc, dme.str_final
              FROM faers.rxnorm_atc_patch p
                       JOIN cte1 dme ON lower(p.name) LIKE concat(str_final, '%')
              GROUP BY dme.str_final
              HAVING count(DISTINCT p.atc) = 1)
UPDATE faers.drug_mapping_exact dme
SET atc_code   = cte2.atc,
    atc_method = 'patch inferred'
FROM cte2
WHERE cte2.str_final = dme.atc_str
  AND dme.atc_code IS NULL;

-- name: infer_from_patch_single_in
WITH cte1 AS (SELECT DISTINCT atc_str AS str_final
              FROM faers.drug_mapping_exact
              WHERE atc_code IS NULL
                AND atc_str NOT LIKE '%/%'
                AND atc_str IS NOT NULL),
     cte2 AS (SELECT string_agg(DISTINCT p.atc, ',') AS atc, dme.str_final
              FROM faers.rxnorm_atc_patch p
                       JOIN cte1 dme ON lower(p.name) LIKE concat(str_final, '%')
              WHERE "Ingredients" = 1
              GROUP BY dme.str_final
              HAVING count(DISTINCT p.atc) = 1)
UPDATE faers.drug_mapping_exact dme
SET atc_code   = cte2.atc,
    atc_method = 'patch inferred single'
FROM cte2
WHERE cte2.str_final = dme.atc_str
  AND dme.atc_code IS NULL;

-- name: from_rxnconso
WITH cte AS (SELECT rx.rxcui, string_agg(DISTINCT rx.code, ',') AS atc
             FROM faers.standard_combined_drug_mapping e
                      JOIN faers.rxnconso rx ON e.rxcui = rx.rxcui
             WHERE rx.sab = 'ATC'
               AND length(rx.code) = 7
               AND e.atc_code IS NULL
             GROUP BY rx.rxcui
             HAVING count(DISTINCT rx.code) = 1)
UPDATE faers.standard_combined_drug_mapping s
SET atc_code   = cte.atc,
    atc_method = 'rxnconso'
FROM cte
WHERE s.rxcui = cte.rxcui
  AND s.atc_code IS NULL;
