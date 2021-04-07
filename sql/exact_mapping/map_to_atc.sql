-- name: populate_str
UPDATE faers.drug_mapping_exact m
SET str = r.str
FROM faers.rxnconso r
WHERE cast(r.rxcui AS TEXT) = m.rxcui
  AND r.tty NOT IN ('TMSY', 'SY')
  AND r.sab = 'RXNORM'
  AND m.str IS NULL;


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

-- name: create_atc_case_drug_current
CREATE TABLE faers.atc_case_drug_c AS (SELECT a.primaryid,
                                              drug_seq,
                                              role_cod,
                                              drm.atc_str    AS rx_str,
                                              drm.atc_code   AS atc_code,
                                              drm.atc_method AS atc_method
                                       FROM faers.drug a
                                                JOIN faers.drug_mapping_exact drm ON
                                               coalesce(upper(a.drugname), 'a') = coalesce(drm.drugname, 'a') AND
                                               coalesce(upper(a.prod_ai), 'a') = coalesce(drm.prod_ai, 'a') AND
                                               coalesce(lower(a.route), 'a') = coalesce(drm.route, 'a') AND
                                               coalesce(lower(a.dose_amt), 'a') = coalesce(drm.dose_amt, 'a') AND
                                               coalesce(lower(a.dose_form), 'a') = coalesce(drm.dose_form, 'a') AND
                                               coalesce(lower(a.dose_unit), 'a') = coalesce(drm.dose_unit, 'a') AND
                                               coalesce(lower(a.nda_num), 'a') = coalesce(drm.nda_num, 'a')
                                       WHERE drm.atc_code IS NOT NULL
                                       GROUP BY a.primaryid, drug_seq, role_cod, drm.atc_str,
                                                drm.atc_code,
                                                drm.atc_method);

-- name: create_atc_case_drug_legacy
CREATE TABLE faers.atc_case_drug_l AS (SELECT a.isr,
                                              drug_seq,
                                              role_cod,
                                              drm.atc_str    AS rx_str,
                                              drm.atc_code   AS atc_code,
                                              drm.atc_method AS atc_method
                                       FROM faers.drug_legacy a
                                                JOIN faers.drug_mapping_exact drm ON
                                               coalesce(upper(a.drugname), 'a') = coalesce(drm.drugname, 'a') AND
                                               coalesce(lower(a.route), 'a') = coalesce(drm.route, 'a') AND
                                               coalesce(cast(a.nda_num AS TEXT), 'a') =
                                               coalesce(drm.nda_num, 'a')
                                       WHERE drm.atc_code IS NOT NULL
                                         AND prod_ai IS NULL
                                         AND dose_amt IS NULL
                                         AND dose_form IS NULL
                                         AND dose_unit IS NULL
                                       GROUP BY a.isr, drug_seq, role_cod, drm.atc_str, drm.rxcui_final,
                                                drm.atc_code,
                                                drm.atc_method);
