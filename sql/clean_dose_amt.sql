-- name: remove_amount_when_is_dose_form_count
UPDATE faers.drug_mapping_exact_java
SET dose_amt_clean  = NULL,
    dose_unit_clean = NULL
WHERE dose_unit = 'df';

-- name: trim_trailing_dot
UPDATE faers.drug_mapping_exact_java
SET dose_amt_clean = trim(TRAILING '.' FROM dose_amt_clean)
WHERE dose_amt_clean LIKE '%.';

-- name: prepend_zero
UPDATE faers.drug_mapping_exact_java
SET dose_amt_clean = concat('0', dose_amt_clean)
WHERE dose_amt_clean LIKE '.%';

-- name: remove_junk
UPDATE faers.drug_mapping_exact_java
SET dose_unit_clean = NULL,
    dose_amt_clean  = NULL
WHERE dose_amt_clean LIKE 'unk%'
   OR dose_amt_clean LIKE '%-%'
   OR dose_amt_clean LIKE '%to%'
   OR dose_amt_clean LIKE '%/%'
   OR dose_amt_clean LIKE '%and%'
   OR dose_amt_clean ~ '.*\..*\..*';

-- name: exchange_o_for_0
UPDATE faers.drug_mapping_exact_java
SET dose_amt_clean = replace(dose_amt_clean, 'o', '0')
WHERE dose_amt_clean LIKE '%o%';

-- name: convert_comma_to_dot
UPDATE faers.drug_mapping_exact_java
SET dose_amt_clean = replace(dose_amt_clean, ',', '.')
WHERE dose_amt_clean LIKE '%,%';

-- name: trim_trailing_dot
UPDATE faers.drug_mapping_exact_java
SET dose_amt_clean = trim(TRAILING '.' FROM dose_amt_clean)
WHERE dose_amt_clean LIKE '%.';

-- name: remove_non_numeric_chars
UPDATE faers.drug_mapping_exact_java
SET dose_amt_clean = regexp_replace(dose_amt_clean, '[^0-9.]', '')
WHERE dose_amt_clean ~ '[^0-9.]';

-- name: remove_junk
UPDATE faers.drug_mapping_exact_java
SET dose_unit_clean = NULL,
    dose_amt_clean  = NULL
WHERE dose_amt_clean LIKE 'unk%'
   OR dose_amt_clean LIKE '%-%'
   OR dose_amt_clean LIKE '%to%'
   OR dose_amt_clean LIKE '%/%'
   OR dose_amt_clean LIKE '%and%'
   OR dose_amt_clean ~ '.*\..*\..*';

-- name: convert_ug_to_mg
UPDATE faers.drug_mapping_exact_java
SET dose_amt_clean  = trim(TRAILING '0' FROM cast(round((cast(dose_amt_clean AS NUMERIC) / 1000), 5) AS VARCHAR)),
    dose_unit_clean = 'mg'
WHERE dose_unit_clean = 'ug';

-- name: trim_trailing_dot
UPDATE faers.drug_mapping_exact_java
SET dose_amt_clean = trim(TRAILING '.' FROM dose_amt_clean)
WHERE dose_amt_clean LIKE '%.';