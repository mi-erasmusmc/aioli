-- name: remove_amount_when_is_dose_form_count
UPDATE faers.drug_mapping_exact
SET dose_amt_clean  = NULL,
    dose_unit_clean = NULL
where dose_unit = 'df';

-- name: trim_trailing_dot
UPDATE faers.drug_mapping_exact
SET dose_amt_clean = trim(trailing '.' from dose_amt_clean)
WHERE dose_amt_clean LIKE '%.';

-- name: prepend_zero
UPDATE faers.drug_mapping_exact
SET dose_amt_clean = concat('0', dose_amt_clean)
WHERE dose_amt_clean LIKE '.%';

-- name: remove_junk
UPDATE faers.drug_mapping_exact
SET dose_unit_clean = NULL,
    dose_amt_clean  = NULL
WHERE dose_amt_clean like 'unk%'
   OR dose_amt_clean like '%-%'
   OR dose_amt_clean like '%to%'
   OR dose_amt_clean like '%/%'
   OR dose_amt_clean like '%and%'
   OR dose_amt_clean ~ '.*\..*\..*';

-- name: exchange_o_for_0
UPDATE faers.drug_mapping_exact
SET dose_amt_clean = replace(dose_amt_clean, 'o', '0')
WHERE dose_amt_clean like '%o%';

-- name: convert_comma_to_dot
UPDATE faers.drug_mapping_exact
SET dose_amt_clean = replace(dose_amt_clean, ',', '.')
WHERE dose_amt_clean like '%,%';

-- name: remove_non_numeric_chars
UPDATE faers.drug_mapping_exact
SET dose_amt_clean = regexp_replace(dose_amt_clean, '[^0-9.]', '')
WHERE dose_amt_clean ~ '[^0-9.]';

-- name: convert_ug_to_mg
UPDATE faers.drug_mapping_exact
SET dose_amt_clean  = trim(trailing '0' from cast(round((cast(dose_amt_clean as numeric) / 1000), 5) as varchar)),
    dose_unit_clean = 'mg'
WHERE dose_unit_clean = 'ug';