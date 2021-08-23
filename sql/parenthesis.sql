-- name: brand_in_parenthesis
UPDATE faers.drug_mapping_exact_java dme
SET rx_brand_name  = lower(rx.str),
    drugname_clean = replace(drugname_clean,
                             concat(' (', regexp_replace(dme.drugname_clean, '.* \((.*)\)', '\1', 'gi'), ')'),
                             '')
FROM rxnorm.rxnconso rx
WHERE lower(rx.str) = regexp_replace(dme.drugname_clean, '.* \((.*)\)', '\1', 'gi')
  AND rx.sab = 'RXNORM'
  AND rx.tty = 'BN'
  AND dme.rx_brand_name IS NULL
  AND dme.drugname_clean ~* '.* \((.*)\)';

-- name: dose_form_in_parenthesis
UPDATE faers.drug_mapping_exact_java dme
SET rx_dose_form   = lower(rx.str),
    drugname_clean = replace(drugname_clean,
                             concat(' (', regexp_replace(dme.drugname_clean, '.* \((.*)\)', '\1', 'gi'), ')'),
                             '')
FROM rxnorm.rxnconso rx
WHERE lower(rx.str) = regexp_replace(dme.drugname_clean, '.* \((.*)\)', '\1', 'gi')
  AND rx.sab = 'RXNORM'
  AND rx.tty = 'DF'
  AND dme.rx_dose_form IS NULL
  AND dme.drugname_clean ~* '.* \((.*)\)';


-- name: ingredient_in_parenthesis
UPDATE faers.drug_mapping_exact_java dme
SET rx_ingredient  = lower(rx.str),
    drugname_clean = replace(drugname_clean,
                             concat(' (', regexp_replace(dme.drugname_clean, '.* \((.*)\)', '\1', 'gi'), ')'),
                             '')
FROM rxnorm.rxnconso rx
WHERE lower(rx.str) = regexp_replace(dme.drugname_clean, '.* \((.*)\)', '\1', 'gi')
  AND rx.sab = 'RXNORM'
  AND rx.tty IN ('IN', 'MIN', 'PIN')
  AND dme.rx_ingredient IS NULL
  AND dme.drugname_clean ~* '.* \((.*)\)';

-- name: ingredient_outside_parenthesis
UPDATE faers.drug_mapping_exact_java dme
SET rx_ingredient = lower(rx.str)
FROM rxnorm.rxnconso rx
WHERE lower(rx.str) =
      replace(drugname_clean, concat(' (', regexp_replace(dme.drugname_clean, '.* \((.*)\)', '\1', 'gi'), ')'),
              '')
  AND rx.sab = 'RXNORM'
  AND rx.tty IN ('IN', 'MIN', 'PIN')
  AND dme.rx_ingredient IS NULL
  AND dme.drugname_clean ~* '.* \((.*)\)';


-- name: brand_outside_parenthesis
UPDATE faers.drug_mapping_exact_java dme
SET rx_brand_name = lower(rx.str)
FROM rxnorm.rxnconso rx
WHERE lower(rx.str) =
      replace(drugname_clean, concat(' (', regexp_replace(dme.drugname_clean, '.* \((.*)\)', '\1', 'gi'), ')'),
              '')
  AND rx.sab = 'RXNORM'
  AND rx.tty = 'BN'
  AND dme.rx_brand_name IS NULL
  AND dme.drugname_clean ~* '.* \((.*)\)';


-- name: brand_in_any_parenthesis
UPDATE faers.drug_mapping_exact_java dme
SET rx_brand_name  = lower(rx.str),
    drugname_clean = replace(drugname_clean,
                             concat(' (', regexp_replace(dme.drugname_clean, '.*\((.*)\).*', '\1', 'gi'), ')'),
                             '')
FROM rxnorm.rxnconso rx
WHERE lower(rx.str) = regexp_replace(dme.drugname_clean, '.*\((.*)\).*', '\1', 'gi')
  AND rx.sab = 'RXNORM'
  AND rx.tty = 'BN'
  AND dme.rx_brand_name IS NULL
  AND dme.drugname_clean ~* '.* \((.*)\)';

-- name: dose_form_in_any_parenthesis
UPDATE faers.drug_mapping_exact_java dme
SET rx_dose_form = lower(rx.str)
FROM rxnorm.rxnconso rx
WHERE lower(rx.str) = regexp_replace(dme.drugname_clean, '.*\((.*)\).*', '\1', 'gi')
  AND rx.sab = 'RXNORM'
  AND rx.tty = 'DF'
  AND dme.rx_dose_form IS NULL
  AND dme.drugname_clean ~* '.*\((.*)\).*';


-- name: ingredient_in_any_parenthesis
UPDATE faers.drug_mapping_exact_java dme
SET rx_ingredient = lower(rx.str)
FROM rxnorm.rxnconso rx
WHERE lower(rx.str) = regexp_replace(dme.drugname_clean, '.*\((.*)\).*', '\1', 'gi')
  AND rx.sab = 'RXNORM'
  AND rx.tty IN ('IN', 'MIN', 'PIN')
  AND dme.rx_ingredient IS NULL
  AND dme.drugname_clean ~* '.*\((.*)\).*';
