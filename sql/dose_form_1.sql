-- name: remove_df
UPDATE faers.drug_mapping_exact_java
SET dose_unit_clean = NULL,
    dose_amt_clean  = NULL
WHERE lower(dose_unit_clean) = 'df';

-- name: set_temp_dose_form
UPDATE faers.drug_mapping_exact_java
SET temp_dose_form = trim(BOTH ' ' FROM concat(route_clean, ' ', dose_form_clean))
WHERE route_clean IS NOT NULL
   OR dose_form_clean IS NOT NULL;

-- name: deduplicate_and_order_temp_dose_form
UPDATE faers.drug_mapping_exact_java
SET temp_dose_form = trim(BOTH ' ' FROM
                          array_to_string(
                                  array(SELECT DISTINCT unnest(string_to_array(temp_dose_form, ' ')) AS w ORDER BY w),
                                  ' '))
WHERE temp_dose_form IS NOT NULL;

-- name: set_known_rx_dose_forms
WITH cte AS (SELECT lower(array_to_string(array(SELECT DISTINCT unnest(string_to_array(rx.str, ' ')) AS w ORDER BY w),
                                          ' ')) AS orderd,
                    rx.str
             FROM faers.rxnconso rx
             WHERE rx.tty = 'DF'
               AND rx.sab = 'RXNORM'
             GROUP BY lower(array_to_string(array(SELECT DISTINCT unnest(string_to_array(rx.str, ' ')) AS w ORDER BY w),
                                            ' ')), rx.str)
UPDATE faers.drug_mapping_exact_java dme
SET rx_dose_form = lower(cte.str)
FROM cte
WHERE dme.temp_dose_form = cte.orderd
  AND dme.temp_dose_form != 'unknown';

-- name: set_single_word_rx_dose_form
UPDATE faers.drug_mapping_exact_java dme
SET rx_dose_form = temp_dose_form
WHERE temp_dose_form NOT LIKE '% %'
  AND temp_dose_form != 'for'
  AND rx_dose_form IS NULL;

-- name: delete_the_word_for
UPDATE faers.drug_mapping_exact_java
SET temp_dose_form = replace(temp_dose_form, 'for ', '')
WHERE temp_dose_form LIKE 'for %'
   OR temp_dose_form LIKE '% for %';
