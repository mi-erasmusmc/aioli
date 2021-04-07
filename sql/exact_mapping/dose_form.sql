-- name: remove_df
UPDATE faers.drug_mapping_exact
SET dose_unit_clean = NULL,
    dose_amt_clean  = NULL
WHERE lower(dose_unit_clean) = 'df';

-- name: set_temp_dose_form
UPDATE faers.drug_mapping_exact
SET temp_dose_form = trim(BOTH ' ' FROM concat(route_clean, ' ', dose_form_clean))
WHERE route_clean IS NOT NULL
   OR dose_form_clean IS NOT NULL;

-- name: deduplicate_and_order_temp_dose_form
UPDATE faers.drug_mapping_exact
SET temp_dose_form = trim(BOTH ' ' FROM
                          array_to_string(
                                  array(SELECT DISTINCT unnest(string_to_array(temp_dose_form, ' ')) AS w ORDER BY w),
                                  ' '))
WHERE temp_dose_form IS NOT NULL;

-- name: set_single_word_rx_dose_form
UPDATE faers.drug_mapping_exact dme
SET rx_dose_form = temp_dose_form
WHERE temp_dose_form NOT LIKE '% %'
  AND temp_dose_form != 'for'
  AND rx_dose_form IS NULL;

-- name: set_known_rx_dose_forms
WITH cte AS (SELECT lower(array_to_string(array(SELECT DISTINCT unnest(string_to_array(rx.str, ' ')) AS w ORDER BY w),
                                          ' ')) AS orderd,
                    rx.str
             FROM faers.rxnconso rx
             WHERE rx.tty = 'DF'
               AND rx.sab = 'RXNORM'
             GROUP BY lower(array_to_string(array(SELECT DISTINCT unnest(string_to_array(rx.str, ' ')) AS w ORDER BY w),
                                            ' ')), rx.str)
UPDATE faers.drug_mapping_exact dme
SET rx_dose_form = lower(cte.str)
FROM cte
WHERE dme.temp_dose_form = cte.orderd
  AND dme.temp_dose_form != 'unknown';

-- name: delete_the_word_for
UPDATE faers.drug_mapping_exact
SET temp_dose_form = replace(temp_dose_form, 'for ', '')
WHERE temp_dose_form LIKE 'for %'
   OR temp_dose_form LIKE '% for %';

-- name: set_rx_dose_from_first_term
UPDATE faers.drug_mapping_exact
SET rx_dose_form = substring(temp_dose_form FROM '[^ ]+'::TEXT)
WHERE rx_dose_form IS NULL
  AND temp_dose_form LIKE '% %';

-- name: extended_release
UPDATE faers.drug_mapping_exact
SET rx_dose_form = 'extended release'
WHERE rx_dose_form IS NULL
  AND (dose_form LIKE '% er' OR dose_form LIKE 'er %' OR lower(drugname) LIKE '% er');

-- name: extended_release_tab
UPDATE faers.drug_mapping_exact
SET rx_dose_form = 'extended release oral tablet'
WHERE (rx_dose_form = 'tablet' OR rx_dose_form = 'oral tablet')
  AND (dose_form LIKE '% er' OR dose_form LIKE 'er %' OR lower(drugname) LIKE '% er' OR dose_form LIKE '% xr' OR
       dose_form LIKE 'xr %' OR lower(drugname) LIKE '% xr');

-- name: extended_release_oral
UPDATE faers.drug_mapping_exact
SET rx_dose_form = 'extended release oral'
WHERE rx_dose_form = 'oral'
  AND (dose_form LIKE '% er' OR dose_form LIKE 'er %' OR lower(drugname) LIKE '% er' OR dose_form LIKE '% xr' OR
       dose_form LIKE 'xr %' OR lower(drugname) LIKE '% xr');

-- name: extended_release_cap
UPDATE faers.drug_mapping_exact
SET rx_dose_form = 'extended release oral capsule'
WHERE (rx_dose_form = 'capsule' OR rx_dose_form = 'oral capsule')
  AND (dose_form LIKE '% er' OR dose_form LIKE 'er %' OR lower(drugname) LIKE '% er' OR dose_form LIKE '% xr' OR
       dose_form LIKE 'xr %' OR lower(drugname) LIKE '% xr');


-- name: df_in_drug
WITH cte1 AS (SELECT lower(str) AS df,
                     array_length(string_to_array(lower(str), ' '), 1)
                                AS word
              FROM faers.rxnconso rx
              WHERE rx.tty = 'DF'
                AND rx.sab = 'RXNORM'
                AND length(str) > 3
              GROUP BY lower(str))
UPDATE faers.drug_mapping_exact dme
SET rx_dose_form = cte1.df
FROM cte1
WHERE dme.drugname_clean LIKE concat('%', cte1.df, '%')
  AND dme.rx_dose_form IS NULL
  AND cast(cte1.word AS INT) = $1;

-- name: clean_empty
UPDATE faers.drug_mapping_exact
SET rx_dose_form = NULL
WHERE rx_dose_form = '';