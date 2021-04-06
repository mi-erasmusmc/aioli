-- name: remove_df
UPDATE faers.drug_mapping_exact
SET dose_unit_clean = NULL,
    dose_amt_clean  = NULL
WHERE lower(dose_unit_clean) = 'df';

-- name: set_temp_dose_form
UPDATE faers.drug_mapping_exact
SET temp_dose_form = trim(both ' ' from concat(route_clean, ' ', dose_form_clean))
WHERE route_clean IS NOT NULL
   OR dose_form_clean IS NOT NULL;

-- name: deduplicate_and_order_temp_dose_form
UPDATE faers.drug_mapping_exact
SET temp_dose_form = trim(both ' ' from
                          array_to_string(
                                  ARRAY(SELECT DISTINCT UNNEST(string_to_array(temp_dose_form, ' ')) AS w order by w),
                                  ' '))
WHERE temp_dose_form IS NOT NULL;

-- name: set_single_word_rx_dose_form
UPDATE faers.drug_mapping_exact dme
SET rx_dose_form = temp_dose_form
WHERE temp_dose_form NOT LIKE '% %'
  AND temp_dose_form != 'for'
  AND rx_dose_form IS NULL;

-- name: set_known_rx_dose_forms
WITH cte AS (SELECT lower(array_to_string(ARRAY(SELECT DISTINCT UNNEST(string_to_array(rx.str, ' ')) AS w order by w),
                                          ' ')) AS orderd,
                    rx.str
             FROM faers.rxnconso rx
             WHERE rx.tty = 'DF'
               AND rx.sab = 'RXNORM'
             group by lower(array_to_string(ARRAY(SELECT DISTINCT UNNEST(string_to_array(rx.str, ' ')) AS w order by w),
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
SET rx_dose_form = substring(temp_dose_form from '[^ ]+'::text)
WHERE rx_dose_form IS NULL
  AND temp_dose_form LIKE '% %';

-- name: extended_release
update faers.drug_mapping_exact
set rx_dose_form = 'extended release'
where rx_dose_form is null
  and (dose_form like '% er' or dose_form like 'er %' or lower(drugname) like '% er');

-- name: extended_release_tab
update faers.drug_mapping_exact
set rx_dose_form = 'extended release oral tablet'
where (rx_dose_form = 'tablet' or rx_dose_form = 'oral tablet')
  and (dose_form like '% er' or dose_form like 'er %' or lower(drugname) like '% er' or dose_form like '% xr' or
       dose_form like 'xr %' or lower(drugname) like '% xr');

-- name: extended_release_oral
update faers.drug_mapping_exact
set rx_dose_form = 'extended release oral'
where rx_dose_form = 'oral'
  and (dose_form like '% er' or dose_form like 'er %' or lower(drugname) like '% er' or dose_form like '% xr' or
       dose_form like 'xr %' or lower(drugname) like '% xr');

-- name: extended_release_cap
update faers.drug_mapping_exact
set rx_dose_form = 'extended release oral capsule'
where (rx_dose_form = 'capsule' or rx_dose_form = 'oral capsule')
  and (dose_form like '% er' or dose_form like 'er %' or lower(drugname) like '% er' or dose_form like '% xr' or
       dose_form like 'xr %' or lower(drugname) like '% xr');


-- name: df_in_drug
WITH cte1 AS (select lower(str) AS df,
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
update faers.drug_mapping_exact
set rx_dose_form = null
where rx_dose_form = '';