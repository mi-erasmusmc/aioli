
-- name: set_rx_dose_from_first_term
UPDATE faers.drug_mapping_exact_java
SET rx_dose_form = substring(temp_dose_form FROM '[^ ]+'::TEXT)
WHERE rx_dose_form IS NULL
  AND temp_dose_form LIKE '% %';

-- name: extended_release
UPDATE faers.drug_mapping_exact_java
SET rx_dose_form = 'extended release'
WHERE rx_dose_form IS NULL
  AND (dose_form LIKE '% er' OR dose_form LIKE 'er %' OR lower(drugname) LIKE '% er');

-- name: extended_release_tab
UPDATE faers.drug_mapping_exact_java
SET rx_dose_form = 'extended release oral tablet'
WHERE (rx_dose_form = 'tablet' OR rx_dose_form = 'oral tablet')
  AND (dose_form LIKE '% er' OR dose_form LIKE 'er %' OR lower(drugname) LIKE '% er' OR dose_form LIKE '% xr' OR
       dose_form LIKE 'xr %' OR lower(drugname) LIKE '% xr');

-- name: extended_release_oral
UPDATE faers.drug_mapping_exact_java
SET rx_dose_form = 'extended release oral'
WHERE rx_dose_form = 'oral'
  AND (dose_form LIKE '% er' OR dose_form LIKE 'er %' OR lower(drugname) LIKE '% er' OR dose_form LIKE '% xr' OR
       dose_form LIKE 'xr %' OR lower(drugname) LIKE '% xr');

-- name: extended_release_cap
UPDATE faers.drug_mapping_exact_java
SET rx_dose_form = 'extended release oral capsule'
WHERE (rx_dose_form = 'capsule' OR rx_dose_form = 'oral capsule')
  AND (dose_form LIKE '% er' OR dose_form LIKE 'er %' OR lower(drugname) LIKE '% er' OR dose_form LIKE '% xr' OR
       dose_form LIKE 'xr %' OR lower(drugname) LIKE '% xr');

-- name: clean_empty
UPDATE faers.drug_mapping_exact_java
SET rx_dose_form = NULL
WHERE rx_dose_form = '';