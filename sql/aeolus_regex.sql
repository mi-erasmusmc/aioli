-- name: remove_tablet
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, '(.*)(\w|^)\(tablets?\)|tablets?(\w|$)', '\1\2', 'gi')
WHERE rxcui IS NULL
  AND drug_name_clean ~* '.*tablet.*';

-- name: remove_capsule
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, '(.*)(\w|^)\(capsules?\)|capsules?(\w|$)', '\1\2', 'gi')
WHERE rxcui IS NULL
  AND drug_name_clean ~* '.*capsule.*';

-- name: remove_mg
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean,
                                     '\(*(\y\d*\.*\d*\ *mg\,*\ *\/*\\*\ *\d*\.*\d*\ *(m2|ml)*\ *\,*\+*\ *\y)\)*', '\3',
                                     'gi')
WHERE rxcui IS NULL
  AND drug_name_clean ~* '\(*(\y\d*\.*\d*\ *mg\,*\ *\/*\\*\ *\d*\.*\d*\ *(m2|ml)*\ *\,*\+*\ *\y)\)*';

-- name: remove_milligrams
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean,
                                     '\(*(\y\d*\.*\d*\ *milligrams?\,*\ *\/*\\*\ *\d*\.*\d*\ *(m2|millimeters?)*\ *\,*\+*\ *\y)\)*',
                                     '\3', 'gi')
WHERE rxcui IS NULL
  AND drug_name_clean ~* '\(*(\y\d*\.*\d*\ *milligrams?\,*\ *\/*\\*\ *\d*\.*\d*\ *(m2|milliliters?)*\ *\,*\+*\ *\y)\)*';

-- name: remove_hcl
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, '(\y\ *(hcl|hydrochloride)\y)', '\3', 'gi')
WHERE rxcui IS NULL
  AND drug_name_clean ~* '\(*(\y\ *(hcl|hydrochloride)\ *\y)\)*';

-- name: remove_nos
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, '\(\y(formulation|generic|nos)\y\)|\y(formulation|generic|nos)\y',
                                     '\3', 'gi')
WHERE rxcui IS NULL
  AND drug_name_clean ~* '\y(formulation|generic|nos)\y';
