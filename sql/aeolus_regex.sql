-- name: remove_tablet
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '(.*)(\w|^)\(tablets?\)|tablets?(\w|$)', '\1\2', 'gi')
where rxcui is null
  and drug_name_clean ~* '.*tablet.*';

-- name: remove_capsule
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '(.*)(\w|^)\(capsules?\)|capsules?(\w|$)', '\1\2', 'gi')
where rxcui is null
  and drug_name_clean ~* '.*capsule.*';

-- name: remove_mg
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean,
                                     '\(*(\y\d*\.*\d*\ *mg\,*\ *\/*\\*\ *\d*\.*\d*\ *(m2|ml)*\ *\,*\+*\ *\y)\)*', '\3',
                                     'gi')
where rxcui is null
  and drug_name_clean ~* '\(*(\y\d*\.*\d*\ *mg\,*\ *\/*\\*\ *\d*\.*\d*\ *(m2|ml)*\ *\,*\+*\ *\y)\)*';

-- name: remove_milligrams
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean,
                                     '\(*(\y\d*\.*\d*\ *milligrams?\,*\ *\/*\\*\ *\d*\.*\d*\ *(m2|millimeters?)*\ *\,*\+*\ *\y)\)*',
                                     '\3', 'gi')
where rxcui is null
  and drug_name_clean ~* '\(*(\y\d*\.*\d*\ *milligrams?\,*\ *\/*\\*\ *\d*\.*\d*\ *(m2|milliliters?)*\ *\,*\+*\ *\y)\)*';

-- name: remove_hcl
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '(\y\ *(hcl|hydrochloride)\y)', '\3', 'gi')
where rxcui is null
  and drug_name_clean ~* '\(*(\y\ *(hcl|hydrochloride)\ *\y)\)*';

-- name: remove_nos
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '\(\y(formulation|generic|nos)\y\)|\y(formulation|generic|nos)\y',
                                     '\3', 'gi')
where rxcui is null
  and drug_name_clean ~* '\y(formulation|generic|nos)\y';
