-- name: eu
UPDATE faers.drug_mapping a
SET update_method   = 'regex EU drug name to active ingredient',
    drug_name_clean = lower(b.active_substance)
FROM faers.eu_drug_name_active_ingredient_mapping b
WHERE lower(a.drug_name_clean) = lower(b.brand_name)
  AND a.concept_id is null;

-- name: eu_parentheses
update faers.drug_mapping a
set update_method   = 'regex EU drug name in parentheses to active ingredient',
    drug_name_clean = lower(b.active_substance)
from faers.eu_drug_name_active_ingredient_mapping b
where lower(regexp_replace(a.drug_name_clean, '.* \((.*)\)', '\1', 'gi')) = lower(b.brand_name)
  and a.concept_id is null
  and a.drug_name_clean ~* '.* \((.*)\)';
