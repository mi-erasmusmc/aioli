UPDATE faers.drug_mapping_exact_java
SET dose_form_clean = concat(' ', regexp_replace(dose_form, '[\\(\\)\\,\\.]', '', 'gi'), ' ');

UPDATE faers.drug_mapping_exact_java
SET route_clean = concat(' ', regexp_replace(route, '[\\(\\)\\,\\.]', '', 'gi'), ' ');