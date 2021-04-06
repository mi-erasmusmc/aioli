-- name: vit_c
UPDATE faers.drug_mapping
SET concept_id = 19011773
WHERE rxcui = '1088438,1151'
  AND concept_id IS NULL;

-- name: vit_d
UPDATE faers.drug_mapping
SET concept_id = 19009405
WHERE lower(drug_name_original) IN ('daily vitamins d');

-- name: vit_b
UPDATE faers.drug_mapping
SET concept_id = 19010970
WHERE lower(drug_name_original) IN ('daily vitamins b');


-- name: lexipro
UPDATE faers.drug_mapping
SET concept_id = 715939
WHERE lower(drug_name_original) IN ('lexipro');

-- name: lexipro
UPDATE faers.drug_mapping
SET concept_id = 45775387,
    rxcui      = 1592713
WHERE lower(drug_name_original) IN ('xigduo xr');


-- name: multivit
UPDATE faers.drug_mapping
SET concept_id = 19135832
WHERE drug_name_clean IN
      ('vitamins', 'vitamin', 'gummy vitamins', 'prenatal vitamins', 'prenatal vitamin', 'vitamins and minerals',
       'herbal preparation')
   OR drug_name_clean LIKE '%multi%vitamin%'
   OR drug_name_clean LIKE '%centrum%silver%';

-- name: nda_nr_rubbish
UPDATE faers.drug_mapping
SET concept_id      = NULL,
    rxcui           = NULL,
    update_method   = NULL,
    drug_name_clean = lower(drug_name_original)
WHERE lower(drug_name_original) IN
      ('antibiotic', 'birth control', 'antibiotics', 'unknown medication', 'proton pump inhibitors', 'antidepressants',
       'probiotic                          /06395501/', 'blinded investigational drug, unspecified', 'anesthetic',
       'unknown pde5 inhibitors', 'no drug name', 'no study drug', 'herbals', 'no treatment received',
       'no drug administered', 'blinded pre-treatment', 'all other therapeutic products', 'chemotherapy',
       'unspecified drug')
   OR (update_method LIKE '%nda%' AND concept_id = 19027008 AND lower(drug_name_original) NOT LIKE '%bupren%')
   OR (update_method LIKE '%nda%' AND concept_id = 40170680)
   OR (update_method LIKE '%nda%' AND concept_id = 1378382)
   OR (update_method LIKE '%nda%' AND concept_id = 1787101)
   OR (update_method LIKE '%nda%' AND concept_id = 19026972)
   OR (update_method LIKE '%nda%' AND concept_id = 46274205)
   OR (update_method LIKE '%nda%' AND concept_id = 44816294)
   OR (update_method LIKE '%nda%' AND concept_id = 42900505)
   OR (update_method LIKE '%nda%' AND concept_id = 43014237 AND lower(drug_name_original) NOT LIKE '%pomal%');






