WITH cte AS (SELECT DISTINCT dme.drugname_clean, lower(mr.str) AS str, out.match, dme.occurrences
             FROM faers.mrconso mr
                      JOIN faers.output out ON out.cui = mr.cui
                      JOIN faers.drug_mapping_exact dme
                           ON dme.drugname_clean = lower(replace(out.drugname_clean, ' w ', ' / '))
             WHERE mr.sab = 'RXNORM'
               AND mr.c13 IN ('MIN', 'IN', 'PIN')
               AND out.match > 0.59
               AND mr.str LIKE '%]'
               AND dme.rx_ingredient IS NULL)
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = cte.str
FROM cte
WHERE cte.drugname_clean = dme.drugname_clean
  AND dme.rx_ingredient IS NULL;


WITH cte AS (SELECT DISTINCT dme.drugname_clean,
                             lower(mr.str)                   AS string,
                             dme.rx_dose_form,
                             (CASE
                                  WHEN lower(mr.str) LIKE '%disintegrating oral tablet'
                                      THEN 'disintegrating oral tablet'
                                  WHEN lower(mr.str) LIKE '%extended release oral tablet'
                                      THEN 'extended release oral tablet'
                                  WHEN lower(mr.str) LIKE '%delayed release oral capsule'
                                      THEN 'delayed release oral capsule'
                                  ELSE dme.rx_dose_form END) AS dose_form
             FROM faers.mrconso mr
                      JOIN faers.output out ON out.cui = mr.cui
                      JOIN faers.drug_mapping_exact dme
                           ON dme.drugname_clean = replace(out.drugname_clean, ' w ', ' / ')
             WHERE mr.sab = 'RXNORM'
               AND out.match > 0.59
               AND dme.rx_ingredient IS NULL
               AND mr.c13 = 'SCDF'
               AND lower(mr.str) LIKE concat('% ', dme.rx_dose_form)
               AND dme.rx_dose_form LIKE '% %')
UPDATE faers.drug_mapping_exact dme
SET rx_dose_form  = cte.dose_form,
    rx_ingredient = replace(cte.string, concat(' ', cte.dose_form), '')
FROM cte
WHERE cte.drugname_clean = dme.drugname_clean;

-- name: exactly_one_space_in_scdf
WITH cte AS (SELECT DISTINCT dme.drugname_clean,
                             lower(mr.str)                             AS string,
                             lower(regexp_replace(mr.str, '^.* ', '')) AS dose_form
             FROM faers.mrconso mr
                      JOIN faers.output out ON out.cui = mr.cui
                      JOIN faers.drug_mapping_exact dme
                           ON dme.drugname_clean = replace(out.drugname_clean, ' w ', ' / ')
             WHERE mr.sab = 'RXNORM'
               AND out.match > 0.59
               AND dme.rx_ingredient IS NULL
               AND mr.str ~* '^[^\s]+\s[^\s]+$'
               AND mr.c13 = 'SCDF')
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = replace(cte.string, concat(' ', cte.dose_form), ''),
    rx_dose_form  = cte.dose_form
FROM cte
WHERE cte.drugname_clean = dme.drugname_clean;


WITH cte AS (SELECT DISTINCT dme.drugname_clean,
                             lower(mr.str) AS string,
                             dme.occurrences,
                             (CASE
                                  WHEN lower(mr.str) LIKE '%disintegrating oral tablet'
                                      THEN 'disintegrating oral tablet'
                                  WHEN lower(mr.str) LIKE '%extended release oral tablet'
                                      THEN 'extended release oral tablet'
                                  WHEN lower(mr.str) LIKE '%delayed release oral capsule'
                                      THEN 'delayed release oral capsule'
                                  WHEN lower(mr.str) LIKE '%transdermal system'
                                      THEN 'transdermal system'
                                  WHEN lower(mr.str) LIKE '%drug implant'
                                      THEN 'drug implant'
                                  WHEN lower(mr.str) LIKE '%inhalation powder'
                                      THEN 'inhalation powder'
                                  WHEN lower(mr.str) LIKE '%oral capsule'
                                      THEN 'oral capsule'
                                  WHEN lower(mr.str) LIKE '%oral tablet'
                                      THEN 'oral tablet'
                                  WHEN lower(mr.str) LIKE '%topical cream'
                                      THEN 'topical cream'
                                  WHEN lower(mr.str) LIKE '%prefilled syringe'
                                      THEN 'prefilled syringe'
                                  WHEN lower(mr.str) LIKE '%ophthalmic ointment'
                                      THEN 'ophthalmic ointment'
                                  WHEN lower(mr.str) LIKE '%topical gel'
                                      THEN 'topical gel'
                                  WHEN lower(mr.str) LIKE '%oral lozenge'
                                      THEN 'oral lozenge'
                                  WHEN lower(mr.str) LIKE '%metered dose inhaler'
                                      THEN 'metered dose inhaler'
                                  WHEN lower(mr.str) LIKE '%topical ointment'
                                      THEN 'topical ointment'
                                  WHEN lower(mr.str) LIKE '%rectal suppository'
                                      THEN 'rectal suppository'
                                  WHEN lower(mr.str) LIKE '%oral solution'
                                      THEN 'oral solution'
                                  WHEN lower(mr.str) LIKE '%topical lotion'
                                      THEN 'topical lotion'
                                  WHEN lower(mr.str) LIKE '%vaginal cream'
                                      THEN 'vaginal cream'
                                  WHEN lower(mr.str) LIKE '%injection'
                                      THEN 'injection'
                                  WHEN lower(mr.str) LIKE '%mouthwash'
                                      THEN 'mouthwash'
                                  WHEN lower(mr.str) LIKE '%topical oil'
                                      THEN 'topical oil'
                                  WHEN lower(mr.str) LIKE '%topical foam'
                                      THEN 'topical foam'
                                  WHEN lower(mr.str) LIKE '%ophthalmic solution'
                                      THEN 'ophthalmic solution'
                                  WHEN lower(mr.str) LIKE '%metered dose nasal spray'
                                      THEN 'metered dose nasal spray'
                                  WHEN lower(mr.str) LIKE '%oral suspension'
                                      THEN 'oral suspension'
                                  WHEN lower(mr.str) LIKE '%chewable tablet'
                                      THEN 'chewable tablet'
                                  WHEN lower(mr.str) LIKE '%irrigation solution'
                                      THEN 'irrigation solution'
                                  WHEN lower(mr.str) LIKE '%oral gel'
                                      THEN 'oral gel'
                                  WHEN lower(mr.str) LIKE '%oral paste'
                                      THEN 'oral paste'
                                  WHEN lower(mr.str) LIKE '%nasal spray'
                                      THEN 'nasal spray'
                                  WHEN lower(mr.str) LIKE '%ophthalmic suspension'
                                      THEN 'ophthalmic suspension'
                                 END)      AS dose_form
             FROM faers.mrconso mr
                      JOIN faers.output out ON out.cui = mr.cui
                      JOIN faers.drug_mapping_exact dme
                           ON dme.drugname_clean = replace(out.drugname_clean, ' w ', ' / ')
             WHERE mr.sab = 'RXNORM'
               AND out.match > 0.59
               AND dme.rx_ingredient IS NULL
               AND mr.c13 = 'SCDF')
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = replace(cte.string, concat(' ', cte.dose_form), ''),
    rx_dose_form  = cte.dose_form
FROM cte
WHERE cte.drugname_clean = dme.drugname_clean;


-- and mr.c13 not in ('SY', 'TMSY')


WITH cte AS (SELECT DISTINCT dme.drugname_clean,
                             lower(mr.str)                             AS string,
                             lower(regexp_replace(mr.str, '^.* ', '')) AS unit,
                             substring(mr.str, '[0-9]+[.]?[0-9]*')     AS amount
             FROM faers.mrconso mr
                      JOIN faers.output out ON out.cui = mr.cui
                      JOIN faers.drug_mapping_exact dme
                           ON dme.drugname_clean = replace(out.drugname_clean, ' w ', ' / ')
             WHERE mr.sab = 'RXNORM'
               AND mr.c13 = 'SCDC'
               AND dme.rx_ingredient IS NULL)
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient   = replace(cte.string, concat(' ', cte.amount, ' ', cte.unit), ''),
    dose_unit_clean = cte.unit,
    dose_amt_clean  = cte.amount
FROM cte
WHERE cte.drugname_clean = dme.drugname_clean;

WITH cte AS (SELECT DISTINCT dme.drugname_clean,
                             lower(mr.str) AS string,
                             mr.c13,
                             out.match,
                             mr.c14
             FROM faers.mrconso mr
                      JOIN faers.output out ON out.cui = mr.cui
                      JOIN faers.drug_mapping_exact dme
                           ON dme.drugname_clean = replace(out.drugname_clean, ' w ', ' / ')
             WHERE mr.sab = 'RXNORM'
               AND mr.c13 = 'SCD'
               AND dme.drugname_clean ~ '.*[0-9].*'
               AND dme.rxcui IS NULL)
UPDATE faers.drug_mapping_exact dme
SET str   = cte.string,
    tty   = cte.c13,
    rxcui = cte.c14
FROM cte
WHERE cte.drugname_clean = dme.drugname_clean;
