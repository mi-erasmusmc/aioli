with cte as (select distinct dme.drugname_clean, LOWER(mr.str) AS str, out.match, dme.occurrences
             from faers.mrconso mr
                      join faers.output out on out.cui = mr.cui
                      join faers.drug_mapping_exact dme
                           on dme.drugname_clean = LOWER(replace(out.drugname_clean, ' w ', ' / '))
             where mr.sab = 'RXNORM'
               and mr.c13 in ('MIN', 'IN', 'PIN')
               and out.match > 0.59
               and mr.str like '%]'
               and dme.rx_ingredient is null)
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = cte.str
FROM cte
WHERE cte.drugname_clean = dme.drugname_clean
  and dme.rx_ingredient is null;


with cte as (select distinct dme.drugname_clean,
                             lower(mr.str)                   as string,
                             dme.rx_dose_form,
                             (CASE
                                  WHEN lower(mr.str) LIKE '%disintegrating oral tablet'
                                      THEN 'disintegrating oral tablet'
                                  WHEN lower(mr.str) LIKE '%extended release oral tablet'
                                      then 'extended release oral tablet'
                                  WHEN lower(mr.str) LIKE '%delayed release oral capsule'
                                      then 'delayed release oral capsule'
                                  ELSE dme.rx_dose_form END) AS dose_form
             from faers.mrconso mr
                      join faers.output out on out.cui = mr.cui
                      join faers.drug_mapping_exact dme
                           on dme.drugname_clean = replace(out.drugname_clean, ' w ', ' / ')
             where mr.sab = 'RXNORM'
               and out.match > 0.59
               and dme.rx_ingredient is null
               and mr.c13 = 'SCDF'
               and lower(mr.str) like concat('% ', dme.rx_dose_form)
               and dme.rx_dose_form like '% %')
UPDATE faers.drug_mapping_exact dme
SET rx_dose_form  = cte.dose_form,
    rx_ingredient = replace(cte.string, concat(' ', cte.dose_form), '')
FROM cte
WHERE cte.drugname_clean = dme.drugname_clean;

-- name: exactly_one_space_in_scdf
with cte as (select distinct dme.drugname_clean,
                             lower(mr.str)                             as string,
                             lower(regexp_replace(mr.str, '^.* ', '')) AS dose_form
             from faers.mrconso mr
                      join faers.output out on out.cui = mr.cui
                      join faers.drug_mapping_exact dme
                           on dme.drugname_clean = replace(out.drugname_clean, ' w ', ' / ')
             where mr.sab = 'RXNORM'
               and out.match > 0.59
               and dme.rx_ingredient is null
               and mr.str ~* '^[^\s]+\s[^\s]+$'
               and mr.c13 = 'SCDF')
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = replace(cte.string, concat(' ', cte.dose_form), ''),
    rx_dose_form  = cte.dose_form
from cte
where cte.drugname_clean = dme.drugname_clean;


with cte as (select distinct dme.drugname_clean,
                             lower(mr.str) as string,
                             dme.occurrences,
                             (CASE
                                  WHEN lower(mr.str) LIKE '%disintegrating oral tablet'
                                      THEN 'disintegrating oral tablet'
                                  WHEN lower(mr.str) LIKE '%extended release oral tablet'
                                      then 'extended release oral tablet'
                                  WHEN lower(mr.str) LIKE '%delayed release oral capsule'
                                      then 'delayed release oral capsule'
                                  WHEN lower(mr.str) LIKE '%transdermal system'
                                      then 'transdermal system'
                                  WHEN lower(mr.str) LIKE '%drug implant'
                                      then 'drug implant'
                                  WHEN lower(mr.str) LIKE '%inhalation powder'
                                      then 'inhalation powder'
                                  WHEN lower(mr.str) LIKE '%oral capsule'
                                      then 'oral capsule'
                                  WHEN lower(mr.str) LIKE '%oral tablet'
                                      THEN 'oral tablet'
                                  WHEN lower(mr.str) LIKE '%topical cream'
                                      then 'topical cream'
                                  WHEN lower(mr.str) LIKE '%prefilled syringe'
                                      then 'prefilled syringe'
                                  WHEN lower(mr.str) LIKE '%ophthalmic ointment'
                                      then 'ophthalmic ointment'
                                  WHEN lower(mr.str) LIKE '%topical gel'
                                      then 'topical gel'
                                  WHEN lower(mr.str) LIKE '%oral lozenge'
                                      then 'oral lozenge'
                                  WHEN lower(mr.str) LIKE '%metered dose inhaler'
                                      then 'metered dose inhaler'
                                  WHEN lower(mr.str) LIKE '%topical ointment'
                                      then 'topical ointment'
                                  WHEN lower(mr.str) LIKE '%rectal suppository'
                                      then 'rectal suppository'
                                  WHEN lower(mr.str) LIKE '%oral solution'
                                      then 'oral solution'
                                  WHEN lower(mr.str) LIKE '%topical lotion'
                                      then 'topical lotion'
                                  WHEN lower(mr.str) LIKE '%vaginal cream'
                                      then 'vaginal cream'
                                  WHEN lower(mr.str) LIKE '%injection'
                                      then 'injection'
                                  WHEN lower(mr.str) LIKE '%mouthwash'
                                      then 'mouthwash'
                                  WHEN lower(mr.str) LIKE '%topical oil'
                                      then 'topical oil'
                                  WHEN lower(mr.str) LIKE '%topical foam'
                                      then 'topical foam'
                                  WHEN lower(mr.str) LIKE '%ophthalmic solution'
                                      then 'ophthalmic solution'
                                  WHEN lower(mr.str) LIKE '%metered dose nasal spray'
                                      then 'metered dose nasal spray'
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
             from faers.mrconso mr
                      join faers.output out on out.cui = mr.cui
                      join faers.drug_mapping_exact dme
                           on dme.drugname_clean = replace(out.drugname_clean, ' w ', ' / ')
             where mr.sab = 'RXNORM'
               and out.match > 0.59
               and dme.rx_ingredient is null
               and mr.c13 = 'SCDF')
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient = replace(cte.string, concat(' ', cte.dose_form), ''),
    rx_dose_form  = cte.dose_form
from cte
where cte.drugname_clean = dme.drugname_clean;


-- and mr.c13 not in ('SY', 'TMSY')


with cte as (select distinct dme.drugname_clean,
                             lower(mr.str)                             as string,
                             lower(regexp_replace(mr.str, '^.* ', '')) as unit,
                             substring(mr.str, '[0-9]+[.]?[0-9]*')     AS amount
             from faers.mrconso mr
                      join faers.output out on out.cui = mr.cui
                      join faers.drug_mapping_exact dme
                           on dme.drugname_clean = replace(out.drugname_clean, ' w ', ' / ')
             where mr.sab = 'RXNORM'
               and mr.c13 = 'SCDC'
               and dme.rx_ingredient is null)
UPDATE faers.drug_mapping_exact dme
SET rx_ingredient   = replace(cte.string, concat(' ', cte.amount, ' ', cte.unit), ''),
    dose_unit_clean = cte.unit,
    dose_amt_clean  = cte.amount
from cte
where cte.drugname_clean = dme.drugname_clean;

with cte as (select distinct dme.drugname_clean,
                             lower(mr.str) as string,
                             mr.c13,
                             out.match,
                             mr.c14
             from faers.mrconso mr
                      join faers.output out on out.cui = mr.cui
                      join faers.drug_mapping_exact dme
                           on dme.drugname_clean = replace(out.drugname_clean, ' w ', ' / ')
             where mr.sab = 'RXNORM'
               and mr.c13 = 'SCD'
               and dme.drugname_clean ~ '.*[0-9].*'
               and dme.rxcui is null)
UPDATE faers.drug_mapping_exact dme
SET str   = cte.string,
    tty   = cte.c13,
    rxcui = cte.c14
from cte
where cte.drugname_clean = dme.drugname_clean;
