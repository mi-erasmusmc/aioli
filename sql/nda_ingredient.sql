-- create NDA (new drug application) number mapping table
-- (NDA num maps to ingredient(s) in the FDA orange book reference dataset)

-- note the following table should be created one time when the FDA orange book (NDA ingredient lookup) table is loaded
-- name: drop_nda_lookup_table
drop table if exists faers.nda_ingredient;
-- name: create_nda_lookup_table
create table faers.nda_ingredient as
select distinct appl_no, ingredient, trade_name
from faers.nda;

-- name: drop_nda_mapping
drop table if exists faers.drug_nda_mapping;
-- name: create_nda_mapping
create table faers.drug_nda_mapping as
select distinct drug_name_original, nda_num, nda_ingredient, concept_id, update_method, rxcui
from (
         select distinct drugname              as drug_name_original,
                         nda_num,
                         null                  as nda_ingredient,
                         cast(null as integer) as concept_id,
                         null                  as update_method,
                         null                  as rxcui
         from faers.drug a
                  inner join faers.unique_all_case b
                             on a.primaryid = b.primaryid
         where b.isr is null
           and nda_num is not null
         union
         select distinct drugname              as drug_name_original,
                         cast(nda_num as varchar),
                         null                  as nda_ingredient,
                         cast(null as integer) as concept_id,
                         null                  as update_method,
                         null                  as rxcui
         from faers.drug_legacy a
                  inner join faers.unique_all_case b
                             on cast(a.isr as varchar) = b.isr
         where b.isr is not null
           and nda_num is not null
     ) aa;

-- name: drop_nda_index
drop index if exists faers.nda_num_ix;
-- name: create_nda_index
create index nda_num_ix on faers.drug_nda_mapping (nda_num);
-- name: set_nda_ingredient
UPDATE faers.drug_nda_mapping a
SET nda_ingredient = lower(ndai.ingredient)
FROM faers.nda_ingredient ndai
WHERE ndai.appl_no = a.nda_num;


-- name: map_nda_ingredient_to_rxnorm
WITH cte AS (SELECT a.drug_name_original, string_agg(DISTINCT CAST(rx.rxcui AS varchar), ',') AS rxcui
             FROM faers.drug_nda_mapping a
                      JOIN faers.rxnconso rx
                           ON lower(rx.str) = lower(a.nda_ingredient)
             GROUP BY a.drug_name_original)
UPDATE faers.drug_nda_mapping a
SET update_method = 'drug nda_num ingredients',
    rxcui         = cte.rxcui
FROM cte
WHERE cte.drug_name_original = a.drug_name_original;

-- name: update_concept_ids
UPDATE faers.drug_nda_mapping as drm
SET concept_id = c.concept_id
FROM staging_vocabulary.concept c
WHERE drm.rxcui = c.concept_code
  AND c.vocabulary_id = 'RxNorm';

-- name: update_drug_regex_table
UPDATE faers.drug_mapping AS drm
SET rxcui           = ai.rxcui,
    concept_id      = ai.concept_id,
    drug_name_clean = ai.nda_ingredient,
    update_method   = 'drug nda_num ingredients'
FROM faers.drug_nda_mapping AS ai
WHERE drm.drug_name_original = ai.drug_name_original
  AND drm.concept_id IS NULL
  AND (drm.rxcui IS NULL
    OR drm.rxcui NOT LIKE '%,%')
  AND ai.concept_id IS NOT NULL;

-- name: append_ambiguous_rxcuis
UPDATE faers.drug_mapping AS drm
SET rxcui         = concat(drm.rxcui, ',', ai.rxcui),
    update_method = 'drug nda_num ingredients ambiguous'
FROM faers.drug_nda_mapping ai
WHERE drm.drug_name_original = ai.drug_name_original
  AND drm.rxcui IS NOT NULL
  AND drm.concept_id IS NULL
  AND ai.rxcui IS NOT NULL
  AND ai.rxcui != drm.rxcui;