-- name: populate_standard_concept_id
update faers.drug_mapping drm
set standard_concept_id = concept_id;

-- name: to_standard_ingredient
with cte1 as (
    select distinct cast(scdm.standard_concept_id as integer)
    from faers.drug_mapping scdm
             inner join staging_vocabulary.concept c
                        on scdm.standard_concept_id = cast(c.concept_id as varchar)
                            and scdm.concept_id is not null
                            and (c.concept_class_id not in ('Clinical Drug Form', 'Ingredient', 'Dose Form')
                                or c.standard_concept is null)
),
     cte2 as (
         select concept_id_1, string_agg(distinct cast(concept_id_2 as varchar), ',') as concept_id_2
         from staging_vocabulary.concept_relationship cr
                  inner join staging_vocabulary.concept a
                             on cr.concept_id_1 = a.concept_id
                                 and cr.invalid_reason is null
                                 and a.vocabulary_id = 'RxNorm'
                  inner join staging_vocabulary.concept b
                             on cr.concept_id_2 = b.concept_id
                                 and b.vocabulary_id = 'RxNorm'
                                 and b.standard_concept = 'S'
                                 and b.concept_class_id = 'Ingredient'
                                 and concept_id_1 in (select cte1.standard_concept_id from cte1)
         group by concept_id_1
     )
update faers.drug_mapping scdm
set standard_concept_id = cte2.concept_id_2
from cte2
where scdm.standard_concept_id = cast(cte2.concept_id_1 as varchar);

-- name: to_standard_ingredient_incl_multi
with cte1 as (
    select distinct cast(scdm.standard_concept_id as integer)
    from faers.drug_mapping scdm
             inner join staging_vocabulary.concept c
                        on scdm.standard_concept_id = cast(c.concept_id as varchar)
                            and scdm.concept_id is not null
                            and (c.concept_class_id not in ('Ingredient', 'Dose Form')
                                or c.standard_concept is null)
),
     cte2 as (
         select concept_id_1, string_agg(distinct cast(concept_id_2 as varchar), ',') as concept_id_2
         from staging_vocabulary.concept_relationship cr
                  inner join staging_vocabulary.concept a
                             on cr.concept_id_1 = a.concept_id
                                 and cr.invalid_reason is null
                                 and a.vocabulary_id = 'RxNorm'
                  inner join staging_vocabulary.concept b
                             on cr.concept_id_2 = b.concept_id
                                 and b.vocabulary_id = 'RxNorm'
                                 and b.standard_concept = 'S'
                                 and b.concept_class_id = 'Ingredient'
                                 and concept_id_1 in (select cte1.standard_concept_id from cte1)
         group by concept_id_1
     )
update faers.drug_mapping scdm
set standard_concept_id = cte2.concept_id_2
from cte2
where scdm.standard_concept_id = cast(cte2.concept_id_1 as varchar);

-- name: brands_to_branded_dose_group
with cte1 as (select distinct scdm.concept_id
              from faers.drug_mapping scdm
                       inner join staging_vocabulary.concept c
                                  on scdm.standard_concept_id = cast(c.concept_id as varchar)
                                      and scdm.concept_id is not null
                                      and (c.concept_class_id not in ('Clinical Drug Form', 'Ingredient', 'Dose Form')
                                          or c.standard_concept is null)),
     cte2 as (select concept_id_1, max(concept_id_2) as concept_id_2
              from staging_vocabulary.concept_relationship cr
                       inner join staging_vocabulary.concept a
                                  on cr.concept_id_1 = a.concept_id
                                      and cr.invalid_reason is null
                                      and a.vocabulary_id = 'RxNorm'
                       inner join staging_vocabulary.concept b
                                  on cr.concept_id_2 = b.concept_id
                                      and b.vocabulary_id = 'RxNorm'
                                      and b.concept_class_id = 'Branded Dose Group'
                                      and concept_id_1 in (select cte1.concept_id from cte1)
              group by concept_id_1)
update faers.drug_mapping scdm
set standard_concept_id = cte2.concept_id_2
from cte2
where scdm.standard_concept_id = cast(cte2.concept_id_1 as varchar);

-- name: to_clinical_dose_group
with cte1 as (select distinct scdm.standard_concept_id
              from faers.drug_mapping scdm
                       inner join staging_vocabulary.concept c
                                  on scdm.standard_concept_id = cast(c.concept_id as varchar)
                                      and scdm.concept_id is not null
                                      and (c.concept_class_id not in ('Clinical Drug Form', 'Ingredient', 'Dose Form')
                                          or c.standard_concept is null)),
     cte2 as (select concept_id_1, max(concept_id_2) as concept_id_2
              from staging_vocabulary.concept_relationship cr
                       inner join staging_vocabulary.concept a
                                  on cr.concept_id_1 = a.concept_id
                                      and cr.invalid_reason is null
                                      and a.vocabulary_id = 'RxNorm'
                       inner join staging_vocabulary.concept b
                                  on cr.concept_id_2 = b.concept_id
                                      and b.vocabulary_id = 'RxNorm'
                                      and b.concept_class_id = 'Clinical Dose Group'
                                      and cast(concept_id_1 as varchar) in (select cte1.standard_concept_id from cte1)
              group by concept_id_1)
update faers.drug_mapping scdm
set standard_concept_id = cte2.concept_id_2
from cte2
where scdm.standard_concept_id = cast(cte2.concept_id_1 as varchar);

-- name: to_clinical_drug_comp
with cte1 as (select distinct scdm.standard_concept_id
              from faers.drug_mapping scdm
                       inner join staging_vocabulary.concept c
                                  on scdm.standard_concept_id = cast(c.concept_id as varchar)
                                      and scdm.concept_id is not null
                                      and (c.concept_class_id not in ('Clinical Drug Form', 'Ingredient', 'Dose Form')
                                          or c.standard_concept is null)),
     cte2 as (select concept_id_1, max(concept_id_2) as concept_id_2
              from staging_vocabulary.concept_relationship cr
                       inner join staging_vocabulary.concept a
                                  on cr.concept_id_1 = a.concept_id
                                      and cr.invalid_reason is null
                                      and a.vocabulary_id = 'RxNorm'
                       inner join staging_vocabulary.concept b
                                  on cr.concept_id_2 = b.concept_id
                                      and b.vocabulary_id = 'RxNorm'
                                      and b.concept_class_id = 'Clinical Drug Comp'
                                      and cast(concept_id_1 as varchar) in (select cte1.standard_concept_id from cte1)
              group by concept_id_1
     )
update faers.drug_mapping scdm
set standard_concept_id = cte2.concept_id_2
from cte2
where scdm.standard_concept_id = cast(cte2.concept_id_1 as varchar);

-- name: multiple_ingredients_to_clinical_drug_form
with cte1 as (select distinct scdm.standard_concept_id,
                              count(distinct c.concept_name)                               as ingredient_count,
                              concat('%', string_agg(distinct c.concept_name, '%/%'), '%') as cdf
              from faers.drug_mapping scdm
                       inner join staging_vocabulary.concept c
                                  on scdm.standard_concept_id like concat('%', cast(c.concept_id as varchar), '%')
              where scdm.standard_concept_id like '%,%'
                and c.concept_class_id = 'Ingredient'
              group by scdm.standard_concept_id),
     cte2 as (select max(c.concept_id) as id, cte1.standard_concept_id as standadard_id
              from staging_vocabulary.concept c
                       join cte1 on c.concept_name like cte1.cdf
              where concept_class_id = 'Clinical Drug Form'
                and array_length(regexp_split_to_array(c.concept_name, '/'), 1) = cte1.ingredient_count
                and standard_concept = 'S'
              group by cte1.standard_concept_id)
update faers.drug_mapping drm
set standard_concept_id = cast(cte2.id as varchar)
from cte2
where cte2.standadard_id = drm.standard_concept_id;


-- name: split_multi_ingredients_to_separate_entries
insert into faers.drug_mapping (select distinct scdm.drug_name_original,
                                                           scdm.drug_name_clean,
                                                           scdm.concept_id,
                                                           scdm.update_method,
                                                           scdm.rxcui,
                                                           scdm.name_count,
                                                           cast(c.concept_id as varchar) as standard_concept_id
                                           from staging_vocabulary.concept c
                                                    join faers.drug_mapping scdm
                                                         on cast(c.concept_id as varchar) = any
                                                            (string_to_array(scdm.standard_concept_id, ','))
                                           where scdm.standard_concept_id like '%,%'
                                             and c.concept_class_id = 'Ingredient'
                                             and c.standard_concept = 'S'
                                             and c.invalid_reason is null);


-- name: single_ingredient_clinical_drug_form_to_ingredient
with cte1 as (select distinct cast(scdm.standard_concept_id as integer)
              from faers.drug_mapping scdm
                       inner join staging_vocabulary.concept c
                                  on scdm.standard_concept_id = cast(c.concept_id as varchar)
                                      and scdm.concept_id is not null
                                      and c.concept_class_id = 'Clinical Drug Form'
                                      and c.concept_name not like '%/%'
),
     cte2 as (select concept_id_1, string_agg(distinct cast(concept_id_2 as varchar), ',') as concept_id_2
              from staging_vocabulary.concept_relationship cr
                       inner join staging_vocabulary.concept a
                                  on cr.concept_id_1 = a.concept_id
                                      and cr.invalid_reason is null
                                      and a.vocabulary_id = 'RxNorm'
                       inner join staging_vocabulary.concept b
                                  on cr.concept_id_2 = b.concept_id
                                      and b.vocabulary_id = 'RxNorm'
                                      and b.standard_concept = 'S'
                                      and b.concept_class_id = 'Ingredient'
                                      and concept_id_1 in (select cte1.standard_concept_id from cte1)
              group by concept_id_1
     )
update faers.drug_mapping scdm
set standard_concept_id = cte2.concept_id_2
from cte2
where scdm.standard_concept_id = cast(cte2.concept_id_1 as varchar);

-- name: clean_unmapped_multi
update faers.drug_mapping drm
set standard_concept_id = concept_id
where standard_concept_id like '%,%';

-- name: delete_multi
delete from faers.drug_mapping drm
where standard_concept_id like '%,%';

-- name: standardize_residue
with cte1 as (select distinct scdm.standard_concept_id
              from faers.drug_mapping scdm
                       inner join staging_vocabulary.concept c
                                  on scdm.standard_concept_id = cast(c.concept_id as varchar)
                                      and scdm.concept_id is not null
                                      and c.standard_concept is null
                                      and c.concept_class_id = 'Clinical Drug Form'),
     cte2 as (
         select concept_id_1, max(concept_id_2) as concept_id_2
         from staging_vocabulary.concept_relationship cr
                  inner join staging_vocabulary.concept a
                             on cr.concept_id_1 = a.concept_id
                                 and cr.invalid_reason is null
                                 and a.vocabulary_id = 'RxNorm'
                  inner join staging_vocabulary.concept b
                             on cr.concept_id_2 = b.concept_id
                                 and b.vocabulary_id = 'RxNorm'
                                 and b.standard_concept = 'S'
                                 and b.concept_class_id = 'Clinical Drug Form'
                                 and cast(concept_id_1 as varchar) in (select cte1.standard_concept_id from cte1)
         group by concept_id_1)
update faers.drug_mapping scdm
set standard_concept_id = cte2.concept_id_2
from cte2
where scdm.standard_concept_id = cast(cte2.concept_id_1 as varchar);


-- name: manual_mappings_kenalog
update faers.drug_mapping
set standard_concept_id = '903963'
where concept_id = 19026370
   or standard_concept_id = '19026370';

-- name: manual_mappings_tylenol
update faers.drug_mapping
set standard_concept_id = '1125315'
where concept_id = 19052129
   or standard_concept_id = '19052129';

-- name: manual_mappings_inderal
update faers.drug_mapping
set standard_concept_id = '1353766'
where concept_id = 19014310
   or standard_concept_id = '19014310';

-- name: manual_mappings_allegra
update faers.drug_mapping
set standard_concept_id = '40129571'
where concept_id in (19084668, 40223264, 19088750)
   or standard_concept_id in ('19084668', '40223264', '19088750');

-- name: manual_mappings_robitussin
update faers.drug_mapping
set standard_concept_id = '40141015'
where concept_id in (40230101)
   or standard_concept_id in ('40230101');

-- name: manual_mapping_reve_vita
update faers.drug_mapping
set standard_concept_id = '40156013'
where concept_id in (40168125)
   or standard_concept_id in ('40168125');

-- name: manual_mapping_optiray
update faers.drug_mapping
set standard_concept_id = '19069131'
where concept_id in (19125391)
   or standard_concept_id in ('19125391');







