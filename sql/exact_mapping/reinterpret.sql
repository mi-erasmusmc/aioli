-- name: pin_to_in
with cte1 as (
    select distinct c.concept_id, dme.rx_ingredient
    from faers.drug_mapping_exact dme
             inner join staging_vocabulary.concept c
                        on dme.rx_ingredient = lower(c.concept_name)
    where c.concept_class_id = 'Precise Ingredient'
      and dme.rx_ingredient is not null
      and dme.rxcui is null),
     cte2 as (
         select cte1.rx_ingredient, string_agg(distinct lower(b.concept_name), ',') as ingredient
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
                  inner join cte1 on cte1.concept_id = concept_id_1
         group by cte1.rx_ingredient
         having count(distinct lower(a.concept_name)) = 1)
update faers.drug_mapping_exact dme
set rx_ingredient = cte2.ingredient
from cte2
where cte2.rx_ingredient = dme.rx_ingredient
  and rxcui is null;

-- name: remap_rx_df
update faers.drug_mapping_exact
set rx_dose_form = $2
where rx_dose_form = $1;
