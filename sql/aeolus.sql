-- name: parentheses
UPDATE faers.drug_mapping a
SET update_method   = 'regex ingredient name in parentheses',
    concept_id      = b.concept_id,
    drug_name_clean = lower(b.concept_name)
FROM staging_vocabulary.concept b
WHERE b.vocabulary_id = 'RxNorm'
  AND b.concept_class_id NOT LIKE 'Dose Form'
  AND lower(b.concept_name) = regexp_replace(a.drug_name_clean, '.* \((.*)\)', '\1', 'gi')
  AND a.concept_id is null
  AND drug_name_clean ~* '.* \((.*)\)';

-- name: drop_manual_table
DROP TABLE IF EXISTS faers.manual_mappings;

-- name: create_manual_table
CREATE TABLE faers.manual_mappings
(
    drug_name_original TEXT,
    concept_name       TEXT,
    concept_id         INTEGER
);

-- name: set_from_manual_table
UPDATE faers.drug_mapping dm
SET concept_id    = mm.concept_id,
    update_method = 'manual mapping list'
FROM faers.manual_mappings mm
WHERE (upper(dm.drug_name_original) = upper(mm.drug_name_original)
    OR (dm.drug_name_clean) = lower(mm.drug_name_original))
  AND dm.concept_id IS NULL;


-- name: vits
update faers.drug_mapping a
set drug_name_clean = 'multivitamin preparation'
where drug_name_clean like '%vitamin%'
  and drug_name_clean not like '%vitamin a%'
  and drug_name_clean not like '%vitamin b%'
  and drug_name_clean not like '%vitamin c%'
  and drug_name_clean not like '%vitamin k%'
  and drug_name_clean not like '%vitamin d%'
  and drug_name_clean not like '%vitamin e%'
  and a.concept_id is null;

-- name: find_mapping_vits
UPDATE faers.drug_mapping a
SET update_method = 'regex vitamins',
    concept_id    = b.concept_id
FROM staging_vocabulary.concept b
WHERE b.vocabulary_id = 'RxNorm'
  AND lower(b.concept_name) = a.drug_name_clean
  and a.concept_id is null;

-- in this section of mapping logic we derive RxNorm concepts for multi ingredient drugs (in any order of occurrence in the drug name) and for single ingredient clinical names and brand name drugs from within complex drug name strings

-- create a table that will hold the combined mapping of single and multiple ingredients and brand names based on separating and combining lists of words that occur within (single or multiple) ingredients or brands

-- name: 1_drop_table_words
drop table if exists drug_mapping_words;
-- name: 2_create_table_words
create table drug_mapping_words as
select distinct *
from (
         select drug_name_original, concept_name, concept_id, update_method, unnest(word_list::text[]) as word
         from (
                  select drug_name_original,
                         concept_name,
                         concept_id,
                         update_method,
                         regexp_split_to_array(lower(drug_name_original),
                                               E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') as word_list
                  from (
                           select distinct drugname              as drug_name_original,
                                           cast(null as varchar) as concept_name,
                                           cast(null as integer) as concept_id,
                                           null                  as update_method
                           from faers.drug a
                                    inner join faers.unique_all_case b on a.primaryid = b.primaryid
                           where b.isr is null
                           union
                           select distinct drugname              as drug_name_original,
                                           cast(null as varchar) as concept_name,
                                           cast(null as integer) as concept_id,
                                           null                  as update_method
                           from faers.drug_legacy a
                                    inner join faers.unique_all_case b on cast(a.isr as varchar) = b.isr
                           where b.isr is not null
                       ) aa
                  order by drug_name_original desc
              ) bb
     ) cc
where word NOT IN ('', 'syrup', 'hcl', 'hydrochloride', 'acetic', 'sodium', 'calcium', 'sulphate', 'monohydrate')
  and word not in (select distinct unnest(regexp_split_to_array(lower(concept_name),
                                                                E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                   from staging_vocabulary.concept b
                   where b.vocabulary_id = 'RxNorm'
                     and b.concept_class_id = 'Dose Form'
                   order by 1);


-- name: 3_drop_table_multi_ingredient
drop table if exists rxnorm_mapping_multi_ingredient_list;
-- name: 4_create_multi_ingredient_table
create table rxnorm_mapping_multi_ingredient_list as
select ingredient_list, max(concept_id) as concept_id, max(concept_name) as concept_name
from (
         select concept_id, concept_name, string_agg(word, ' ' order by word) as ingredient_list
         from (
                  select concept_id, concept_name, unnest(word_list::text[]) as word
                  from (
                           select concept_id,
                                  concept_name,
                                  regexp_split_to_array(lower(concept_name),
                                                        E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') as word_list
                           from (
                                    select lower(concept_name) as concept_name, concept_id
                                    from staging_vocabulary.concept b
                                    where b.vocabulary_id = 'RxNorm'
                                      and b.concept_class_id = 'Clinical Drug Form'
                                      and b.standard_concept = 'S'
                                      and concept_name like '%\/%'
                                ) aa
                           order by concept_name desc
                       ) bb
              ) cc
         where word not in ('')
           and word not in (select distinct unnest(regexp_split_to_array(lower(concept_name),
                                                                         E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                            from staging_vocabulary.concept b
                            where b.vocabulary_id = 'RxNorm'
                              and b.concept_class_id = 'Dose Form'
                            order by 1)
           and word not in ('', '-', ' ', 'a', 'and', 'ex', '10a', '11a', '12f', '18c', '19f', '99m', 'g', 'g1', 'g2',
                            'g3', 'g4', 'h', 'i', 'in', 'jelly', 'leaf', 'o', 'of', 'or', 'p', 's', 't', 'v', 'with',
                            'x', 'y', 'z')
           and word !~ '^\d+$|\y\d+\-\d+\y|\y\d+\.\d+\y'
         group by concept_id, concept_name
     ) dd
group by ingredient_list;

-- name: 5_drop_mapping_list
drop table if exists drug_mapping_multi_ingredient_list;
-- name: 6_create_mapping_list
create table drug_mapping_multi_ingredient_list as
select drug_name_original, ingredient_list, max(concept_id) as concept_id, max(concept_name) as concept_name
from (
         select concept_id, drug_name_original, concept_name, string_agg(word, ' ' order by word) as ingredient_list
         from (
                  select distinct concept_id, drug_name_original, concept_name, unnest(word_list::text[]) as word
                  from (
                           select concept_id,
                                  drug_name_original,
                                  concept_name,
                                  regexp_split_to_array(lower(drug_name_original),
                                                        E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') as word_list
                           from (
                                    select distinct drugname              as drug_name_original,
                                                    cast(null as varchar) as concept_name,
                                                    cast(null as integer) as concept_id,
                                                    null                  as update_method
                                    from faers.drug a
                                             inner join faers.unique_all_case b on a.primaryid = b.primaryid
                                    where b.isr is null
                                    union
                                    select distinct drugname              as drug_name_original,
                                                    cast(null as varchar) as concept_name,
                                                    cast(null as integer) as concept_id,
                                                    null                  as update_method
                                    from faers.drug_legacy a
                                             inner join faers.unique_all_case b on cast(a.isr as varchar) = b.isr
                                    where b.isr is not null
                                ) aa
                           order by concept_name desc
                       ) bb
              ) cc
         where word not in ('')
           and word not in (select distinct unnest(regexp_split_to_array(lower(concept_name),
                                                                         E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                            from staging_vocabulary.concept b
                            where b.vocabulary_id = 'RxNorm'
                              and b.concept_class_id = 'Dose Form'
                            order by 1)
           and word in (select *
                        from (
                                 select distinct unnest(regexp_split_to_array(lower(concept_name),
                                                                              E'[\ \,\(\)\{\}\\\\/\^\%\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+')) as word
                                 from staging_vocabulary.concept b
                                 where b.vocabulary_id = 'RxNorm'
                                   and b.concept_class_id = 'Clinical Drug Form'
                                   and b.standard_concept = 'S'
                                   and b.concept_name like '%\/%'
                             ) aa
                        where word not in
                              ('', '-', ' ', 'a', 'and', 'ex', '10a', '11a', '12f', '18c', '19f', '99m', 'g', 'g1',
                               'g2',
                               'g3', 'g4', 'h', 'i', 'in', 'jelly', 'leaf', 'o', 'of', 'or', 'p', 's', 't', 'v', 'with',
                               'x', 'y', 'z')
                          and word !~ '^\d+$|\y\d+\-\d+\y|\y\d+\.\d+\y'
                        order by 1
         )
         group by concept_id, drug_name_original, concept_name
     ) dd
group by drug_name_original, ingredient_list;


-- name: 7_map_drugs_with_multiple_ingredient
UPDATE drug_mapping_words c
SET update_method = 'multiple ingredient match',
    concept_name  = b.concept_name,
    concept_id    = b.concept_id
from (
         select distinct a.drug_name_original,
                         max(lower(b1.concept_name)) as concept_name,
                         max(b1.concept_id)          as concept_id
         from drug_mapping_multi_ingredient_list a
                  inner join rxnorm_mapping_multi_ingredient_list b1
                             on a.ingredient_list = b1.ingredient_list
         group by a.drug_name_original
     ) b
where c.drug_name_original = b.drug_name_original
  and c.concept_id is null;


-- name: 8_drop_single_ingredient_table
drop table if exists rxnorm_mapping_single_ingredient_list;
-- name: 9_create_single_ingredient_vocab_table
create table rxnorm_mapping_single_ingredient_list as
select ingredient_list, max(concept_id) as concept_id, max(concept_name) as concept_name
from (
         select concept_id, concept_name, string_agg(word, ' ' order by word) as ingredient_list
         from (
                  select concept_id, concept_name, unnest(word_list::text[]) as word
                  from (
                           select concept_id,
                                  concept_name,
                                  regexp_split_to_array(lower(concept_name),
                                                        E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') as word_list
                           from (
                                    select lower(concept_name) as concept_name, concept_id
                                    from staging_vocabulary.concept b
                                    where b.vocabulary_id = 'RxNorm'
                                      and b.concept_class_id = 'Ingredient'
                                ) aa
                           order by concept_name desc
                       ) bb
              ) cc
         where word not in ('')
           and word not in (select distinct unnest(regexp_split_to_array(lower(concept_name),
                                                                         E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                            from staging_vocabulary.concept b
                            where b.vocabulary_id = 'RxNorm'
                              and b.concept_class_id = 'Dose Form'
                            order by 1)
           and word not in ('', '-', ' ', 'a', 'and', 'ex', '10a', '11a', '12f', '18c', '19f', '99m', 'g', 'g1', 'g2',
                            'g3', 'g4', 'h', 'i', 'in', 'jelly', 'leaf', 'o', 'of', 'or', 'p', 's', 't', 'v', 'with',
                            'x', 'y', 'z')
           and word !~ '^\d+$|\y\d+\-\d+\y|\y\d+\.\d+\y'
         group by concept_id, concept_name
     ) dd
group by ingredient_list;

-- name: 10_drop_drug_mapping_single_ing
drop table if exists drug_mapping_single_ingredient_list;
-- name: 11_create_single_ingredient_drug_mapping_table
create table drug_mapping_single_ingredient_list as
select drug_name_original, ingredient_list, max(concept_id) as concept_id, max(concept_name) as concept_name
from (
         select concept_id, drug_name_original, concept_name, string_agg(word, ' ' order by word) as ingredient_list
         from (
                  select distinct concept_id, drug_name_original, concept_name, unnest(word_list::text[]) as word
                  from (
                           select concept_id,
                                  drug_name_original,
                                  concept_name,
                                  regexp_split_to_array(lower(drug_name_original),
                                                        E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') as word_list
                           from (
                                    select distinct drugname              as drug_name_original,
                                                    cast(null as varchar) as concept_name,
                                                    cast(null as integer) as concept_id,
                                                    null                  as update_method
                                    from faers.drug a
                                             inner join faers.unique_all_case b on a.primaryid = b.primaryid
                                    where b.isr is null
                                      and drugname not like '%/%'
                                      and drugname not like '% and %'
                                      and drugname not like '% with %'
                                      and drugname not like '%+%'
                                    union
                                    select distinct drugname              as drug_name_original,
                                                    cast(null as varchar) as concept_name,
                                                    cast(null as integer) as concept_id,
                                                    null                  as update_method
                                    from faers.drug_legacy a
                                             inner join faers.unique_all_case b on cast(a.isr as varchar) = b.isr
                                    where b.isr is not null
                                      and drugname not like '%/%'
                                      and drugname not like '% and %'
                                      and drugname not like '% with %'
                                      and drugname not like '%+%'
                                ) aa
                           order by concept_name desc
                       ) bb
              ) cc
         where word not in ('')
           and word not in (select distinct unnest(regexp_split_to_array(lower(concept_name),
                                                                         E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                            from staging_vocabulary.concept b
                            where b.vocabulary_id = 'RxNorm'
                              and b.concept_class_id = 'Dose Form'
                            order by 1)
           and word in (select *
                        from (
                                 select distinct unnest(regexp_split_to_array(lower(concept_name),
                                                                              E'[\ \,\(\)\{\}\\\\/\^\%\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+')) as word
                                 from staging_vocabulary.concept b
                                 where b.vocabulary_id = 'RxNorm'
                                   and b.concept_class_id in ('Ingredient')
                             ) aa
                        where word not in
                              ('', '-', ' ', 'a', 'and', 'ex', '10a', '11a', '12f', '18c', '19f', '99m', 'g', 'g1',
                               'g2',
                               'g3', 'g4', 'h', 'i', 'in', 'jelly', 'leaf', 'o', 'of', 'or', 'p', 's', 't', 'v', 'with',
                               'x', 'y', 'z')
                          and word !~ '^\d+$|\y\d+\-\d+\y|\y\d+\.\d+\y'
                        order by 1
         )
         group by concept_id, drug_name_original, concept_name
     ) dd
group by drug_name_original, ingredient_list;


-- name: 12_map_drugs_with_single_ingredient_to_ingredient_concepts
update drug_mapping_words c
SET update_method = 'single ingredient match',
    concept_name  = b.concept_name,
    concept_id    = b.concept_id
from (
         select distinct a.drug_name_original,
                         max(lower(b1.concept_name)) as concept_name,
                         max(b1.concept_id)          as concept_id
         from drug_mapping_single_ingredient_list a
                  inner join rxnorm_mapping_single_ingredient_list b1
                             on a.ingredient_list = b1.ingredient_list
         group by a.drug_name_original
     ) b
where c.drug_name_original = b.drug_name_original
  and c.update_method is null
  and b.concept_name not in
      ('vitamin a', 'sodium', 'hydrochloride', 'hcl', 'calcium', 'cold cream', 'vitamin b 12', 'maleate', 'tartrate',
       'mesylate', 'monohydrate', 'succinate', 'corn syrup', 'factor x', 'protein s')
  and c.update_method is null
  and c.concept_id is null;


-- name: 13_drop_brands_table
drop table if exists rxnorm_mapping_brand_name_list;
-- name: 14_create_brands_table
create table rxnorm_mapping_brand_name_list as
select ingredient_list, max(concept_id) as concept_id, max(concept_name) as concept_name
from (
         select concept_id, concept_name, string_agg(word, ' ' order by word) as ingredient_list
         from (
                  select concept_id, concept_name, unnest(word_list::text[]) as word
                  from (
                           select concept_id,
                                  concept_name,
                                  regexp_split_to_array(lower(concept_name),
                                                        E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') as word_list
                           from (
                                    select lower(concept_name) as concept_name, concept_id
                                    from staging_vocabulary.concept b
                                    where b.vocabulary_id = 'RxNorm'
                                      and b.concept_class_id = 'Brand Name'
                                ) aa
                           order by concept_name desc
                       ) bb
              ) cc
         where word not in ('')
           and word not in (select distinct unnest(regexp_split_to_array(lower(concept_name),
                                                                         E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                            from staging_vocabulary.concept b
                            where b.vocabulary_id = 'RxNorm'
                              and b.concept_class_id = 'Dose Form'
                            order by 1)
           and word not in ('', '-', ' ', 'A', 'AND', 'EX', '10A', '11A', '12F', '18C', '19F', '99M', 'G', 'G1', 'G2',
                            'G3', 'G4', 'H', 'I', 'IN', 'JELLY', 'LEAF', 'O', 'OF', 'OR', 'P', 'S', 'T', 'V', 'WITH',
                            'X', 'Y', 'Z')
           and word !~ '^\d+$|\y\d+\-\d+\y|\y\d+\.\d+\y'
         group by concept_id, concept_name
     ) dd
group by ingredient_list;

-- name: 15_drop_source_brand_mapping_table
drop table if exists drug_mapping_brand_name_list;
-- name: 16_create_source_brand_mapping_table
create table drug_mapping_brand_name_list as
select drug_name_original, ingredient_list, max(concept_id) as concept_id, max(concept_name) as concept_name
from (
         select concept_id, drug_name_original, concept_name, string_agg(word, ' ' order by word) as ingredient_list
         from (
                  select distinct concept_id, drug_name_original, concept_name, unnest(word_list::text[]) as word
                  from (
                           select concept_id,
                                  drug_name_original,
                                  concept_name,
                                  regexp_split_to_array(lower(drug_name_original),
                                                        E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') as word_list
                           from (
                                    select distinct drugname              as drug_name_original,
                                                    cast(null as varchar) as concept_name,
                                                    cast(null as integer) as concept_id,
                                                    null                  as update_method
                                    from faers.drug a
                                             inner join faers.unique_all_case b on a.primaryid = b.primaryid
                                    where b.isr is null
                                      and drugname not like '%/%'
                                      and drugname not like '% and %'
                                      and drugname not like '% with %'
                                      and drugname not like '%+%'
                                    union
                                    select distinct drugname              as drug_name_original,
                                                    cast(null as varchar) as concept_name,
                                                    cast(null as integer) as concept_id,
                                                    null                  as update_method
                                    from faers.drug_legacy a
                                             inner join faers.unique_all_case b on cast(a.isr as varchar) = b.isr
                                    where b.isr is not null
                                      and drugname not like '%/%'
                                      and drugname not like '% and %'
                                      and drugname not like '% with %'
                                      and drugname not like '%+%'
                                ) aa
                           order by concept_name desc
                       ) bb
              ) cc
         where word not in ('')
           and word not in (select distinct unnest(regexp_split_to_array(lower(concept_name),
                                                                         E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                            from staging_vocabulary.concept b
                            where b.vocabulary_id = 'RxNorm'
                              and b.concept_class_id = 'Dose Form'
                            order by 1)
           and word in (select *
                        from (
                                 select distinct unnest(regexp_split_to_array(lower(concept_name),
                                                                              E'[\ \,\(\)\{\}\\\\/\^\%\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+')) as word
                                 from staging_vocabulary.concept b
                                 where b.vocabulary_id = 'RxNorm'
                                   and b.concept_class_id in ('Brand Name')
                             ) aa
                        where word not in
                              ('', '-', ' ', 'a', 'and', 'ex', '10a', '11a', '12f', '18c', '19f', '99m', 'g', 'g1',
                               'g2',
                               'g3', 'g4', 'h', 'i', 'in', 'jelly', 'leaf', 'o', 'of', 'or', 'p', 's', 't', 'v', 'with',
                               'x', 'y', 'z')
                          and word !~ '^\d+$|\y\d+\-\d+\y|\y\d+\.\d+\y'
                        order by 1
         )
         group by concept_id, drug_name_original, concept_name
     ) dd
group by drug_name_original, ingredient_list;

-- name: 17_map_drugs_containing_brands
UPDATE drug_mapping_words c
SET update_method = 'brand name match',
    concept_name  = b.concept_name,
    concept_id    = b.concept_id
from (
         select distinct a.drug_name_original,
                         max(lower(b1.concept_name)) as concept_name,
                         max(b1.concept_id)          as concept_id
         from drug_mapping_brand_name_list a
                  inner join rxnorm_mapping_brand_name_list b1
                             on a.ingredient_list = b1.ingredient_list
         group by a.drug_name_original
     ) b
where c.drug_name_original = b.drug_name_original
  and c.update_method is null
  and c.concept_id is null
  and b.concept_name not in
      ('g.b.h. shampoo', 'a.p.l.', 'c.p.m.', 'allergy cream', 'mg 217', 'acid jelly', 'c/t/s', 'm.a.h.', 'i.d.a.',
       'n.t.a.', 'formula 21', 'pro otic', 'e.s.p.', 'preparation h cream', 'h 9600 sr',
       '12 hour cold', 'glyceryl t', 'g bid', 'at 10', 'compound 347', 'ms/s', 'hydro 40', 'hp 502', 'liquid pred',
       'oral peroxide', 'baby gas', 'bc powder 742/38/222', 'comfort gel', 'mag 64', 'k effervescent', 'nasal la',
       'therapeutic shampoo',
       'chewable calcium', 'pain relief (effervescent)', 'stress liquid', 'iron 300', 'fs shampoo', 't/gel conditioner',
       'ex dec', 'dr.s cream', 'joint gel', 'cp oral', 'otic care', 'dr.s cream',
       'nasal relief', 'medicated blue', 'fe 50', 'biotene toothpaste', 'vitamin a', 'sodium', 'hydrochloride', 'hcl',
       'calcium', 'long lasting nasal', 'triple paste', 'k + potassium', 'nasal decongestant syrup',
       'cold cream', 'vitamin b 12', 'maleate', 'tartrate', 'mesylate', 'monohydrate', 'succinate', 'corn syrup',
       'factor x', 'protein s');


-- name: 18_update_original_drug_regex
UPDATE faers.drug_mapping c
SET update_method   = b.update_method,
    drug_name_clean = b.concept_name,
    concept_id      = b.concept_id
from (
         select distinct drug_name_original, concept_name, concept_id, update_method
         from drug_mapping_words
         where concept_id is not null
     ) b
where c.drug_name_original = b.drug_name_original
  and c.concept_id is null;
