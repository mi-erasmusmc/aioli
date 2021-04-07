-- name: parentheses
UPDATE faers.drug_mapping a
SET update_method   = 'regex ingredient name in parentheses',
    concept_id      = b.concept_id,
    drug_name_clean = lower(b.concept_name)
FROM staging_vocabulary.concept b
WHERE b.vocabulary_id = 'RxNorm'
  AND b.concept_class_id NOT LIKE 'Dose Form'
  AND lower(b.concept_name) = regexp_replace(a.drug_name_clean, '.* \((.*)\)', '\1', 'gi')
  AND a.concept_id IS NULL
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
UPDATE faers.drug_mapping a
SET drug_name_clean = 'multivitamin preparation'
WHERE drug_name_clean LIKE '%vitamin%'
  AND drug_name_clean NOT LIKE '%vitamin a%'
  AND drug_name_clean NOT LIKE '%vitamin b%'
  AND drug_name_clean NOT LIKE '%vitamin c%'
  AND drug_name_clean NOT LIKE '%vitamin k%'
  AND drug_name_clean NOT LIKE '%vitamin d%'
  AND drug_name_clean NOT LIKE '%vitamin e%'
  AND a.concept_id IS NULL;

-- name: find_mapping_vits
UPDATE faers.drug_mapping a
SET update_method = 'regex vitamins',
    concept_id    = b.concept_id
FROM staging_vocabulary.concept b
WHERE b.vocabulary_id = 'RxNorm'
  AND lower(b.concept_name) = a.drug_name_clean
  AND a.concept_id IS NULL;

-- in this section of mapping logic we derive RxNorm concepts for multi ingredient drugs (in any order of occurrence in the drug name) and for single ingredient clinical names and brand name drugs from within complex drug name strings

-- create a table that will hold the combined mapping of single and multiple ingredients and brand names based on separating and combining lists of words that occur within (single or multiple) ingredients or brands

-- name: 1_drop_table_words
DROP TABLE IF EXISTS drug_mapping_words;
-- name: 2_create_table_words
CREATE TABLE drug_mapping_words AS
SELECT DISTINCT *
FROM (
         SELECT drug_name_original, concept_name, concept_id, update_method, unnest(word_list::TEXT[]) AS word
         FROM (
                  SELECT drug_name_original,
                         concept_name,
                         concept_id,
                         update_method,
                         regexp_split_to_array(lower(drug_name_original),
                                               E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') AS word_list
                  FROM (
                           SELECT DISTINCT drugname              AS drug_name_original,
                                           cast(NULL AS VARCHAR) AS concept_name,
                                           cast(NULL AS INTEGER) AS concept_id,
                                           NULL                  AS update_method
                           FROM faers.drug a
                                    INNER JOIN faers.unique_all_case b ON a.primaryid = b.primaryid
                           WHERE b.isr IS NULL
                           UNION
                           SELECT DISTINCT drugname              AS drug_name_original,
                                           cast(NULL AS VARCHAR) AS concept_name,
                                           cast(NULL AS INTEGER) AS concept_id,
                                           NULL                  AS update_method
                           FROM faers.drug_legacy a
                                    INNER JOIN faers.unique_all_case b ON cast(a.isr AS VARCHAR) = b.isr
                           WHERE b.isr IS NOT NULL
                       ) aa
                  ORDER BY drug_name_original DESC
              ) bb
     ) cc
WHERE word NOT IN ('', 'syrup', 'hcl', 'hydrochloride', 'acetic', 'sodium', 'calcium', 'sulphate', 'monohydrate')
  AND word NOT IN (SELECT DISTINCT unnest(regexp_split_to_array(lower(concept_name),
                                                                E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                   FROM staging_vocabulary.concept b
                   WHERE b.vocabulary_id = 'RxNorm'
                     AND b.concept_class_id = 'Dose Form'
                   ORDER BY 1);


-- name: 3_drop_table_multi_ingredient
DROP TABLE IF EXISTS rxnorm_mapping_multi_ingredient_list;
-- name: 4_create_multi_ingredient_table
CREATE TABLE rxnorm_mapping_multi_ingredient_list AS
SELECT ingredient_list, max(concept_id) AS concept_id, max(concept_name) AS concept_name
FROM (
         SELECT concept_id, concept_name, string_agg(word, ' ' ORDER BY word) AS ingredient_list
         FROM (
                  SELECT concept_id, concept_name, unnest(word_list::TEXT[]) AS word
                  FROM (
                           SELECT concept_id,
                                  concept_name,
                                  regexp_split_to_array(lower(concept_name),
                                                        E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') AS word_list
                           FROM (
                                    SELECT lower(concept_name) AS concept_name, concept_id
                                    FROM staging_vocabulary.concept b
                                    WHERE b.vocabulary_id = 'RxNorm'
                                      AND b.concept_class_id = 'Clinical Drug Form'
                                      AND b.standard_concept = 'S'
                                      AND concept_name LIKE '%\/%'
                                ) aa
                           ORDER BY concept_name DESC
                       ) bb
              ) cc
         WHERE word NOT IN ('')
           AND word NOT IN (SELECT DISTINCT unnest(regexp_split_to_array(lower(concept_name),
                                                                         E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                            FROM staging_vocabulary.concept b
                            WHERE b.vocabulary_id = 'RxNorm'
                              AND b.concept_class_id = 'Dose Form'
                            ORDER BY 1)
           AND word NOT IN ('', '-', ' ', 'a', 'and', 'ex', '10a', '11a', '12f', '18c', '19f', '99m', 'g', 'g1', 'g2',
                            'g3', 'g4', 'h', 'i', 'in', 'jelly', 'leaf', 'o', 'of', 'or', 'p', 's', 't', 'v', 'with',
                            'x', 'y', 'z')
           AND word !~ '^\d+$|\y\d+\-\d+\y|\y\d+\.\d+\y'
         GROUP BY concept_id, concept_name
     ) dd
GROUP BY ingredient_list;

-- name: 5_drop_mapping_list
DROP TABLE IF EXISTS drug_mapping_multi_ingredient_list;
-- name: 6_create_mapping_list
CREATE TABLE drug_mapping_multi_ingredient_list AS
SELECT drug_name_original, ingredient_list, max(concept_id) AS concept_id, max(concept_name) AS concept_name
FROM (
         SELECT concept_id, drug_name_original, concept_name, string_agg(word, ' ' ORDER BY word) AS ingredient_list
         FROM (
                  SELECT DISTINCT concept_id, drug_name_original, concept_name, unnest(word_list::TEXT[]) AS word
                  FROM (
                           SELECT concept_id,
                                  drug_name_original,
                                  concept_name,
                                  regexp_split_to_array(lower(drug_name_original),
                                                        E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') AS word_list
                           FROM (
                                    SELECT DISTINCT drugname              AS drug_name_original,
                                                    cast(NULL AS VARCHAR) AS concept_name,
                                                    cast(NULL AS INTEGER) AS concept_id,
                                                    NULL                  AS update_method
                                    FROM faers.drug a
                                             INNER JOIN faers.unique_all_case b ON a.primaryid = b.primaryid
                                    WHERE b.isr IS NULL
                                    UNION
                                    SELECT DISTINCT drugname              AS drug_name_original,
                                                    cast(NULL AS VARCHAR) AS concept_name,
                                                    cast(NULL AS INTEGER) AS concept_id,
                                                    NULL                  AS update_method
                                    FROM faers.drug_legacy a
                                             INNER JOIN faers.unique_all_case b ON cast(a.isr AS VARCHAR) = b.isr
                                    WHERE b.isr IS NOT NULL
                                ) aa
                           ORDER BY concept_name DESC
                       ) bb
              ) cc
         WHERE word NOT IN ('')
           AND word NOT IN (SELECT DISTINCT unnest(regexp_split_to_array(lower(concept_name),
                                                                         E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                            FROM staging_vocabulary.concept b
                            WHERE b.vocabulary_id = 'RxNorm'
                              AND b.concept_class_id = 'Dose Form'
                            ORDER BY 1)
           AND word IN (SELECT *
                        FROM (
                                 SELECT DISTINCT unnest(regexp_split_to_array(lower(concept_name),
                                                                              E'[\ \,\(\)\{\}\\\\/\^\%\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+')) AS word
                                 FROM staging_vocabulary.concept b
                                 WHERE b.vocabulary_id = 'RxNorm'
                                   AND b.concept_class_id = 'Clinical Drug Form'
                                   AND b.standard_concept = 'S'
                                   AND b.concept_name LIKE '%\/%'
                             ) aa
                        WHERE word NOT IN
                              ('', '-', ' ', 'a', 'and', 'ex', '10a', '11a', '12f', '18c', '19f', '99m', 'g', 'g1',
                               'g2',
                               'g3', 'g4', 'h', 'i', 'in', 'jelly', 'leaf', 'o', 'of', 'or', 'p', 's', 't', 'v', 'with',
                               'x', 'y', 'z')
                          AND word !~ '^\d+$|\y\d+\-\d+\y|\y\d+\.\d+\y'
                        ORDER BY 1
         )
         GROUP BY concept_id, drug_name_original, concept_name
     ) dd
GROUP BY drug_name_original, ingredient_list;


-- name: 7_map_drugs_with_multiple_ingredient
UPDATE drug_mapping_words c
SET update_method = 'multiple ingredient match',
    concept_name  = b.concept_name,
    concept_id    = b.concept_id
FROM (
         SELECT DISTINCT a.drug_name_original,
                         max(lower(b1.concept_name)) AS concept_name,
                         max(b1.concept_id)          AS concept_id
         FROM drug_mapping_multi_ingredient_list a
                  INNER JOIN rxnorm_mapping_multi_ingredient_list b1
                             ON a.ingredient_list = b1.ingredient_list
         GROUP BY a.drug_name_original
     ) b
WHERE c.drug_name_original = b.drug_name_original
  AND c.concept_id IS NULL;


-- name: 8_drop_single_ingredient_table
DROP TABLE IF EXISTS rxnorm_mapping_single_ingredient_list;
-- name: 9_create_single_ingredient_vocab_table
CREATE TABLE rxnorm_mapping_single_ingredient_list AS
SELECT ingredient_list, max(concept_id) AS concept_id, max(concept_name) AS concept_name
FROM (
         SELECT concept_id, concept_name, string_agg(word, ' ' ORDER BY word) AS ingredient_list
         FROM (
                  SELECT concept_id, concept_name, unnest(word_list::TEXT[]) AS word
                  FROM (
                           SELECT concept_id,
                                  concept_name,
                                  regexp_split_to_array(lower(concept_name),
                                                        E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') AS word_list
                           FROM (
                                    SELECT lower(concept_name) AS concept_name, concept_id
                                    FROM staging_vocabulary.concept b
                                    WHERE b.vocabulary_id = 'RxNorm'
                                      AND b.concept_class_id = 'Ingredient'
                                ) aa
                           ORDER BY concept_name DESC
                       ) bb
              ) cc
         WHERE word NOT IN ('')
           AND word NOT IN (SELECT DISTINCT unnest(regexp_split_to_array(lower(concept_name),
                                                                         E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                            FROM staging_vocabulary.concept b
                            WHERE b.vocabulary_id = 'RxNorm'
                              AND b.concept_class_id = 'Dose Form'
                            ORDER BY 1)
           AND word NOT IN ('', '-', ' ', 'a', 'and', 'ex', '10a', '11a', '12f', '18c', '19f', '99m', 'g', 'g1', 'g2',
                            'g3', 'g4', 'h', 'i', 'in', 'jelly', 'leaf', 'o', 'of', 'or', 'p', 's', 't', 'v', 'with',
                            'x', 'y', 'z')
           AND word !~ '^\d+$|\y\d+\-\d+\y|\y\d+\.\d+\y'
         GROUP BY concept_id, concept_name
     ) dd
GROUP BY ingredient_list;

-- name: 10_drop_drug_mapping_single_ing
DROP TABLE IF EXISTS drug_mapping_single_ingredient_list;
-- name: 11_create_single_ingredient_drug_mapping_table
CREATE TABLE drug_mapping_single_ingredient_list AS
SELECT drug_name_original, ingredient_list, max(concept_id) AS concept_id, max(concept_name) AS concept_name
FROM (
         SELECT concept_id, drug_name_original, concept_name, string_agg(word, ' ' ORDER BY word) AS ingredient_list
         FROM (
                  SELECT DISTINCT concept_id, drug_name_original, concept_name, unnest(word_list::TEXT[]) AS word
                  FROM (
                           SELECT concept_id,
                                  drug_name_original,
                                  concept_name,
                                  regexp_split_to_array(lower(drug_name_original),
                                                        E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') AS word_list
                           FROM (
                                    SELECT DISTINCT drugname              AS drug_name_original,
                                                    cast(NULL AS VARCHAR) AS concept_name,
                                                    cast(NULL AS INTEGER) AS concept_id,
                                                    NULL                  AS update_method
                                    FROM faers.drug a
                                             INNER JOIN faers.unique_all_case b ON a.primaryid = b.primaryid
                                    WHERE b.isr IS NULL
                                      AND drugname NOT LIKE '%/%'
                                      AND drugname NOT LIKE '% and %'
                                      AND drugname NOT LIKE '% with %'
                                      AND drugname NOT LIKE '%+%'
                                    UNION
                                    SELECT DISTINCT drugname              AS drug_name_original,
                                                    cast(NULL AS VARCHAR) AS concept_name,
                                                    cast(NULL AS INTEGER) AS concept_id,
                                                    NULL                  AS update_method
                                    FROM faers.drug_legacy a
                                             INNER JOIN faers.unique_all_case b ON cast(a.isr AS VARCHAR) = b.isr
                                    WHERE b.isr IS NOT NULL
                                      AND drugname NOT LIKE '%/%'
                                      AND drugname NOT LIKE '% and %'
                                      AND drugname NOT LIKE '% with %'
                                      AND drugname NOT LIKE '%+%'
                                ) aa
                           ORDER BY concept_name DESC
                       ) bb
              ) cc
         WHERE word NOT IN ('')
           AND word NOT IN (SELECT DISTINCT unnest(regexp_split_to_array(lower(concept_name),
                                                                         E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                            FROM staging_vocabulary.concept b
                            WHERE b.vocabulary_id = 'RxNorm'
                              AND b.concept_class_id = 'Dose Form'
                            ORDER BY 1)
           AND word IN (SELECT *
                        FROM (
                                 SELECT DISTINCT unnest(regexp_split_to_array(lower(concept_name),
                                                                              E'[\ \,\(\)\{\}\\\\/\^\%\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+')) AS word
                                 FROM staging_vocabulary.concept b
                                 WHERE b.vocabulary_id = 'RxNorm'
                                   AND b.concept_class_id IN ('Ingredient')
                             ) aa
                        WHERE word NOT IN
                              ('', '-', ' ', 'a', 'and', 'ex', '10a', '11a', '12f', '18c', '19f', '99m', 'g', 'g1',
                               'g2',
                               'g3', 'g4', 'h', 'i', 'in', 'jelly', 'leaf', 'o', 'of', 'or', 'p', 's', 't', 'v', 'with',
                               'x', 'y', 'z')
                          AND word !~ '^\d+$|\y\d+\-\d+\y|\y\d+\.\d+\y'
                        ORDER BY 1
         )
         GROUP BY concept_id, drug_name_original, concept_name
     ) dd
GROUP BY drug_name_original, ingredient_list;


-- name: 12_map_drugs_with_single_ingredient_to_ingredient_concepts
UPDATE drug_mapping_words c
SET update_method = 'single ingredient match',
    concept_name  = b.concept_name,
    concept_id    = b.concept_id
FROM (
         SELECT DISTINCT a.drug_name_original,
                         max(lower(b1.concept_name)) AS concept_name,
                         max(b1.concept_id)          AS concept_id
         FROM drug_mapping_single_ingredient_list a
                  INNER JOIN rxnorm_mapping_single_ingredient_list b1
                             ON a.ingredient_list = b1.ingredient_list
         GROUP BY a.drug_name_original
     ) b
WHERE c.drug_name_original = b.drug_name_original
  AND c.update_method IS NULL
  AND b.concept_name NOT IN
      ('vitamin a', 'sodium', 'hydrochloride', 'hcl', 'calcium', 'cold cream', 'vitamin b 12', 'maleate', 'tartrate',
       'mesylate', 'monohydrate', 'succinate', 'corn syrup', 'factor x', 'protein s')
  AND c.update_method IS NULL
  AND c.concept_id IS NULL;


-- name: 13_drop_brands_table
DROP TABLE IF EXISTS rxnorm_mapping_brand_name_list;
-- name: 14_create_brands_table
CREATE TABLE rxnorm_mapping_brand_name_list AS
SELECT ingredient_list, max(concept_id) AS concept_id, max(concept_name) AS concept_name
FROM (
         SELECT concept_id, concept_name, string_agg(word, ' ' ORDER BY word) AS ingredient_list
         FROM (
                  SELECT concept_id, concept_name, unnest(word_list::TEXT[]) AS word
                  FROM (
                           SELECT concept_id,
                                  concept_name,
                                  regexp_split_to_array(lower(concept_name),
                                                        E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') AS word_list
                           FROM (
                                    SELECT lower(concept_name) AS concept_name, concept_id
                                    FROM staging_vocabulary.concept b
                                    WHERE b.vocabulary_id = 'RxNorm'
                                      AND b.concept_class_id = 'Brand Name'
                                ) aa
                           ORDER BY concept_name DESC
                       ) bb
              ) cc
         WHERE word NOT IN ('')
           AND word NOT IN (SELECT DISTINCT unnest(regexp_split_to_array(lower(concept_name),
                                                                         E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                            FROM staging_vocabulary.concept b
                            WHERE b.vocabulary_id = 'RxNorm'
                              AND b.concept_class_id = 'Dose Form'
                            ORDER BY 1)
           AND word NOT IN ('', '-', ' ', 'A', 'AND', 'EX', '10A', '11A', '12F', '18C', '19F', '99M', 'G', 'G1', 'G2',
                            'G3', 'G4', 'H', 'I', 'IN', 'JELLY', 'LEAF', 'O', 'OF', 'OR', 'P', 'S', 'T', 'V', 'WITH',
                            'X', 'Y', 'Z')
           AND word !~ '^\d+$|\y\d+\-\d+\y|\y\d+\.\d+\y'
         GROUP BY concept_id, concept_name
     ) dd
GROUP BY ingredient_list;

-- name: 15_drop_source_brand_mapping_table
DROP TABLE IF EXISTS drug_mapping_brand_name_list;
-- name: 16_create_source_brand_mapping_table
CREATE TABLE drug_mapping_brand_name_list AS
SELECT drug_name_original, ingredient_list, max(concept_id) AS concept_id, max(concept_name) AS concept_name
FROM (
         SELECT concept_id, drug_name_original, concept_name, string_agg(word, ' ' ORDER BY word) AS ingredient_list
         FROM (
                  SELECT DISTINCT concept_id, drug_name_original, concept_name, unnest(word_list::TEXT[]) AS word
                  FROM (
                           SELECT concept_id,
                                  drug_name_original,
                                  concept_name,
                                  regexp_split_to_array(lower(drug_name_original),
                                                        E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') AS word_list
                           FROM (
                                    SELECT DISTINCT drugname              AS drug_name_original,
                                                    cast(NULL AS VARCHAR) AS concept_name,
                                                    cast(NULL AS INTEGER) AS concept_id,
                                                    NULL                  AS update_method
                                    FROM faers.drug a
                                             INNER JOIN faers.unique_all_case b ON a.primaryid = b.primaryid
                                    WHERE b.isr IS NULL
                                      AND drugname NOT LIKE '%/%'
                                      AND drugname NOT LIKE '% and %'
                                      AND drugname NOT LIKE '% with %'
                                      AND drugname NOT LIKE '%+%'
                                    UNION
                                    SELECT DISTINCT drugname              AS drug_name_original,
                                                    cast(NULL AS VARCHAR) AS concept_name,
                                                    cast(NULL AS INTEGER) AS concept_id,
                                                    NULL                  AS update_method
                                    FROM faers.drug_legacy a
                                             INNER JOIN faers.unique_all_case b ON cast(a.isr AS VARCHAR) = b.isr
                                    WHERE b.isr IS NOT NULL
                                      AND drugname NOT LIKE '%/%'
                                      AND drugname NOT LIKE '% and %'
                                      AND drugname NOT LIKE '% with %'
                                      AND drugname NOT LIKE '%+%'
                                ) aa
                           ORDER BY concept_name DESC
                       ) bb
              ) cc
         WHERE word NOT IN ('')
           AND word NOT IN (SELECT DISTINCT unnest(regexp_split_to_array(lower(concept_name),
                                                                         E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                            FROM staging_vocabulary.concept b
                            WHERE b.vocabulary_id = 'RxNorm'
                              AND b.concept_class_id = 'Dose Form'
                            ORDER BY 1)
           AND word IN (SELECT *
                        FROM (
                                 SELECT DISTINCT unnest(regexp_split_to_array(lower(concept_name),
                                                                              E'[\ \,\(\)\{\}\\\\/\^\%\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+')) AS word
                                 FROM staging_vocabulary.concept b
                                 WHERE b.vocabulary_id = 'RxNorm'
                                   AND b.concept_class_id IN ('Brand Name')
                             ) aa
                        WHERE word NOT IN
                              ('', '-', ' ', 'a', 'and', 'ex', '10a', '11a', '12f', '18c', '19f', '99m', 'g', 'g1',
                               'g2',
                               'g3', 'g4', 'h', 'i', 'in', 'jelly', 'leaf', 'o', 'of', 'or', 'p', 's', 't', 'v', 'with',
                               'x', 'y', 'z')
                          AND word !~ '^\d+$|\y\d+\-\d+\y|\y\d+\.\d+\y'
                        ORDER BY 1
         )
         GROUP BY concept_id, drug_name_original, concept_name
     ) dd
GROUP BY drug_name_original, ingredient_list;

-- name: 17_map_drugs_containing_brands
UPDATE drug_mapping_words c
SET update_method = 'brand name match',
    concept_name  = b.concept_name,
    concept_id    = b.concept_id
FROM (
         SELECT DISTINCT a.drug_name_original,
                         max(lower(b1.concept_name)) AS concept_name,
                         max(b1.concept_id)          AS concept_id
         FROM drug_mapping_brand_name_list a
                  INNER JOIN rxnorm_mapping_brand_name_list b1
                             ON a.ingredient_list = b1.ingredient_list
         GROUP BY a.drug_name_original
     ) b
WHERE c.drug_name_original = b.drug_name_original
  AND c.update_method IS NULL
  AND c.concept_id IS NULL
  AND b.concept_name NOT IN
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
FROM (
         SELECT DISTINCT drug_name_original, concept_name, concept_id, update_method
         FROM drug_mapping_words
         WHERE concept_id IS NOT NULL
     ) b
WHERE c.drug_name_original = b.drug_name_original
  AND c.concept_id IS NULL;
