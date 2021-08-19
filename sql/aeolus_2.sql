SET SEARCH_PATH = faers;

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
           AND word NOT IN (SELECT DISTINCT unnest(regexp_split_to_array(lower(str),
                                                                         E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                            FROM faers.rxnconso b
                            WHERE b.sab = 'RXNORM'
                              AND b.tty = 'DF'
                            ORDER BY 1)
           AND word IN (SELECT *
                        FROM (
                                 SELECT DISTINCT unnest(regexp_split_to_array(lower(str),
                                                                              E'[\ \,\(\)\{\}\\\\/\^\%\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+')) AS word
                                 FROM faers.rxnconso b
                                 WHERE b.sab = 'RXNORM'
                                   AND b.tty = 'IN'
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
                                    SELECT lower(str) AS concept_name, rxcui AS concept_id
                                    FROM faers.rxnconso b
                                    WHERE b.sab = 'RXNORM'
                                      AND b.tty = 'BN'
                                ) aa
                           ORDER BY concept_name DESC
                       ) bb
              ) cc
         WHERE word NOT IN ('')
           AND word NOT IN (SELECT DISTINCT unnest(regexp_split_to_array(lower(str),
                                                                         E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                            FROM faers.rxnconso b
                            WHERE b.sab = 'RXNORM'
                              AND b.tty = 'DF'
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
           AND word NOT IN (SELECT DISTINCT unnest(regexp_split_to_array(lower(str),
                                                                         E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
                            FROM faers.rxnconso b
                            WHERE b.sab = 'RXNORM'
                              AND b.tty = 'DF'
                            ORDER BY 1)
           AND word IN (SELECT *
                        FROM (
                                 SELECT DISTINCT unnest(regexp_split_to_array(lower(str),
                                                                              E'[\ \,\(\)\{\}\\\\/\^\%\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+')) AS word
                                 FROM faers.rxnconso b
                                 WHERE b.sab = 'RXNORM'
                                   AND b.tty = 'BN'
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


-- name: 18_update_big_mapping_table
UPDATE faers.drug_mapping_exact_java d
SET rx_ingredient = b.concept_name
FROM (
         SELECT DISTINCT drug_name_original, concept_name
         FROM drug_mapping_words
         WHERE concept_id IS NOT NULL
           AND update_method != 'brand name match'
     ) b
WHERE d.drugname = b.drug_name_original
  AND d.rx_ingredient IS NULL;

UPDATE faers.drug_mapping_exact_java d
SET rx_brand_name = b.concept_name
FROM (
         SELECT DISTINCT drug_name_original, concept_name
         FROM drug_mapping_words
         WHERE concept_id IS NOT NULL
           AND update_method = 'brand name match'
     ) b
WHERE d.drugname = b.drug_name_original
  AND d.rx_brand_name IS NULL;
