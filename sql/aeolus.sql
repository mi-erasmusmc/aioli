SET SEARCH_PATH = faers;

-- name: 1_drop_table_words
DROP TABLE IF EXISTS faers.drug_mapping_words;
-- name: 2_create_table_words
CREATE TABLE faers.drug_mapping_words AS
WITH cte AS (SELECT DISTINCT unnest(regexp_split_to_array(lower(str),
                                                          E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+')) AS df
             FROM cem.faers.rxnconso b
             WHERE b.sab = 'RXNORM'
               AND b.tty = 'DF'
             ORDER BY 1)
SELECT DISTINCT *
FROM (
         SELECT drug_name_original, concept_name, concept_id, update_method, unnest(word_list::TEXT[]) AS word
         FROM (
                  SELECT drug_name_original,
                         concept_name,
                         concept_id,
                         update_method,
                         regexp_split_to_array(lower(drug_name_original),
                                               E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=\|]+') AS word_list
                  FROM (
                           SELECT DISTINCT drugname              AS drug_name_original,
                                           cast(NULL AS VARCHAR) AS concept_name,
                                           cast(NULL AS INTEGER) AS concept_id,
                                           NULL                  AS update_method
                           FROM faers.drug_mapping_exact_java a
                           WHERE rx_ingredient IS NULL
                              OR rx_brand_name IS NULL
                       ) aa
                  ORDER BY drug_name_original DESC
              ) bb
     ) cc
WHERE word NOT IN ('', 'syrup', 'hcl', 'hydrochloride', 'acetic', 'sodium', 'calcium', 'sulphate', 'monohydrate')
  AND word NOT IN (SELECT df FROM cte);


-- name: 3_drop_table_multi_ingredient
DROP TABLE IF EXISTS rxnorm_mapping_multi_ingredient_list;
-- name: 4_create_multi_ingredient_table
CREATE TABLE rxnorm_mapping_multi_ingredient_list AS
WITH cte AS (SELECT DISTINCT unnest(regexp_split_to_array(lower(str),
                                                          E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+')) AS df
             FROM cem.faers.rxnconso b
             WHERE b.sab = 'RXNORM'
               AND b.tty = 'DF'
             ORDER BY 1)
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
                                    FROM cem.faers.rxnconso b
                                    WHERE b.sab = 'RXNORM'
                                      AND b.tty = 'MIN'
                                ) aa
                           ORDER BY concept_name DESC
                       ) bb
              ) cc
         WHERE word NOT IN ('')
           AND word NOT IN (SELECT df FROM cte)
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
WITH cte AS (SELECT DISTINCT unnest(regexp_split_to_array(lower(str),
                                                          E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+')) AS df
             FROM cem.faers.rxnconso b
             WHERE b.sab = 'RXNORM'
               AND b.tty = 'DF'
             ORDER BY 1)
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
                                    FROM faers.drug_mapping_exact_java a
                                    WHERE rx_ingredient IS NULL
                                       OR rx_brand_name IS NULL
                                ) aa
                           ORDER BY concept_name DESC
                       ) bb
              ) cc
         WHERE word NOT IN ('')
           AND word NOT IN (SELECT df FROM cte)
           AND word IN (SELECT *
                        FROM (
                                 SELECT DISTINCT unnest(regexp_split_to_array(lower(str),
                                                                              E'[\ \,\(\)\{\}\\\\/\^\%\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+')) AS word
                                 FROM faers.rxnconso b
                                 WHERE b.sab = 'RXNORM'
                                   AND b.tty = 'MIN'
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
                                    SELECT lower(str) AS concept_name, rxcui AS concept_id
                                    FROM faers.rxnconso b
                                    WHERE b.sab = 'RXNORM'
                                      AND b.tty = 'IN'
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
           AND word NOT IN ('', '-', ' ', 'a', 'and', 'ex', '10a', '11a', '12f', '18c', '19f', '99m', 'g', 'g1', 'g2',
                            'g3', 'g4', 'h', 'i', 'in', 'jelly', 'leaf', 'o', 'of', 'or', 'p', 's', 't', 'v', 'with',
                            'x', 'y', 'z')
           AND word !~ '^\d+$|\y\d+\-\d+\y|\y\d+\.\d+\y'
         GROUP BY concept_id, concept_name
     ) dd
GROUP BY ingredient_list;