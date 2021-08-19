SET SEARCH_PATH = faers;

WITH cte1 AS (SELECT DISTINCT drugname_clean
              FROM drug_mapping_exact_java
              WHERE rxcui IS NULL
                AND drugname_clean LIKE '% %'
                AND rx_brand_name IS NULL),
     cte2 AS (
         SELECT cte1.drugname_clean, string_agg(DISTINCT lower(r2.str), ',') AS brand_name
         FROM cte1
                  JOIN rxnorm.rxnconso r ON lower(r.str) = ANY (string_to_array(drugname_clean, ' '))
                  JOIN rxnorm.rxnconso r2 ON r.rxcui = r2.rxcui
         WHERE r2.tty = 'BN'
           AND r2.sab = 'RXNORM'
           AND array_length(string_to_array(r2.str, ' '), 1) = 1
         GROUP BY cte1.drugname_clean
         HAVING count(DISTINCT lower(r2.str)) = 1
     )
UPDATE drug_mapping_exact_java m
SET rx_brand_name = cte2.brand_name
FROM cte2
WHERE cte2.drugname_clean = m.drugname_clean
  AND m.rx_brand_name IS NULL;