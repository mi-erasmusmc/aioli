SET SEARCH_PATH = faers;

ALTER TABLE drug_mapping_exact_java
    DROP COLUMN IF EXISTS final_id;

ALTER TABLE drug_mapping_exact_java
    ADD final_id INT;

-- Set the final id from the ingredient strings
WITH cte1 AS (SELECT DISTINCT lower(rx_ingredient) AS ing
              FROM drug_mapping_exact_java
              WHERE final_id IS NULL),
     cte2 AS (SELECT DISTINCT r.rxcui AS rxcui, cte1.ing
              FROM cte1
                       JOIN rxnconso r ON cte1.ing = lower(r.str)
                  AND r.tty IN ('IN', 'MIN')
                  AND r.sab = 'RXNORM')
UPDATE drug_mapping_exact_java dme
SET final_id = cte2.rxcui
FROM cte2
WHERE lower(dme.rx_ingredient) = cte2.ing;


-- Set final id from things that have been mapped exactly
WITH cte1 AS (SELECT DISTINCT rxcui FROM drug_mapping_exact_java WHERE final_id IS NULL)
        ,
     cte2 AS (
         SELECT DISTINCT cte1.rxcui, lower(string_agg(DISTINCT r_in.str, ' / ' ORDER BY r_in.str)) AS ingr
         FROM cte1
                  JOIN rxnrel rel ON rel.rxcui1 = cte1.rxcui
                  JOIN rxnconso r_in ON rel.rxcui2 = r_in.rxcui
             AND r_in.tty IN ('IN', 'MIN')
             AND r_in.sab = 'RXNORM'
         GROUP BY cte1.rxcui
     )
        ,
     cte3 AS (
         SELECT DISTINCT cte2.rxcui AS rxcui, r.rxcui AS final_id
         FROM rxnconso r
                  JOIN cte2 ON lower(r.str) = cte2.ingr AND r.tty IN ('IN', 'MIN')
             AND r.sab = 'RXNORM'
     )
UPDATE drug_mapping_exact_java dme
SET final_id = cte3.final_id
FROM cte3
WHERE dme.rxcui = cte3.rxcui
  AND dme.final_id IS NULL;


-- Overly complex stuff to catch 50 or so edge cases....
WITH cte1 AS (SELECT DISTINCT rxcui FROM drug_mapping_exact_java WHERE final_id IS NULL),
     cte2 AS (SELECT DISTINCT cte1.rxcui AS original, r2.rxcui AS rxcui
              FROM cte1
                       JOIN rxnrel rel ON cte1.rxcui = rel.rxcui1
                       JOIN rxnconso r_in ON rel.rxcui2 = r_in.rxcui
                       JOIN rxnrel rel2 ON rel.rxcui2 = rel2.rxcui1
                       JOIN rxnconso r2 ON r2.rxcui = rel2.rxcui2
                  AND r_in.tty = 'SBDF'
                  AND r_in.sab = 'RXNORM'
                  AND r2.tty = 'SCDF'),
     cte3 AS (SELECT DISTINCT cte2.original                                                 AS rxcui,
                              lower(string_agg(DISTINCT r_in.str, ' / ' ORDER BY r_in.str)) AS ingr
              FROM cte2
                       JOIN rxnrel rel ON rel.rxcui1 = cte2.rxcui
                       JOIN rxnconso r_in ON rel.rxcui2 = r_in.rxcui
                  AND r_in.tty IN ('IN', 'MIN')
                  AND r_in.sab = 'RXNORM'
              GROUP BY cte2.original),
     cte4 AS (SELECT DISTINCT cte3.rxcui AS rxcui, r.rxcui AS final_id
              FROM rxnconso r
                       JOIN cte3 ON lower(r.str) = cte3.ingr AND r.tty IN ('IN', 'MIN')
                  AND r.sab = 'RXNORM')
UPDATE drug_mapping_exact_java dme
SET final_id = cte4.final_id
FROM cte4
WHERE dme.rxcui = cte4.rxcui
  AND dme.final_id IS NULL;

-- Overly complex stuff to catch 50 or so edge cases....
WITH cte1 AS (SELECT DISTINCT rxcui FROM drug_mapping_exact_java WHERE final_id IS NULL),
     cte2 AS (SELECT DISTINCT cte1.rxcui AS original, r_in.rxcui AS rxcui
              FROM cte1
                       JOIN rxnrel rel ON cte1.rxcui = rel.rxcui1
                       JOIN rxnconso r_in ON rel.rxcui2 = r_in.rxcui
                  AND r_in.tty = 'SCDF'
                  AND r_in.sab = 'RXNORM'),
     cte3 AS (SELECT DISTINCT cte2.original                                                 AS rxcui,
                              lower(string_agg(DISTINCT r_in.str, ' / ' ORDER BY r_in.str)) AS ingr
              FROM cte2
                       JOIN rxnrel rel ON rel.rxcui1 = cte2.rxcui
                       JOIN rxnconso r_in ON rel.rxcui2 = r_in.rxcui
                  AND r_in.tty IN ('IN', 'MIN')
                  AND r_in.sab = 'RXNORM'
              GROUP BY cte2.original),
     cte4 AS (SELECT DISTINCT cte3.rxcui AS rxcui, r.rxcui AS final_id
              FROM rxnconso r
                       JOIN cte3 ON lower(r.str) = cte3.ingr AND r.tty IN ('IN', 'MIN')
                  AND r.sab = 'RXNORM')
UPDATE drug_mapping_exact_java dme
SET final_id = cte4.final_id
FROM cte4
WHERE dme.rxcui = cte4.rxcui
  AND dme.final_id IS NULL;

