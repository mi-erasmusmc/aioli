SET SEARCH_PATH = faers;

-- Convert precise ingredients to ingredients
WITH cte1 AS (SELECT DISTINCT rx_ingredient FROM drug_mapping_exact_java),
     cte2 AS (SELECT DISTINCT cte1.rx_ingredient AS old, r_in.str AS new
              FROM cte1
                       JOIN rxnorm.rxnconso r_pin ON cte1.rx_ingredient = lower(r_pin.str)
                       JOIN rxnorm.rxnrel rel ON rel.rxcui1 = r_pin.rxcui
                       JOIN rxnorm.rxnconso r_in ON rel.rxcui2 = r_in.rxcui
              WHERE r_pin.tty = 'PIN'
                AND r_pin.sab = 'RXNORM'
                AND r_in.tty = 'IN'
                AND r_in.sab = 'RXNORM')
UPDATE drug_mapping_exact_java dme
SET rx_ingredient = cte2.new
FROM cte2
WHERE dme.rx_ingredient = cte2.old;

-- Set ingredient where only brand name is available
WITH cte1 AS (SELECT DISTINCT rx_brand_name FROM drug_mapping_exact_java WHERE rx_ingredient IS NULL),
     cte2 AS (
         SELECT DISTINCT cte1.rx_brand_name, lower(string_agg(DISTINCT r_in.str, ' / ' ORDER BY r_in.str)) AS ingr
         FROM cte1
                  JOIN rxnorm.rxnconso r_pin ON cte1.rx_brand_name = lower(r_pin.str)
                  JOIN rxnorm.rxnrel rel ON rel.rxcui1 = r_pin.rxcui
                  JOIN rxnorm.rxnconso r_in ON rel.rxcui2 = r_in.rxcui
         WHERE r_pin.tty = 'BN'
           AND r_pin.sab = 'RXNORM'
           AND r_in.tty IN ('IN')
           AND r_in.sab = 'RXNORM'
         GROUP BY cte1.rx_brand_name
     ),
     cte3 AS (SELECT DISTINCT cte2.rx_brand_name AS bn, lower(r.str) AS ingredient
              FROM rxnorm.rxnconso r
                       JOIN cte2 ON lower(r.str) = cte2.ingr AND r.tty IN ('IN', 'MIN')
                  AND r.sab = 'RXNORM'
     )
UPDATE drug_mapping_exact_java dme
SET rx_ingredient = lower(cte3.ingredient)
FROM cte3
WHERE dme.rx_brand_name = cte3.bn;