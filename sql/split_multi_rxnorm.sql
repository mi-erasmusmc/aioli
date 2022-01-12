SET SEARCH_PATH = faers;

-- MAKE THE IDS AUTO INCREMENT PROPERLY.
CREATE SEQUENCE faers.drug_mapping_exact_java_id_seq;

ALTER TABLE faers.drug_mapping_exact_java
    ALTER COLUMN id SET DEFAULT nextval('faers.drug_mapping_exact_java_id_seq'::regclass);

ALTER SEQUENCE faers.drug_mapping_exact_java_id_seq OWNED BY faers.drug_mapping_exact_java.id;

SELECT setval('faers.drug_mapping_exact_java_id_seq', (SELECT max(id) FROM faers.drug_mapping_exact_java));


-- CREATE INDEX
CREATE INDEX ON drug_mapping_exact_java (final_id);

-- SPLIT MULTI INGREDIENT DRUGS
INSERT INTO drug_mapping_exact_java (drugname, prod_ai, route, dose_amt, dose_form, dose_unit, nda_num, drugname_clean,
                                     prod_ai_clean, route_clean, dose_form_clean, temp_dose_form, dose_amt_clean,
                                     dose_unit_clean, rx_ingredient, rx_dose_form, rx_brand_name, rxcui, str, tty,
                                     atc_str, atc_code, atc_method, occurrences,
                                     final_id) (SELECT DISTINCT dme.drugname,
                                                                dme.prod_ai,
                                                                dme.route,
                                                                dme.dose_amt,
                                                                dme.dose_form,
                                                                dme.dose_unit,
                                                                dme.nda_num,
                                                                dme.drugname_clean,
                                                                dme.prod_ai_clean,
                                                                dme.route_clean,
                                                                dme.dose_form_clean,
                                                                dme.temp_dose_form,
                                                                dme.dose_amt_clean,
                                                                dme.dose_unit_clean,
                                                                dme.rx_ingredient,
                                                                dme.rx_dose_form,
                                                                dme.rx_brand_name,
                                                                dme.rxcui,
                                                                dme.str,
                                                                dme.tty,
                                                                dme.atc_str,
                                                                dme.atc_code,
                                                                dme.atc_method,
                                                                dme.occurrences,
                                                                r2.rxcui
                                                FROM drug_mapping_exact_java dme
                                                         JOIN rxnorm.rxnconso r ON dme.final_id = r.rxcui
                                                         JOIN rxnorm.rxnrel rel ON r.rxcui = rel.rxcui1
                                                         JOIN rxnorm.rxnconso r2 ON rel.rxcui2 = r2.rxcui
                                                WHERE r.tty = 'MIN'
                                                  AND r2.tty = 'IN'
                                                  AND r2.sab = 'RXNORM');

-- DUMP ORIGINAL MULTI INGREDIENT VALUES
WITH cte AS (SELECT DISTINCT final_id
             FROM drug_mapping_exact_java dme
                      JOIN rxnorm.rxnconso r ON dme.final_id = r.rxcui
             WHERE r.tty = 'MIN')
DELETE
FROM drug_mapping_exact_java
WHERE final_id IN (SELECT * FROM cte)