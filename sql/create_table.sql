SET SEARCH_PATH = faers;

-- name: drop_table
DROP TABLE IF EXISTS drug_mapping_exact_java;

-- name: create_table
CREATE TABLE drug_mapping_exact_java AS
SELECT row_number() OVER () AS id,
       drugname,
       prod_ai,
       route,
       dose_amt,
       dose_form,
       dose_unit,
       nda_num,
       drugname_clean,
       prod_ai_clean,
       route_clean,
       dose_form_clean,
       temp_dose_form,
       dose_amt_clean,
       dose_unit_clean,
       rx_ingredient,
       rx_dose_form,
       rx_brand_name,
       rxcui,
       str,
       tty,
       atc_str,
       atc_code,
       atc_method,
       sum(occurrences)     AS occurrences
FROM (
         SELECT upper(drugname)                         AS drugname,
                upper(prod_ai)                          AS prod_ai,
                lower(route)                            AS route,
                lower(dose_amt)                         AS dose_amt,
                lower(dose_form)                        AS dose_form,
                lower(dose_unit)                        AS dose_unit,
                lower(nda_num)                          AS nda_num,
                lower(drugname)                         AS drugname_clean,
                lower(prod_ai)                          AS prod_ai_clean,
                NULL                                    AS route_clean,
                NULL                                    AS dose_form_clean,
                NULL                                    AS temp_dose_form,
                trim(TRAILING '.' FROM lower(dose_amt)) AS dose_amt_clean,
                lower(dose_unit)                        AS dose_unit_clean,
                NULL                                    AS rx_ingredient,
                NULL                                    AS rx_dose_form,
                NULL                                    AS rx_brand_name,
                cast(NULL AS INT)                       AS rxcui,
                NULL                                    AS str,
                NULL                                    AS tty,
                NULL                                    AS atc_str,
                NULL                                    AS atc_code,
                NULL                                    AS atc_method,
                count(*)                                AS occurrences
         FROM drug
         GROUP BY lower(route), upper(prod_ai), upper(drugname), lower(dose_amt), lower(dose_form), lower(dose_unit),
                  lower(nda_num), lower(drugname), lower(prod_ai), trim(TRAILING '.' FROM lower(dose_amt)),
                  lower(dose_unit)
         UNION
         SELECT upper(drugname)       AS drugname,
                NULL                  AS prod_ai,
                lower(route)          AS route,
                NULL                  AS dose_amt,
                NULL                  AS dose_form,
                NULL                  AS dose_unit,
                cast(nda_num AS TEXT) AS nda_num,
                lower(drugname)       AS drugname_clean,
                NULL                  AS prod_ai_clean,
                NULL                  AS route_clean,
                NULL                  AS dose_form_clean,
                NULL                  AS temp_dose_form,
                NULL                  AS dose_amt_clean,
                NULL                  AS dose_unit_clean,
                NULL                  AS rx_ingredient,
                NULL                  AS rx_dose_form,
                NULL                  AS rx_brand_name,
                cast(NULL AS INT)     AS rxcui,
                NULL                  AS str,
                NULL                  AS tty,
                NULL                  AS atc_str,
                NULL                  AS atc_code,
                NULL                  AS atc_method,
                count(*)              AS occurrences
         FROM drug_legacy
         GROUP BY upper(drugname), lower(route), cast(nda_num AS TEXT), lower(drugname)) AS unified
GROUP BY drugname, prod_ai, route, dose_amt, dose_form, dose_unit, nda_num, drugname_clean, prod_ai_clean, route_clean,
         dose_form_clean, temp_dose_form, dose_amt_clean, dose_unit_clean, rx_ingredient, rx_dose_form, rx_brand_name,
         rxcui, str, tty, atc_code, atc_method, atc_str;

ALTER TABLE drug_mapping_exact_java
    ADD CONSTRAINT drug_mapping_exact_java_pk
        PRIMARY KEY (id);

CREATE UNIQUE INDEX drug_mapping_exact_java_id_uindex
    ON drug_mapping_exact_java (id);

CREATE INDEX drug_mapping_exact_java_drugname_index
    ON drug_mapping_exact_java (drugname);

CREATE INDEX drug_mapping_exact_java_drugname_clean_index
    ON drug_mapping_exact_java (drugname_clean);

CREATE INDEX drug_mapping_exact_java_prod_ai_clean_index
    ON drug_mapping_exact_java (prod_ai_clean);

CREATE INDEX drug_mapping_exact_java_rx_brand_name_index
    ON drug_mapping_exact_java (rx_brand_name);

CREATE INDEX drug_mapping_exact_java_rx_dose_form
    ON drug_mapping_exact_java (rx_dose_form);


-- Special index stuffss....
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX drug_mapping_exact_java_drugname_clean_index_2
    ON drug_mapping_exact_java USING gin (drugname_clean gin_trgm_ops);

CREATE INDEX drug_mapping_exact_java_rx_ingredient_index_2
    ON drug_mapping_exact_java USING gin (rx_ingredient gin_trgm_ops);



