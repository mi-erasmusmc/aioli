-- name: drop_table
DROP TABLE IF EXISTS faers.drug_mapping_exact;


-- name: create_table
CREATE TABLE faers.drug_mapping_exact AS
SELECT row_number() over () as id,
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
       sum(occurrences) as occurrences
FROM (
         SELECT upper(drugname)                         as drugname,
                upper(prod_ai)                          as prod_ai,
                lower(route)                            as route,
                lower(dose_amt)                         as dose_amt,
                lower(dose_form)                        as dose_form,
                lower(dose_unit)                        as dose_unit,
                lower(nda_num)                          as nda_num,
                lower(drugname)                         as drugname_clean,
                lower(prod_ai)                          as prod_ai_clean,
                null                                    as route_clean,
                null                                    as dose_form_clean,
                null                                    as temp_dose_form,
                trim(trailing '.' from lower(dose_amt)) as dose_amt_clean,
                lower(dose_unit)                        as dose_unit_clean,
                null                                    as rx_ingredient,
                null                                    as rx_dose_form,
                null                                    as rx_brand_name,
                null                                    as rxcui,
                null                                    as str,
                null                                    as tty,
                count(*)                                as occurrences
         FROM faers.drug
         GROUP BY lower(route), upper(prod_ai), upper(drugname), lower(dose_amt), lower(dose_form), lower(dose_unit),
                  lower(nda_num), lower(drugname), lower(prod_ai), trim(trailing '.' from lower(dose_amt)),
                  lower(dose_unit)
         UNION
         SELECT upper(drugname)       as drugname,
                NULL                  as prod_ai,
                lower(route)          as route,
                NULL                  as dose_amt,
                NULL                  as dose_form,
                NULL                  as dose_unit,
                cast(nda_num as text) as nda_num,
                lower(drugname)       as drugname_clean,
                NULL                  as prod_ai_clean,
                null                  as route_clean,
                null                  as dose_form_clean,
                null                  as temp_dose_form,
                NULL                  as dose_amt_clean,
                NULL                  as dose_unit_clean,
                null                  as rx_ingredient,
                null                  as rx_dose_form,
                null                  as rx_brand_name,
                null                  as rxcui,
                null                  as str,
                null                  as tty,
                count(*)              as occurrences
         FROM faers.drug_legacy
         GROUP BY upper(drugname), lower(route), cast(nda_num as text), lower(drugname)) as unified
group by drugname, prod_ai, route, dose_amt, dose_form, dose_unit, nda_num, drugname_clean, prod_ai_clean, route_clean,
         dose_form_clean, temp_dose_form, dose_amt_clean, dose_unit_clean, rx_ingredient, rx_dose_form, rx_brand_name,
         rxcui, str, tty;
