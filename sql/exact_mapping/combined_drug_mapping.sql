CREATE TABLE faers.joiner_c AS (SELECT DISTINCT a.primaryid,
                                                cast(NULL AS INT) AS isr,
                                                drug_seq,
                                                role_cod,
                                                drm.atc_str       AS rx_str,
                                                drm.rxcui_final   AS rxcui,
                                                drm.atc_code      AS atc_code,
                                                drm.atc_method    AS atc_method
                                FROM faers.drug a
                                         JOIN faers.drug_mapping_exact drm ON
                                        coalesce(upper(a.drugname), 'a') = coalesce(drm.drugname, 'a') AND
                                        coalesce(upper(a.prod_ai), 'a') = coalesce(drm.prod_ai, 'a') AND
                                        coalesce(lower(a.route), 'a') = coalesce(drm.route, 'a') AND
                                        coalesce(lower(a.dose_amt), 'a') = coalesce(drm.dose_amt, 'a') AND
                                        coalesce(lower(a.dose_form), 'a') = coalesce(drm.dose_form, 'a') AND
                                        coalesce(lower(a.dose_unit), 'a') = coalesce(drm.dose_unit, 'a') AND
                                        coalesce(lower(a.nda_num), 'a') = coalesce(drm.nda_num, 'a') AND
                                        coalesce(lower(a.drugname), 'a') = coalesce(drm.drugname_clean, 'a') AND
                                        coalesce(lower(a.prod_ai), 'a') = coalesce(drm.prod_ai_clean, 'a')
                                WHERE drm.atc_code IS NOT NULL);


CREATE TABLE faers.joiner_l AS (SELECT DISTINCT a.isr,
                                                drug_seq,
                                                role_cod,
                                                drm.atc_str     AS rx_str,
                                                drm.rxcui_final AS rxcui,
                                                drm.atc_code    AS atc_code,
                                                drm.atc_method  AS atc_method
                                FROM faers.drug_legacy a
                                         JOIN faers.drug_mapping_exact drm ON
                                        coalesce(upper(a.drugname), 'a') = coalesce(drm.drugname, 'a') AND
                                        coalesce(lower(a.route), 'a') = coalesce(drm.route, 'a') AND
                                        coalesce(cast(a.nda_num AS TEXT), 'a') = coalesce(drm.nda_num, 'a') AND
                                        coalesce(lower(a.drugname), 'a') = coalesce(drm.drugname_clean, 'a')
                                WHERE drm.atc_code IS NOT NULL);