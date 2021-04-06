create table faers.joiner_c as (SELECT DISTINCT a.primaryid,
                                                cast(null as int) as isr,
                                                drug_seq,
                                                role_cod,
                                                drm.str_final     AS rx_str,
                                                drm.rxcui_final   AS rxcui,
                                                drm.atc_final     AS atc_code,
                                                drm.atc_method    AS atc_method
                                FROM faers.drug a
                                         JOIN faers.drug_mapping_exact drm ON
                                        coalesce(upper(a.drugname), 'a') = coalesce(drm.drugname, 'a') and
                                        coalesce(upper(a.prod_ai), 'a') = coalesce(drm.prod_ai, 'a') and
                                        coalesce(lower(a.route), 'a') = coalesce(drm.route, 'a') and
                                        coalesce(lower(a.dose_amt), 'a') = coalesce(drm.dose_amt, 'a') and
                                        coalesce(lower(a.dose_form), 'a') = coalesce(drm.dose_form, 'a') and
                                        coalesce(lower(a.dose_unit), 'a') = coalesce(drm.dose_unit, 'a') and
                                        coalesce(lower(a.nda_num), 'a') = coalesce(drm.nda_num, 'a') and
                                        coalesce(lower(a.drugname), 'a') = coalesce(drm.drugname_clean, 'a') and
                                        coalesce(lower(a.prod_ai), 'a') = coalesce(drm.prod_ai_clean, 'a')
                                WHERE drm.atc_final is not null);


create table faers.joiner_l as (SELECT DISTINCT a.isr,
                                                drug_seq,
                                                role_cod,
                                                drm.str_final   AS rx_str,
                                                drm.rxcui_final AS rxcui,
                                                drm.atc_final   AS atc_code,
                                                drm.atc_method  AS atc_method
                                FROM faers.drug_legacy a
                                         JOIN faers.drug_mapping_exact drm ON
                                        coalesce(upper(a.drugname), 'a') = coalesce(drm.drugname, 'a') and
                                        coalesce(lower(a.route), 'a') = coalesce(drm.route, 'a') and
                                        coalesce(cast(a.nda_num as text), 'a') = coalesce(drm.nda_num, 'a') and
                                        coalesce(lower(a.drugname), 'a') = coalesce(drm.drugname_clean, 'a')
                                WHERE drm.atc_final is not null);