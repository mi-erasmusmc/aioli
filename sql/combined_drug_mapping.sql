SET SEARCH_PATH = faers;

-- name: drop_standard_case_drug
DROP TABLE IF EXISTS standard_case_drug_java;


-- name: create the big final table
CREATE TABLE standard_case_drug_java AS
    (SELECT a.primaryid,
            NULL         AS isr,
            drug_seq,
            role_cod,
            drm.final_id AS standard_concept_id
     FROM faers.drug a
              JOIN faers.drug_mapping_exact_java drm ON
                 coalesce(upper(a.drugname), 'a') =
                 coalesce(drm.drugname, 'a') AND
                 coalesce(upper(a.prod_ai), 'a') =
                 coalesce(drm.prod_ai, 'a') AND
                 coalesce(lower(a.route), 'a') =
                 coalesce(drm.route, 'a') AND
                 coalesce(lower(a.dose_amt), 'a') =
                 coalesce(drm.dose_amt, 'a') AND
                 coalesce(lower(a.dose_form), 'a') =
                 coalesce(drm.dose_form, 'a') AND
                 coalesce(lower(a.dose_unit), 'a') =
                 coalesce(drm.dose_unit, 'a') AND
                 coalesce(lower(a.nda_num), 'a') =
                 coalesce(drm.nda_num, 'a')
     WHERE drm.final_id IS NOT NULL
     UNION ALL
     SELECT NULL         AS primaryid,
            a.isr,
            cast(drug_seq AS VARCHAR),
            role_cod,
            drm.final_id AS standard_concept_id
     FROM faers.drug_legacy a
              JOIN faers.drug_mapping_exact_java drm ON
                 coalesce(upper(a.drugname), 'a') =
                 coalesce(drm.drugname, 'a') AND
                 coalesce(lower(a.route), 'a') =
                 coalesce(drm.route, 'a') AND
                 coalesce(cast(a.nda_num AS TEXT), 'a') =
                 coalesce(drm.nda_num, 'a')
     WHERE drm.final_id IS NOT NULL
       AND prod_ai IS NULL
       AND dose_amt IS NULL
       AND dose_form IS NULL
       AND dose_unit IS NULL);