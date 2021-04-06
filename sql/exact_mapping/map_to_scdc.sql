-- name: exact
with cte1 as (select string_agg(distinct cast(r.rxcui as text), ',') as rxcui,
                     m.id
              from faers.rxnconso r
                       join
                   faers.drug_mapping_exact m
                   on lower(r.str) =
                      lower(concat(m.rx_ingredient, ' ', m.dose_amt_clean, ' ', m.dose_unit_clean))
              where m.rx_ingredient is not null
                and m.dose_unit_clean is not null
                and m.dose_amt_clean is not null
                and m.rxcui is null
                and r.tty = 'SCDC'
                and r.sab = 'RXNORM'
                and r.suppress != 'O'
              group by m.id, r.str
              having count(distinct r.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte1.rxcui,
    tty   = 'SCDC'
from cte1
where cte1.id = m.id;

-- name: drug_clean
with cte1 as (select distinct drugname_clean,
                              string_agg(distinct cast(r2.rxcui as text), ',') as cui
              from faers.drug_mapping_exact m
                       join faers.rxnconso r on lower(m.drugname_clean) = lower(r.str)
                       join faers.rxnconso r2 on r.rxcui = r2.rxcui
              where r2.tty = 'SCDC'
                and r2.sab = 'RXNORM'
                and m.rxcui is null
              group by drugname_clean, rx_dose_form
              having count(distinct r2.rxcui) = 1)
update faers.drug_mapping_exact m
set rxcui = cte1.cui,
    tty   = 'SCDC'
from cte1
where cte1.drugname_clean = m.drugname_clean
  and rxcui is null;
