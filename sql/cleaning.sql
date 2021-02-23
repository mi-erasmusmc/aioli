-- name: remove_unknown
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '\((\ \yunknown|unk\y)\)|\(\y(unknown|unk)\y\)|\y(unknown|unk)\y',
                                     '', 'gi')
where drug_name_clean ~* '.+\y(unknown|unk)\y.+$'
  and rxcui is null;

-- name: remove_blinded
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, ' *blinded *', '', 'gi')
where drug_name_clean like '%blinded%'
  and rxcui is null;

-- name: change_chars
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '\\', '/', 'gi')
where drug_name_clean like '%\%'
  and rxcui is null;

-- name: remove_chars
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '[\*\^\$\?]', '', 'gi')
where rxcui is null;

-- name: remove_trailing_hyphen
update faers.drug_mapping
set drug_name_clean = trim(trailing '-' from drug_name_clean)
where drug_name_clean like '%-'
  and rxcui is null;


-- name: remove_quotes
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '[''""]', '', 'gi')
where rxcui is null;


-- name: remove_spaces_before_closing_parenthesis_char
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, ' +\)', ')', 'gi')
where drug_name_clean like '% )%'
  and rxcui is null;


-- name: remove_trailing_chars
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '[ \.\,]$', '', 'gi')
where (drug_name_clean like '%.'
    or drug_name_clean like '%,'
    or drug_name_clean like '% ')
  and rxcui is null;

-- name: remove_white
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '(\S) +', '\1 ', 'gi')
where drug_name_clean like '%  %'
  and rxcui is null;


-- name: remove_trailing_spaces
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, ' +$', '', 'gi')
where drug_name_clean like '% '
  and rxcui is null;

-- name: remove_leading_spaces
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '^ +', '', 'gi')
where drug_name_clean like ' %'
  and rxcui is null;


-- name: remove_leading_chars
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '^ +', '', 'gi')
where rxcui is null;


-- name: remove_whitespace
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '(\S) +', '\1 ', 'gi')
where rxcui is null;

-- name: remove_remove_nnnn
update faers.drug_mapping a
SET drug_name_clean = regexp_replace(drug_name_clean, '\/\d+\/\ *', '', 'gi')
where rxcui is null
  and drug_name_original ~* '.*\/\d+\/.*';

-- name: remove_empty_parenthesis
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '\(\)', '', 'gi')
where drug_name_clean like '%()%'
  and rxcui is null;