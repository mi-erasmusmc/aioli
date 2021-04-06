-- name: remove_unknown
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '\((\ \yunknown|unk\y)\)|\(\y(unknown|unk)\y\)|\y(unknown|unk)\y',
                                     '', 'gi')
where drug_name_clean ~* '.+\yunknown|unk\y.+$'
  and rxcui is null;

-- name: remove_unknown_residue
update faers.drug_mapping
set drug_name_clean = null
where drug_name_clean = 'ingredient'
   or drug_name_clean = 'product'
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

-- name: trim_semicolon
update faers.drug_mapping
set drug_name_clean = trim(both ';' from drug_name_clean)
where rxcui is null
  and (drug_name_clean like '%;' or drug_name_clean like ';%');

-- name: trim_colon
update faers.drug_mapping
set drug_name_clean = trim(both ':' from drug_name_clean)
where rxcui is null
  and (drug_name_clean like '%:' or drug_name_clean like ':%');

-- name: trim_spaces
update faers.drug_mapping
set drug_name_clean = trim(both ' ' from drug_name_clean)
where rxcui is null
  and (drug_name_clean like '% ' or drug_name_clean like ' %');

-- name: trim_slash
update faers.drug_mapping
set drug_name_clean = trim(both '/' from drug_name_clean)
where rxcui is null
  and (drug_name_clean like '%/' or drug_name_clean like '/%');

-- name: trim_hyphen
update faers.drug_mapping
set drug_name_clean = trim(both '-' from drug_name_clean)
where (drug_name_clean like '%-' or drug_name_clean like '-%')
  and rxcui is null;


-- name: remove_quotes
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '[''""]', '', 'gi')
where rxcui is null;

-- name: reverse_dash
update faers.drug_mapping
set drug_name_clean = replace(drug_name_clean, '\', ' / ')
where drug_name_clean like '%\\%';


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

-- name: add_space_parenthesis
update faers.drug_mapping
set drug_name_clean = replace(drug_name_clean, '(', ' (')
where drug_name_clean like '%(%'
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

-- name: replace_w
update faers.drug_mapping
set drug_name_clean = replace(drug_name_clean, ' w/', ' / ')
where drug_name_clean like '% w/%'
  and drug_name_clean not like '% w/v %'
  and rxcui is null;

-- name: add_space_to_slash
update faers.drug_mapping
set drug_name_clean = replace(drug_name_clean, '/', ' / ')
where drug_name_clean like '%/%'
  and drug_name_clean not like '%meq/l%'
  and drug_name_clean not like '%mg/ml%'
  and rxcui is null;

-- name: remove_nos
update faers.drug_mapping
set drug_name_clean = replace(drug_name_clean, ' nos ', ' ')
where drug_name_clean like '% nos %'
  and rxcui is null;

-- name: remove_nos_ending
update faers.drug_mapping
set drug_name_clean = replace(drug_name_clean, ' nos', '')
where drug_name_clean like '% nos'
  and rxcui is null;

-- name: remove_product
update faers.drug_mapping
set drug_name_clean = replace(drug_name_clean, ' product', '')
where drug_name_clean like '% product'
   or drug_name_clean like '% product %'
    and rxcui is null;

-- name: remove_unspecified
update faers.drug_mapping
set drug_name_clean = replace(drug_name_clean, 'unspecified', '')
where drug_name_clean like '%unspecified%'
  and rxcui is null;

-- name: replace_hooks_1
update faers.drug_mapping
set drug_name_clean = replace(drug_name_clean, '[', '(')
where drug_name_clean like '%[%'
  and rxcui is null;

-- name: replace_hooks_2
update faers.drug_mapping
set drug_name_clean = replace(drug_name_clean, ']', ')')
where drug_name_clean like '%]%'
  and rxcui is null;

-- name: replace_semi_colon
update faers.drug_mapping
set drug_name_clean = replace(drug_name_clean, ';', '/')
where drug_name_clean like '%;%'
  and rxcui is null;

-- name: remove_product
update faers.drug_mapping
set drug_name_clean = replace(drug_name_clean, '/ /', '/')
where drug_name_clean like '%/ /%'
  and rxcui is null;

-- name: remove_nos_parenth
update faers.drug_mapping
set drug_name_clean = replace(drug_name_clean, ' (nos)', '')
where drug_name_clean like '% (nos)'
  and rxcui is null;

-- name: remove_plus
update faers.drug_mapping
set drug_name_clean = replace(drug_name_clean, '(+)', ' / ')
where drug_name_clean like '%(+)%'
  and rxcui is null;


-- name: remove_leading_chars
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '^ +', '', 'gi')
where rxcui is null;


-- name: remove_whitespace
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '(\S) +', '\1 ', 'gi')
where rxcui is null;

-- name: remove_remove_nnnnn
update faers.drug_mapping a
SET drug_name_clean = regexp_replace(drug_name_clean, '\/ \d+\ /\ *', '', 'gi')
where rxcui is null
  and drug_name_clean ~* '.*\/ \d+\ /.*';

-- name: remove_remove_nnn
update faers.drug_mapping a
SET drug_name_clean = regexp_replace(drug_name_clean, '\/\d+\/\ *', '', 'gi')
where rxcui is null
  and drug_name_clean ~* '.*\/\d+\/.*';

-- name: remove_empty_parenthesis
update faers.drug_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '\(\)', '', 'gi')
where drug_name_clean like '%()%'
  and rxcui is null;