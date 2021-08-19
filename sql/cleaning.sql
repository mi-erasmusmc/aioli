SET SEARCH_PATH = faers;

-- name: remove_remove_nnnnn
UPDATE faers.drug_mapping_exact_java a
SET drugname_clean = regexp_replace(drugname_clean, '\/ \d+\ /\ *', '', 'gi')
WHERE rxcui IS NULL
  AND drugname_clean ~* '.*\/ \d+\ /.*';




-- name: remove_remove_nnn
UPDATE faers.drug_mapping_exact_java a
SET drugname_clean = regexp_replace(drugname_clean, '\/\d+\/\ *', '', 'gi')
WHERE rxcui IS NULL
  AND drugname_clean ~* '.*\/\d+\/.*';




-- name: remove_unknown
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = regexp_replace(drugname_clean, '\((\ \yunknown|unk\y)\)|\(\y(unknown|unk)\y\)|\y(unknown|unk)\y',
                                    '', 'gi')
WHERE drugname_clean ~* '.+\yunknown|unk\y.+$'
  AND rxcui IS NULL;




-- name: remove_blinded
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = regexp_replace(drugname_clean, ' *blinded *', '', 'gi')
WHERE drugname_clean LIKE '%blinded%'
  AND rxcui IS NULL;




-- name: change_chars
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = regexp_replace(drugname_clean, '\\', '/', 'gi')
WHERE drugname_clean LIKE '%\%'
  AND rxcui IS NULL;




-- name: remove_chars
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = regexp_replace(drugname_clean, '[\*\^\$\?\@]', ' ', 'gi')
WHERE rxcui IS NULL
  AND drugname_clean SIMILAR TO '%(\*|^|$|\?|@)%';




-- name: remove_trailing_chars
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = regexp_replace(drugname_clean, '(^[\;\,\"\:\/]+)|([\;\,\"\:]+$)', ' ', 'gi')
WHERE rxcui IS NULL
  AND drugname_clean SIMILAR TO '%(;|,|"|:)%';




-- name: trim_spaces
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = trim(BOTH ' ' FROM drugname_clean)
WHERE rxcui IS NULL
  AND (drugname_clean LIKE '% ' OR drugname_clean LIKE ' %');




-- name: trim_hyphen
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = trim(BOTH '-' FROM drugname_clean)
WHERE (drugname_clean LIKE '%-' OR drugname_clean LIKE '-%')
  AND rxcui IS NULL;




-- name: remove_quotes
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = regexp_replace(drugname_clean, '[''""]', '', 'gi')
WHERE rxcui IS NULL
  AND drugname_clean SIMILAR TO '%(''|")%';




-- name: reverse_dash
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = replace(drugname_clean, '\', ' / ')
WHERE drugname_clean LIKE '%\\%';




-- name: remove_spaces_before_closing_parenthesis_char
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = regexp_replace(drugname_clean, ' +\)', ')', 'gi')
WHERE drugname_clean LIKE '% )%'
  AND rxcui IS NULL;




-- name: remove_trailing_chars
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = regexp_replace(drugname_clean, '[ \.\,]$', '', 'gi')
WHERE (drugname_clean LIKE '%.'
    OR drugname_clean LIKE '%,'
    OR drugname_clean LIKE '% ')
  AND rxcui IS NULL;




-- name: remove_white
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = regexp_replace(drugname_clean, '(\S) +', '\1 ', 'gi')
WHERE drugname_clean LIKE '%  %'
  AND rxcui IS NULL;




-- name: add_space_parenthesis
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = replace(drugname_clean, '(', ' (')
WHERE drugname_clean LIKE '%(%'
  AND rxcui IS NULL;




-- name: replace_w
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = replace(drugname_clean, ' w/', ' / ')
WHERE drugname_clean LIKE '% w/%'
  AND drugname_clean NOT LIKE '% w/v %'
  AND rxcui IS NULL;




-- name: add_space_to_slash
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = replace(drugname_clean, '/', ' / ')
WHERE drugname_clean LIKE '%/%'
  AND drugname_clean NOT LIKE '%meq/l%'
  AND drugname_clean NOT LIKE '%mg/ml%'
  AND rxcui IS NULL;




-- name: remove_nos
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = replace(drugname_clean, ' nos ', ' ')
WHERE drugname_clean LIKE '% nos %'
  AND rxcui IS NULL;




-- name: remove_nos_ending
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = replace(drugname_clean, ' nos', '')
WHERE drugname_clean LIKE '% nos'
  AND rxcui IS NULL;




-- name: remove_product
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = replace(drugname_clean, ' product', '')
WHERE drugname_clean LIKE '% product'
   OR drugname_clean LIKE '% product %'
    AND rxcui IS NULL;




-- name: remove_unspecified
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = replace(drugname_clean, 'unspecified', '')
WHERE drugname_clean LIKE '%unspecified%'
  AND rxcui IS NULL;




-- name: replace_hooks_1
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = replace(drugname_clean, '[', '(')
WHERE drugname_clean LIKE '%[%'
  AND rxcui IS NULL;




-- name: replace_hooks_2
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = replace(drugname_clean, ']', ')')
WHERE drugname_clean LIKE '%]%'
  AND rxcui IS NULL;




-- name: replace_semi_colon
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = replace(drugname_clean, ';', '/')
WHERE drugname_clean LIKE '%;%'
  AND rxcui IS NULL;




-- name: remove_product
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = replace(drugname_clean, '/ /', '/')
WHERE drugname_clean LIKE '%/ /%'
  AND rxcui IS NULL;




-- name: remove_nos_parenth
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = replace(drugname_clean, ' (nos)', '')
WHERE drugname_clean LIKE '% (nos)'
  AND rxcui IS NULL;




-- name: remove_plus
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = replace(drugname_clean, '(+)', ' / ')
WHERE drugname_clean LIKE '%(+)%'
  AND rxcui IS NULL;




-- name: remove_leading_chars
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = regexp_replace(drugname_clean, '^ +', '', 'gi')
WHERE rxcui IS NULL
  AND drugname_clean LIKE ' %';




-- name: remove_whitespace
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = regexp_replace(drugname_clean, '(\S) +', '\1 ', 'gi')
WHERE rxcui IS NULL
  AND drugname_clean LIKE '%  %';




-- name: remove_empty_parenthesis
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = regexp_replace(drugname_clean, '\(\)', '', 'gi')
WHERE drugname_clean LIKE '%()%'
  AND rxcui IS NULL;




-- name: remove_residue
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = NULL
WHERE (drugname_clean = 'ingredient'
    OR drugname_clean = 'product')
  AND rxcui IS NULL;




-- name: remove_trailing_chars
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = regexp_replace(drugname_clean, '(^[\;\,\"\:\/]+)|([\;\,\"\:]+$)', '', 'gi')
WHERE rxcui IS NULL
  AND drugname_clean SIMILAR TO '%(;|,|"|:)';




-- name: trim_spaces
UPDATE faers.drug_mapping_exact_java
SET drugname_clean = trim(BOTH FROM drugname_clean)
WHERE rxcui IS NULL
  AND (drugname_clean LIKE '% ' OR drugname_clean LIKE ' %');