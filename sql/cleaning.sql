-- name: remove_unknown
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, '\((\ \yunknown|unk\y)\)|\(\y(unknown|unk)\y\)|\y(unknown|unk)\y',
                                     '', 'gi')
WHERE drug_name_clean ~* '.+\yunknown|unk\y.+$'
  AND rxcui IS NULL;

-- name: remove_unknown_residue
UPDATE faers.drug_mapping
SET drug_name_clean = NULL
WHERE drug_name_clean = 'ingredient'
   OR drug_name_clean = 'product'
    AND rxcui IS NULL;

-- name: remove_blinded
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, ' *blinded *', '', 'gi')
WHERE drug_name_clean LIKE '%blinded%'
  AND rxcui IS NULL;

-- name: change_chars
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, '\\', '/', 'gi')
WHERE drug_name_clean LIKE '%\%'
  AND rxcui IS NULL;

-- name: remove_chars
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, '[\*\^\$\?]', '', 'gi')
WHERE rxcui IS NULL;

-- name: trim_semicolon
UPDATE faers.drug_mapping
SET drug_name_clean = trim(BOTH ';' FROM drug_name_clean)
WHERE rxcui IS NULL
  AND (drug_name_clean LIKE '%;' OR drug_name_clean LIKE ';%');

-- name: trim_colon
UPDATE faers.drug_mapping
SET drug_name_clean = trim(BOTH ':' FROM drug_name_clean)
WHERE rxcui IS NULL
  AND (drug_name_clean LIKE '%:' OR drug_name_clean LIKE ':%');

-- name: trim_spaces
UPDATE faers.drug_mapping
SET drug_name_clean = trim(BOTH ' ' FROM drug_name_clean)
WHERE rxcui IS NULL
  AND (drug_name_clean LIKE '% ' OR drug_name_clean LIKE ' %');

-- name: trim_slash
UPDATE faers.drug_mapping
SET drug_name_clean = trim(BOTH '/' FROM drug_name_clean)
WHERE rxcui IS NULL
  AND (drug_name_clean LIKE '%/' OR drug_name_clean LIKE '/%');

-- name: trim_hyphen
UPDATE faers.drug_mapping
SET drug_name_clean = trim(BOTH '-' FROM drug_name_clean)
WHERE (drug_name_clean LIKE '%-' OR drug_name_clean LIKE '-%')
  AND rxcui IS NULL;


-- name: remove_quotes
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, '[''""]', '', 'gi')
WHERE rxcui IS NULL;

-- name: reverse_dash
UPDATE faers.drug_mapping
SET drug_name_clean = replace(drug_name_clean, '\', ' / ')
WHERE drug_name_clean LIKE '%\\%';


-- name: remove_spaces_before_closing_parenthesis_char
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, ' +\)', ')', 'gi')
WHERE drug_name_clean LIKE '% )%'
  AND rxcui IS NULL;


-- name: remove_trailing_chars
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, '[ \.\,]$', '', 'gi')
WHERE (drug_name_clean LIKE '%.'
    OR drug_name_clean LIKE '%,'
    OR drug_name_clean LIKE '% ')
  AND rxcui IS NULL;

-- name: remove_white
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, '(\S) +', '\1 ', 'gi')
WHERE drug_name_clean LIKE '%  %'
  AND rxcui IS NULL;

-- name: add_space_parenthesis
UPDATE faers.drug_mapping
SET drug_name_clean = replace(drug_name_clean, '(', ' (')
WHERE drug_name_clean LIKE '%(%'
  AND rxcui IS NULL;


-- name: remove_trailing_spaces
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, ' +$', '', 'gi')
WHERE drug_name_clean LIKE '% '
  AND rxcui IS NULL;

-- name: remove_leading_spaces
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, '^ +', '', 'gi')
WHERE drug_name_clean LIKE ' %'
  AND rxcui IS NULL;

-- name: replace_w
UPDATE faers.drug_mapping
SET drug_name_clean = replace(drug_name_clean, ' w/', ' / ')
WHERE drug_name_clean LIKE '% w/%'
  AND drug_name_clean NOT LIKE '% w/v %'
  AND rxcui IS NULL;

-- name: add_space_to_slash
UPDATE faers.drug_mapping
SET drug_name_clean = replace(drug_name_clean, '/', ' / ')
WHERE drug_name_clean LIKE '%/%'
  AND drug_name_clean NOT LIKE '%meq/l%'
  AND drug_name_clean NOT LIKE '%mg/ml%'
  AND rxcui IS NULL;

-- name: remove_nos
UPDATE faers.drug_mapping
SET drug_name_clean = replace(drug_name_clean, ' nos ', ' ')
WHERE drug_name_clean LIKE '% nos %'
  AND rxcui IS NULL;

-- name: remove_nos_ending
UPDATE faers.drug_mapping
SET drug_name_clean = replace(drug_name_clean, ' nos', '')
WHERE drug_name_clean LIKE '% nos'
  AND rxcui IS NULL;

-- name: remove_product
UPDATE faers.drug_mapping
SET drug_name_clean = replace(drug_name_clean, ' product', '')
WHERE drug_name_clean LIKE '% product'
   OR drug_name_clean LIKE '% product %'
    AND rxcui IS NULL;

-- name: remove_unspecified
UPDATE faers.drug_mapping
SET drug_name_clean = replace(drug_name_clean, 'unspecified', '')
WHERE drug_name_clean LIKE '%unspecified%'
  AND rxcui IS NULL;

-- name: replace_hooks_1
UPDATE faers.drug_mapping
SET drug_name_clean = replace(drug_name_clean, '[', '(')
WHERE drug_name_clean LIKE '%[%'
  AND rxcui IS NULL;

-- name: replace_hooks_2
UPDATE faers.drug_mapping
SET drug_name_clean = replace(drug_name_clean, ']', ')')
WHERE drug_name_clean LIKE '%]%'
  AND rxcui IS NULL;

-- name: replace_semi_colon
UPDATE faers.drug_mapping
SET drug_name_clean = replace(drug_name_clean, ';', '/')
WHERE drug_name_clean LIKE '%;%'
  AND rxcui IS NULL;

-- name: remove_product
UPDATE faers.drug_mapping
SET drug_name_clean = replace(drug_name_clean, '/ /', '/')
WHERE drug_name_clean LIKE '%/ /%'
  AND rxcui IS NULL;

-- name: remove_nos_parenth
UPDATE faers.drug_mapping
SET drug_name_clean = replace(drug_name_clean, ' (nos)', '')
WHERE drug_name_clean LIKE '% (nos)'
  AND rxcui IS NULL;

-- name: remove_plus
UPDATE faers.drug_mapping
SET drug_name_clean = replace(drug_name_clean, '(+)', ' / ')
WHERE drug_name_clean LIKE '%(+)%'
  AND rxcui IS NULL;


-- name: remove_leading_chars
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, '^ +', '', 'gi')
WHERE rxcui IS NULL;


-- name: remove_whitespace
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, '(\S) +', '\1 ', 'gi')
WHERE rxcui IS NULL;

-- name: remove_remove_nnnnn
UPDATE faers.drug_mapping a
SET drug_name_clean = regexp_replace(drug_name_clean, '\/ \d+\ /\ *', '', 'gi')
WHERE rxcui IS NULL
  AND drug_name_clean ~* '.*\/ \d+\ /.*';

-- name: remove_remove_nnn
UPDATE faers.drug_mapping a
SET drug_name_clean = regexp_replace(drug_name_clean, '\/\d+\/\ *', '', 'gi')
WHERE rxcui IS NULL
  AND drug_name_clean ~* '.*\/\d+\/.*';

-- name: remove_empty_parenthesis
UPDATE faers.drug_mapping
SET drug_name_clean = regexp_replace(drug_name_clean, '\(\)', '', 'gi')
WHERE drug_name_clean LIKE '%()%'
  AND rxcui IS NULL;