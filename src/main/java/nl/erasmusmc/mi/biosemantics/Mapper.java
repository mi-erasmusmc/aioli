package nl.erasmusmc.mi.biosemantics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static nl.erasmusmc.mi.biosemantics.util.FileUtil.sqlFileToString;

public class Mapper {

    private static final Logger log = LogManager.getLogger();
    private static final List<String> TARGET_COLUMNS = List.of("dose_form_clean", "route_clean");

    private static final String SQL_POPULATE_BRAND_AND_INGREDIENT = "populate_brand_and_ingredient.sql";
    private static final String SQL_PARENTHESIS = "parenthesis.sql";
    private static final String SQL_RENAME_INGREDIENTS = "rename_ingredients.sql";
    private static final String SQL_CREATE_TABLE = "create_table.sql";


    Database db;
    RxNormalizer rxNormalizer;
    AioliApp.Vocab vocab;
    boolean retainMulti;
    boolean skipNormalizer;


    public Mapper(AioliApp.Vocab vocab, boolean retainMulti, boolean skipNormalizer) {
        this.vocab = vocab;
        this.retainMulti = retainMulti;
        this.skipNormalizer = skipNormalizer;
        this.db = new Database();
        this.rxNormalizer = new RxNormalizer();
    }

    public void map() {
        loadArt57();
        db.executeFile(SQL_CREATE_TABLE);
        db.executeFile(SQL_POPULATE_BRAND_AND_INGREDIENT);
//         We do it twice because more trailing chars can appear after cleaning a some
        cleanDrugNameAndAi();
        db.executeFile(SQL_POPULATE_BRAND_AND_INGREDIENT);
        cleanDrugNameAndAi();
        db.executeFile(SQL_POPULATE_BRAND_AND_INGREDIENT);
//         Again twice to catch more
        db.executeFile(SQL_PARENTHESIS);
        cleanDrugNameAndAi();
        db.executeFile(SQL_PARENTHESIS);
        cleanDrugNameAndAi();
        db.executeFile(SQL_POPULATE_BRAND_AND_INGREDIENT);
        db.executeFile("populate_brand_from_any_word.sql");
        if (!skipNormalizer) {
            callRxNormalizer();
        }
        aeolus();
        loadManualMapping();
        if (vocab.equals(AioliApp.Vocab.ATC)) {
            loadRxNormToAtcPatch();
            db.executeFile("clean_dose_amt.sql");
            cleanDoseForm();
            exactMapping();
            remapDoseFrom(1);
        }
        db.executeFile(SQL_RENAME_INGREDIENTS);
        if (vocab.equals(AioliApp.Vocab.ATC)) {
            exactMapping();
            remapDoseFrom(2);
            exactMapping();
        }
        rollUp();
        db.executeFile("combined_drug_mapping.sql");
    }

    // The aeolus mapping algorithm is seperated in two files, as it requires too much overhead for a single transaction.
    private void aeolus() {
        db.executeFile("aeolus_1.sql");
        db.executeFile("aeolus_2.sql");
    }

    private void rollUp() {
        log.info("Getting all final ids for stuff takes 30 minutes");
        String sqlFileName = "rollup_" + vocab.toString().toLowerCase() + ".sql";
        db.executeFile(sqlFileName);

        if (vocab.equals(AioliApp.Vocab.RXNORM) && !retainMulti) {
            db.executeFile("split_multi_rxnorm.sql");
        }
    }

    private void remapDoseFrom(int round) {
        log.info("Remapping dose forms e.g. pill to capsule");
        String manualMapFile = round == 1 ? "remap_df_1.csv" : "remap_df_2.csv";
        Map<String, String> manualMapping = getManualMapping(manualMapFile, false);
        String q = "UPDATE faers.drug_mapping_exact_java SET rx_dose_form = ? WHERE rx_dose_form = ?;";
        db.executeBatch(q, manualMapping);
    }

    private void exactMapping() {
        log.info("Doing the mapping to specific rxnorm types, takes approx 1 hour per .sql file");
        db.executeFile("map_to_sbd.sql");
        db.executeFile("map_to_scd.sql");
        db.executeFile("map_to_sbdf.sql");
        db.executeFile("map_to_scdf.sql");
        db.executeFile("map_to_scdc.sql");
    }

    private void callRxNormalizer() {
        String dnQuery = "SELECT DISTINCT drugname_clean as drug " +
                "FROM faers.drug_mapping_exact_java " +
                "WHERE rxcui IS NULL " +
                "  AND rx_ingredient IS NULL " +
                "  AND rx_brand_name IS NULL " +
                "  AND drugname_clean IS NOT NULL ";
        String aiQuery = "SELECT DISTINCT prod_ai_clean AS drug " +
                "          FROM faers.drug_mapping_exact_java " +
                "          WHERE rxcui IS NULL " +
                "            AND rx_ingredient IS NULL " +
                "            AND prod_ai_clean IS NOT NULL ";
        List.of(aiQuery, dnQuery).forEach(q -> {
            int limit = 2000;
            int results = limit;
            int offset = 0;
            while (results == limit) {
                String query = q + " OFFSET " + offset + " LIMIT " + limit + " ;";
                List<String> drugs = db.executeQueryOneValue(query);
                results = drugs.size();
                Map<String, List<Integer>> rxNormalizerResults = rxNormalizer.callRxNav(drugs);
                offset += (limit - rxNormalizerResults.size());
                storeRxNormalizerResults(rxNormalizerResults);
            }
        });
    }

    private void storeRxNormalizerResults(Map<String, List<Integer>> results) {
        if (results.isEmpty()) {
            return;
        }
        var exact = new AbstractMap.SimpleEntry<>("'SCD','SBD'", "rxcui");
        var ingredient = new AbstractMap.SimpleEntry<>("'IN','MIN'", "rx_ingredient");
        var pin = new AbstractMap.SimpleEntry<>("'PIN'", "rx_ingredient");
        var brand = new AbstractMap.SimpleEntry<>("'BN'", "rx_brand_name");
        var ttys = List.of(exact, ingredient, pin, brand);
        Set<Integer> ids = new HashSet<>();
        results.forEach((name, idList) -> ids.addAll(idList));
        if (ids.isEmpty()) return;
        String sql = "SELECT rxcui, str FROM rxnorm.rxnconso WHERE rxcui IN (%s) AND sab = 'RXNORM' AND TTY IN (%s);";

        for (AbstractMap.SimpleEntry<String, String> tty : ttys) {
            String ttySql = String.format(sql, getIdString(ids), tty.getKey());
            Map<Integer, String> matches = db.executeQueryTwoValues(ttySql);
            log.info("Matched {} {}", matches.size(), tty.getKey());
            updateFromRxNav(results, matches, tty.getValue());
            // Remove found matches from id list
            ids.removeAll(matches.keySet());
            removeFoundFromResults(results, matches);
            if (ids.isEmpty()) break;
        }

        if (ids.isEmpty()) return;

        String restSql = "WITH cte AS (SELECT DISTINCT rel.rxcui1, lower(string_agg(distinct r_in.str, ' / ' ORDER BY r_in.str)) AS ingr " +
                "             FROM rxnorm.rxnrel rel " +
                "                      JOIN rxnorm.rxnconso r_in ON rel.rxcui2 = r_in.rxcui " +
                "             WHERE rel.rxcui1 IN (" + getIdString(ids) + ") " +
                "               AND r_in.tty IN ('IN') " +
                "               AND r_in.sab = 'RXNORM' " +
                "             GROUP BY rel.rxcui1) " +
                "SELECT DISTINCT cte.rxcui1 AS rxcui, r.str AS ingredient " +
                "FROM rxnorm.rxnconso r " +
                "         JOIN cte ON lower(r.str) = cte.ingr AND r.tty IN ('IN', 'MIN') " +
                "    AND r.sab = 'RXNORM'; ";
        Map<Integer, String> matches = db.executeQueryTwoValues(restSql);
        log.info("Matched {} in the rest group", matches.size());
        updateFromRxNav(results, matches, "rx_ingredient");

        String restSql2 = "WITH cte AS (SELECT DISTINCT rel.rxcui1, lower(string_agg(distinct r_in.str, ' / ' ORDER BY r_in.str)) AS ingr " +
                "             FROM rxnorm.rxnrel rel " +
                "                      JOIN rxnorm.rxnconso r_in ON rel.rxcui2 = r_in.rxcui " +
                "             WHERE rel.rxcui1 IN (" + getIdString(ids) + ") " +
                "               AND r_in.tty IN ('DF') " +
                "               AND r_in.sab = 'RXNORM' " +
                "             GROUP BY rel.rxcui1) " +
                "SELECT DISTINCT cte.rxcui1 AS rxcui, r.str AS ingredient " +
                "FROM rxnorm.rxnconso r " +
                "         JOIN cte ON lower(r.str) = cte.ingr AND r.tty IN ('DF') " +
                "    AND r.sab = 'RXNORM'; ";
        Map<Integer, String> dfMatches = db.executeQueryTwoValues(restSql2);
        log.info("Matched {} dose forms in the rest group", dfMatches.size());
        updateFromRxNav(results, dfMatches, "rx_dose_form");
        log.info("{} RxCUIs in the ids returned by rxnav were not stored", ids.size() - matches.size());
    }

    // Remove hits from result list, like this we only update to one kind of tty, based on the order of interest exact, in, pin, bn
    private void removeFoundFromResults(Map<String, List<Integer>> results, Map<Integer, String> matches) {
        Set<Integer> hits = matches.keySet();
        Map<String, List<Integer>> resultsCopy = new HashMap<>(results);
        resultsCopy.forEach((original, ids) -> {
            if (hits.stream().anyMatch(ids::contains)) {
                results.remove(original);
            }
        });
    }

    private String getIdString(Set<Integer> ids) {
        return ids.stream().map(Object::toString).collect(Collectors.joining(","));
    }

    private void updateFromRxNav
            (Map<String, List<Integer>> results, Map<Integer, String> ingredientMatches, String target) {
        String query = String.format("UPDATE faers.drug_mapping_exact_java SET %s = ? WHERE (drugname_clean = ?) AND %s IS NULL; ", target, target);
        if (!target.equals("rxcui")) {
            updateNonRxCui(results, ingredientMatches, query);
        } else {
            updateRxCui(results, ingredientMatches, query);
        }
    }

    // This method updates rxcui integer field
    private void updateRxCui
    (Map<String, List<Integer>> results, Map<Integer, String> ingredientMatches, String query) {
        Map<String, Integer> hits = new HashMap<>();
        results.forEach((originalName, idList) -> {
            AtomicInteger counter = new AtomicInteger();
            AtomicInteger updateValue = new AtomicInteger();
            idList.forEach(id -> {
                String ingredientMatch = ingredientMatches.get(id);
                if (ingredientMatch != null) {
                    updateValue.set(id);
                    counter.getAndIncrement();
                }
            });
            if (counter.get() == 1) {
                log.debug("{} mapped to {}", originalName, updateValue);
                hits.put(originalName, updateValue.get());
            }
        });
        db.executeBatchReversedInteger(query, hits);
    }

    // This method updates string fields
    private void updateNonRxCui
    (Map<String, List<Integer>> results, Map<Integer, String> ingredientMatches, String query) {
        Map<String, String> hits = new HashMap<>();
        results.forEach((originalName, idList) -> {
            AtomicInteger counter = new AtomicInteger();
            AtomicReference<String> updateValue = new AtomicReference<>("");
            idList.forEach(id -> {
                String ingredientMatch = ingredientMatches.get(id);
                if (ingredientMatch != null) {
                    updateValue.set(ingredientMatch.toLowerCase());
                    counter.getAndIncrement();
                }
            });
            if (counter.get() == 1) {
                log.debug("{} mapped to {}", originalName, updateValue);
                hits.put(originalName, updateValue.get());
            }
        });
        db.executeBatchReversedString(query, hits);
    }

    private void cleanDrugNameAndAi() {
        // The cleaning.sql file targets drugname_clean, but we also want to clean the prod_ai field
        String file = "cleaning.sql";
        String aiQuery = sqlFileToString(file).replace("drugname_clean", "prod_ai_clean");
        log.info("Will take about 5 minutes:");
        db.executeFile(file);
        log.info("Will takes about 5 minutes:");
        db.execute(aiQuery);
    }

    private void cleanDoseForm() {
        db.executeFile("clean_dose_form.sql");
        Map<String, String> mapping = getManualMapping("route_and_form_map.csv", false);
        TARGET_COLUMNS.forEach(t -> {
            log.info("Doing stuff on the {} column", t);
            updateBulk(mapping, t);
            removeUnknown(t);
            cleaning(t);
        });
        db.executeFile("dose_form_1.sql");
        Map<String, String> moreMappings = getManualMapping("rx_dose_form.csv", true);
        String query = "UPDATE faers.drug_mapping_exact_java SET rx_dose_form = ? WHERE temp_dose_form = ?;";
        db.executeBatch(query, moreMappings);
        db.executeFile("dose_form_2.sql");
        findDoseFormsInDrugName();
    }

    private void findDoseFormsInDrugName() {
        StringBuilder sb = new StringBuilder();
        List.of("6", "5", "4", "3", "2", "1").forEach(number -> {
            String stmt = "WITH cte1 AS (SELECT lower(str) AS df, " +
                    "                     array_length(string_to_array(lower(str), ' '), 1) " +
                    "                                AS word " +
                    "              FROM rxnorm.rxnconso rx " +
                    "              WHERE rx.tty = 'DF' " +
                    "                AND rx.sab = 'RXNORM' " +
                    "                AND length(str) > 3 " +
                    "              GROUP BY lower(str)) " +
                    "UPDATE faers.drug_mapping_exact_java dme " +
                    "SET rx_dose_form = cte1.df " +
                    "FROM cte1 " +
                    "WHERE dme.drugname_clean LIKE concat('%', cte1.df, '%') " +
                    "  AND dme.rx_dose_form IS NULL " +
                    "  AND cast(cte1.word AS INT) = number;";
            sb.append(stmt.replace("number", number));
        });
        db.execute(sb.toString());
    }

    private void cleaning(String target) {
        var update = "UPDATE faers.drug_mapping_exact_java";
        var q = String.format(
                "%2$s SET %1$s = NULL WHERE %1$s ~ '^[[:space:]]*$'; " +
                        "%2$s SET %1$s = regexp_replace(%1$s, ' +$', '', 'gi') WHERE %1$s LIKE '%% '; " +
                        "%2$s SET %1$s = regexp_replace(%1$s, '^ +', '', 'gi') WHERE %1$s LIKE ' %%'; " +
                        "%2$s SET %1$s = regexp_replace(%1$s, '(\\\\S) +', '\\\\1 ', 'gi') WHERE %1$s LIKE ' %%'; " +
                        "%2$s SET %1$s = NULL WHERE %1$s = '';", target, update);
        db.execute(q);
    }

    private void removeUnknown(String target) {
        Set<String> distinctR = getSpaceSeperatedValues("SELECT DISTINCT lower(str) AS str FROM rxnorm.rxnconso WHERE tty = 'DF' AND sab = 'RXNORM'");
        var q = String.format("select distinct lower(%1$s) from faers.drug_mapping_exact_java where %1$s is not null", target);
        Set<String> distinctF = getSpaceSeperatedValues(q);
        var sb = new StringBuilder();
        final Set<String> finalDistinctR = distinctR;
        distinctF.stream()
                .filter(f -> !finalDistinctR.contains(f))
                .map(unk -> String.format("update faers.drug_mapping_exact_java set %1$s = replace(%1$s, ' %2$s ', ' ') where %1$s like '%% %2$s %%';", target, unk))
                .forEach(sb::append);
        db.execute(sb.toString());
    }

    private Set<String> getSpaceSeperatedValues(String q) {
        Set<String> distinct = new HashSet<>();
        var rs = db.executeQueryOneValue(q);
        rs.forEach(value -> {
            var words = value.split(" ");
            distinct.addAll(Arrays.asList(words));
        });
        return distinct;
    }


    private Map<String, String> getManualMapping(String file, boolean reversed) {
        Map<String, String> map = new HashMap<>();
        try (var br = new BufferedReader(new FileReader("src/main/resources/manual_mappings/" + file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (!reversed) {
                    map.put(values[0], values[1]);
                } else {
                    map.put(values[1], values[0]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

    private void updateBulk(Map<String, String> map, String target) {
        var sb = new StringBuilder();
        map.forEach((k, v) -> sb.append(String.format("update faers.drug_mapping_exact_java set %1$s = replace(%1$s, ' %2$s ', ' %3$s ') where %1$s like '%% %2$s %%';", target, k, v)));
        var query = sb.toString();
        log.debug(query);
        db.execute(query);
    }

    private void loadArt57() {
        String dir = System.getProperty("user.dir") + "/src/main/resources/manual_mappings/art57_rxnorm.tsv";
        log.info("Loading Article 57 list");
        log.info("Loading article 57 data from {}", dir);
        String query = "DROP TABLE IF EXISTS faers.article57_rxnorm;" +
                "CREATE TABLE faers.article57_rxnorm (name TEXT, ingredient TEXT, rxcui TEXT);";
        db.execute(query);
        db.copyFile("faers.article57_rxnorm", dir);
    }

    private void loadRxNormToAtcPatch() {
        log.info("Loading RxNorm to ATC list");
        String dir = System.getProperty("user.dir") + "/src/main/resources/manual_mappings/rxnorm_atc_patch.tsv";
        log.info("Loading rxnorm_atc_patch from {}", dir);
        String query = "DROP TABLE IF EXISTS faers.rxnorm_atc_patch;" +
                "CREATE TABLE faers.rxnorm_atc_patch (code INT, name VARCHAR, ingredients INT, atc VARCHAR);";
        db.execute(query);
        db.copyFile("faers.rxnorm_atc_patch", dir);

        String index = "CREATE EXTENSION IF NOT EXISTS pg_trgm; " +
                "CREATE INDEX rxnorm_atc_patch_name_index " +
                "    ON faers.rxnorm_atc_patch USING gin (lower(name) faers.gin_trgm_ops);";
        db.execute(index);
    }

    private void loadManualMapping() {
        log.info("Manual mapping of drugs");
        String dir = System.getProperty("user.dir") + "/src/main/resources/manual_mappings/manual_mapping.tsv";
        log.info("Loading manual_mapping data from {}", dir);
        String query = "DROP TABLE IF EXISTS faers.manual_mapping;" +
                "CREATE TABLE faers.manual_mapping (drugname VARCHAR, rx_ingredient VARCHAR);";
        db.execute(query);
        db.copyFile("faers.manual_mapping", dir);

        String sql = "UPDATE faers.drug_mapping_exact_java dme " +
                "SET rx_ingredient = mm.rx_ingredient " +
                "FROM faers.manual_mapping mm " +
                "WHERE dme.rx_ingredient IS NULL " +
                "  AND dme.drugname = mm.drugname;";
        db.execute(sql);
    }
}
