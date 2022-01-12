package nl.erasmusmc.mi.biosemantics;

import nl.erasmusmc.mi.biosemantics.conf.PropertiesLoader;
import nl.erasmusmc.mi.biosemantics.exception.DatabaseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static nl.erasmusmc.mi.biosemantics.util.FileUtil.sqlFileToString;

public class Database {

    public static final String EXECUTING_A_QUERY = "Executing a query";
    private final Properties props;
    private final String url;
    private final Logger log = LogManager.getLogger();
    private Connection connection;


    public Database() {
        props = PropertiesLoader.getProperties();
        props.setProperty("allowMultiQueries", "true");
        props.setProperty("socketTimeout", "0");
        props.setProperty("connectTimeout", "0");
        var port = Integer.parseInt(props.getProperty("port"));
        var host = props.getProperty("host");
        var name = props.getProperty("name");
        url = String.format("jdbc:postgresql://%s:%d/%s?", host, port, name);
    }


    private Connection getConnection() {
        try {
            if (connection == null || !connection.isValid(6)) {
                connection = DriverManager.getConnection(url, props);
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            log.error(e.getMessage());
            throw new DatabaseException(e);
        }
        return connection;
    }

    public void executeFile(String fileName) {
        log.info("Executing: {}", fileName);
        var query = sqlFileToString(fileName);
        log.debug(query);
        var conn = getConnection();
        try (var stmt = conn.prepareStatement(query)) {
            stmt.execute();
        } catch (SQLException e) {
            handleSQLException(fileName, e);
        }
    }

    public void execute(String query) {
        log.info(EXECUTING_A_QUERY);
        log.info(query);
        try (var stmt = getConnection().prepareStatement(query)) {
            stmt.execute();
        } catch (SQLException e) {
            handleSQLException(query, e);
        }
    }

    public void executeBatch(String query, Map<String, String> map) {
        try (var stmt = getConnection().prepareStatement(query)) {
            map.forEach((key, value) -> {
                try {
                    stmt.setString(1, key);
                    stmt.setString(2, value);
                    stmt.addBatch();
                } catch (SQLException e) {
                    log.error(e.getMessage());
                    throw new DatabaseException(e);
                }
            });
            stmt.executeBatch();
        } catch (SQLException e) {
            log.error(e.getMessage());
            // Following file has a query which is known to occasionally break postgres, if it happens we just pretend like all is good
            if (!e.getMessage().contains("An I/O error occurred while sending to the backend.")) {
                throw new DatabaseException(e);
            } else {
                log.warn("Going to do the crazy hack for batch execute");
                doCrazyHack();
            }
        }
    }

    public void executeBatchReversedString(String query, Map<String, String> map) {
        try (var stmt = getConnection().prepareStatement(query)) {
            map.forEach((key, value) -> {
                try {
                    stmt.setString(2, key);
                    stmt.setString(1, value);
                    stmt.addBatch();
                } catch (SQLException e) {
                    log.error(e.getMessage());
                    throw new DatabaseException(e);
                }
            });
            stmt.executeBatch();
        } catch (SQLException e) {
            handleSQLException(query, e);
        }
    }

    public void executeBatchReversedInteger(String query, Map<String, Integer> map) {
        try (var stmt = getConnection().prepareStatement(query)) {
            map.forEach((key, value) -> {
                try {
                    stmt.setString(2, key);
                    stmt.setInt(1, value);
                    stmt.addBatch();
                } catch (SQLException e) {
                    log.error(e.getMessage());
                    throw new DatabaseException(e);
                }
            });
            stmt.executeBatch();
        } catch (SQLException e) {
            handleSQLException(query, e);
        }
    }


    public List<String> executeQueryOneValue(String query) {
        log.info(EXECUTING_A_QUERY);
        log.info(query);
        try (var stmt = getConnection().prepareStatement(query)) {
            List<String> drugs = new ArrayList<>();
            var rs = stmt.executeQuery();
            while (rs.next()) {
                drugs.add(rs.getString(1));
            }
            return drugs;
        } catch (SQLException e) {
            log.error(e.getMessage());
            throw new DatabaseException(e);
        }
    }

    public Map<Integer, String> executeQueryTwoValues(String query) {
        log.info(EXECUTING_A_QUERY);
        log.info(query);
        try (var stmt = getConnection().prepareStatement(query)) {
            Map<Integer, String> pair = new HashMap<>();
            var rs = stmt.executeQuery();
            while (rs.next()) {
                pair.put(rs.getInt(1), rs.getString(2));
            }
            return pair;
        } catch (SQLException e) {
            log.error(e.getMessage());
            throw new DatabaseException(e);
        }
    }

    public void copyFile(String table, String path) {
        try {
            String sql = "COPY " + table + " FROM STDIN WITH DELIMITER E'\\t' CSV HEADER QUOTE E'\\b';";
            CopyManager cm = new CopyManager((BaseConnection) getConnection());
            long affected = cm.copyIn(sql, new BufferedReader(new FileReader(path)));
            log.info("Inserted {} rows into {}", affected, table);
        } catch (SQLException | IOException e) {
            log.error(e.getMessage());
            throw new DatabaseException(e);
        }
    }

    // there is an issue when running docker on Mac where the connection is interrupted after 5 minutes, but the query keeps running in the db...
    // Instead of solving the root issue (I have tried (a bit)...), we have a crazy hack
    private void handleSQLException(String fileName, SQLException e) {
        log.warn("Caught SQLException, {}", e.getMessage());
        if (!e.getMessage().contains("An I/O error occurred while sending to the backend.")) {
            throw new DatabaseException(e);
        } else {
            log.warn("Going to do the crazy hack for {}", fileName);
            doCrazyHack();
        }
    }


    private void doCrazyHack() {
        var conn = getConnection();
        var sql = "SELECT " +
                "    pid, " +
                "    now() - pg_stat_activity.query_start AS duration, " +
                "    query, " +
                "    state " +
                "FROM pg_stat_activity " +
                "WHERE (now() - pg_stat_activity.query_start) > INTERVAL '4 minutes';";
        try (var stmt = conn.prepareStatement(sql)) {
            var rs = stmt.executeQuery();
            if (rs.next()) {
                var query = rs.getString("query");
                if (query.contains("SHOW TRANSACTION ISOLATION LEVEL")) {
                    var pid = rs.getInt("pid");
                    try (var killStmt = conn.prepareStatement("SELECT pg_terminate_backend(" + pid + ");")) {
                        log.warn("Killing: {}", query);
                        killStmt.execute();
                    }
                } else {
                    log.warn("Waiting for this query to finish: {}", query);
                }
                Thread.sleep(100000);
                doCrazyHack();
            } else {
                log.info("Seems like we are dong waiting");
            }
        } catch (SQLException | InterruptedException e) {
            throw new DatabaseException(e);
        }
    }
}
