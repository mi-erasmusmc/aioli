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
        props.setProperty("preferQueryMode", "simple");
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
        log.info(query);
        var conn = getConnection();
        try (var stmt = conn.prepareStatement(query)) {
            stmt.execute();
        } catch (SQLException e) {
            log.error(e.getMessage());
            throw new DatabaseException(e);
        }
    }

    public void execute(String query) {
        log.info(EXECUTING_A_QUERY);
        log.info(query);
        try (var stmt = getConnection().prepareStatement(query)) {
            stmt.execute();
        } catch (SQLException e) {
            log.error(e.getMessage());
            throw new DatabaseException(e);
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
            throw new DatabaseException(e);
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
            log.error(e.getMessage());
            throw new DatabaseException(e);
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
            log.error(e.getMessage());
            throw new DatabaseException(e);
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
}
