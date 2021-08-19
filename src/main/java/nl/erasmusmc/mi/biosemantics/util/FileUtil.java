package nl.erasmusmc.mi.biosemantics.util;

import nl.erasmusmc.mi.biosemantics.exception.SqlFileToStringException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileUtil {

    private FileUtil() {
    }

    public static String sqlFileToString(String fileName) {
        try {
            var path = Path.of("sql/" + fileName);
            // Each entire file is submitted as a single transaction, so we commit each query individually
            // to avoid the db consuming a lot of resources on a big transaction
            String queryEnd = ";" + System.lineSeparator();
            return Files.readString(path).replace(queryEnd, queryEnd + " COMMIT; ");
        } catch (IOException e) {
            throw new SqlFileToStringException(e);
        }
    }
}
