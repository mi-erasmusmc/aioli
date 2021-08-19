package nl.erasmusmc.mi.biosemantics.conf;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesLoader {

    private PropertiesLoader() {
    }

    public static Properties getProperties() {
        Properties prop = new Properties();
        try (InputStream is = new FileInputStream("src/main/resources/config.properties")) {
            prop.load(is);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return prop;
    }
}
