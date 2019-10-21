package net.kaiyuan.kyflink.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configs {
    private static Properties properties;
    static {
        InputStream resourceAsStream = Configs.class.getClassLoader().getResourceAsStream("ky-flink-dev.properties");
        try {
            properties = new Properties();
            properties.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static Properties getProperties(){
        return properties;
    }
}
