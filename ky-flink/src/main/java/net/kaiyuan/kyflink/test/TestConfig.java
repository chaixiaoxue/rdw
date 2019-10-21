package net.kaiyuan.kyflink.test;

import net.kaiyuan.kyflink.utils.Configs;

import java.util.Properties;

public class TestConfig {
    public static void main(String[] args) {
        Properties properties = Configs.getProperties();
        String property = properties.getProperty("bootstrap.servers");
        System.out.println(property);
    }
}
