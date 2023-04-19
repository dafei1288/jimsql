package com.dafei1288.jimsql.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class Config {

    private static Config config = new Config();

    private int port;

    private String host;

    private String datadir;


    private File configFile ;

    private Properties properties;

    private Map<String,Object> map ;

    private Config(){}

    public static Config getConfig(InputStream is) throws IOException {
        config.properties = new Properties();
        config.properties.load(is);
        config.init();
        config.properties = null;
        return config;
    }

    public static Config getConfig(String configPath) throws IOException {
        config.configFile = new File(configPath);
        InputStream is = new FileInputStream(configPath);
        Config config = getConfig(is);
        is.close();
        return config;
    }

    public static Config getConfig() throws IOException{
        InputStream is = Config.class.getResourceAsStream("/jimsql.properties");
        Config config = getConfig(is);
        is.close();
        return config;
    }

    private void init(){
        this.port = Integer.parseInt(this.properties.getProperty("port"));
        this.host = this.properties.getProperty("host");
        this.datadir = this.properties.getProperty("datadir");

        this.map = this.properties.keySet().stream().collect(Collectors.toMap(it->it.toString(),it->properties.get(it)));
    }

    public Object getValue(String key){
        return this.map.get(key);
    }

    public static void setConfig(Config config) {
        Config.config = config;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getDatadir() {
        return datadir;
    }

    public void setDatadir(String datadir) {
        this.datadir = datadir;
    }

    public File getConfigFile() {
        return configFile;
    }

    public void setConfigFile(File configFile) {
        this.configFile = configFile;
    }

    public Map<String, Object> getMap() {
        return map;
    }

    public void setMap(Map<String, Object> map) {
        this.map = map;
    }

    @Override
    public String toString() {
        return "Config{" +
            "port=" + port +
            ", host='" + host + '\'' +
            ", datadir='" + datadir + '\'' +
            ", configFile=" + configFile +
//            ", properties=" + properties +
            ", map=" + map +
            '}';
    }
}
