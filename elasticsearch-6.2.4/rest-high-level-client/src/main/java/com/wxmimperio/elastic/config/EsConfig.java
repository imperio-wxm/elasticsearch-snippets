package com.wxmimperio.elastic.config;

import java.util.ResourceBundle;

public class EsConfig {

    private String servers;
    private String port;
    private String userName;
    private String password;

    public EsConfig() {
        initPorps();
    }

    private void initPorps() {
        ResourceBundle resourceBundle = ResourceBundle.getBundle("application");
        this.servers = resourceBundle.getString("es.servers");
        this.port = resourceBundle.getString("es.port");
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return "EsConfig{" +
                "servers='" + servers + '\'' +
                ", port='" + port + '\'' +
                '}';
    }
}
