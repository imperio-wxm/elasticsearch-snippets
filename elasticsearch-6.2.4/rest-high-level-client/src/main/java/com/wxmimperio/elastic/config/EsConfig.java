package com.wxmimperio.elastic.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;


@Configuration
public class EsConfig {

    @Value("es.connector.servers")
    private String servers;
    @Value("es.connector.userName")
    private String userName;
    @Value("es.connector.password")
    private String password;
    @Value("es.connector.max-retry-timeout-millis")
    private Integer maxRetryTimeoutMillis;
    @Value("es.connector.socket-timeout")
    private Integer socketTimeout;

    public EsConfig() {
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }


    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getMaxRetryTimeoutMillis() {
        return maxRetryTimeoutMillis;
    }

    public void setMaxRetryTimeoutMillis(Integer maxRetryTimeoutMillis) {
        this.maxRetryTimeoutMillis = maxRetryTimeoutMillis;
    }

    public Integer getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(Integer socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    @Override
    public String toString() {
        return "EsConfig{" +
                "servers='" + servers + '\'' +
                ", userName='" + userName + '\'' +
                ", password='" + password + '\'' +
                ", maxRetryTimeoutMillis=" + maxRetryTimeoutMillis +
                ", socketTimeout=" + socketTimeout +
                '}';
    }
}
