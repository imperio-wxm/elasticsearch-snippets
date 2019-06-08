package com.wxmimperio.elastic.connector;

import com.wxmimperio.elastic.config.EsConfig;
import com.wxmimperio.elastic.exception.EsException;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.io.IOException;
import java.util.stream.Stream;

@Component
public class RestClientConnector implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(RestClientConnector.class);

    private EsConfig esConfig;
    private RestHighLevelClient client;

    @Autowired
    public RestClientConnector(EsConfig esConfig) {
        this.esConfig = esConfig;
    }

    @PostConstruct
    public void initClient() throws EsException {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(esConfig.getUserName(), esConfig.getPassword()));
        RestClientBuilder restClientBuilder = RestClient.builder(getHttpHost());
        restClientBuilder.setMaxRetryTimeoutMillis(esConfig.getMaxRetryTimeoutMillis());
        restClientBuilder.setRequestConfigCallback(requestConfig -> requestConfig.setSocketTimeout(esConfig.getSocketTimeout()));
        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        this.client = new RestHighLevelClient(restClientBuilder);
    }

    private HttpHost[] getHttpHost() throws EsException {
        try {
            return Stream.of(esConfig.getServers().split(",", -1)).map(s -> {
                String[] url = s.split(":", -1);
                return new HttpHost(url[0], Integer.parseInt(url[1]));
            }).toArray(HttpHost[]::new);
        } catch (Exception e) {
            throw new EsException(String.format("Can not parse es urls = %s", esConfig.getServers()), e);
        }
    }

    @Override
    public void close() throws IOException {
        this.client.close();
    }
}
