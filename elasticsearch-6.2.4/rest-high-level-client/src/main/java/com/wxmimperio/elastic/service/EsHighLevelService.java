package com.wxmimperio.elastic.service;

import com.wxmimperio.elastic.connector.RestClientConnector;
import com.wxmimperio.elastic.exception.EsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.omg.PortableInterceptor.RequestInfoOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;

@Service
public class EsHighLevelService {
    private final static Logger logger = LoggerFactory.getLogger(EsHighLevelService.class);

    private RestClientConnector clientConnector;

    @Autowired
    public EsHighLevelService(RestClientConnector clientConnector) {
        this.clientConnector = clientConnector;
    }

    public void createIndexAsync(String indexName) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.settings(Settings.builder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1)
        );
        createIndexRequest.waitForActiveShards(ActiveShardCount.DEFAULT);
        clientConnector.getRestHighLevelClient().indices().createAsync(createIndexRequest, new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                if (createIndexResponse.isAcknowledged()) {
                    logger.info(String.format("Create index = %s success.", createIndexResponse.index()));
                } else {
                    logger.error(String.format("Create index = %s failure.", indexName));
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(String.format("Create index = %s failure.", indexName), e);
            }
        });
    }

    public void deleteIndexAsync(String[] indexNames) {
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest().indices(indexNames);
        clientConnector.getRestHighLevelClient().indices().deleteAsync(deleteRequest, new ActionListener<DeleteIndexResponse>() {
            @Override
            public void onResponse(DeleteIndexResponse deleteIndexResponse) {
                if (deleteIndexResponse.isAcknowledged()) {
                    logger.info(String.format("Delete index = %s success.", Arrays.asList(indexNames)));
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(String.format("Delete index = %s failure.", Arrays.asList(indexNames)), e);
            }
        });
    }
}
