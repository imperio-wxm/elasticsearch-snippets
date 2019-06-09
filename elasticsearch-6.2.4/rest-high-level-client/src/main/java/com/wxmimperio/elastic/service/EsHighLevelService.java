package com.wxmimperio.elastic.service;

import com.wxmimperio.elastic.connector.RestClientConnector;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.common.settings.Settings;
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

    public void createIndexSync(String indexName) {
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

    public void createIndexAsync(String indexName) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.settings(Settings.builder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1)
        );
        createIndexRequest.waitForActiveShards(ActiveShardCount.DEFAULT);
        try {
            CreateIndexResponse createIndexResponse = clientConnector.getRestHighLevelClient().indices().create(createIndexRequest);
            if (createIndexResponse.isAcknowledged()) {
                logger.info(String.format("Create index = %s success.", createIndexResponse.index()));
            }
        } catch (IOException | ElasticsearchException e) {
            logger.error(String.format("Create index = %s failure.", indexName), e);
        }
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

    public void deleteIndexSync(String[] indexNames) {
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest().indices(indexNames);
        try {
            DeleteIndexResponse deleteIndexResponse = clientConnector.getRestHighLevelClient().indices().delete(deleteRequest);
            if (deleteIndexResponse.isAcknowledged()) {
                logger.info(String.format("Delete index = %s success.", Arrays.asList(indexNames)));
            }
        } catch (IOException | ElasticsearchException e) {
            logger.error(String.format("Delete index = %s failure.", Arrays.asList(indexNames)), e);
        }
    }

    public void closeIndexSync(String[] indexNames) {
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(indexNames);
        try {
            CloseIndexResponse closeIndexResponse = clientConnector.getRestHighLevelClient().indices().close(closeIndexRequest);
            if (closeIndexResponse.isAcknowledged()) {
                logger.info(String.format("Close index = %s success.", Arrays.asList(indexNames)));
            }
        } catch (IOException | ElasticsearchException e) {
            logger.error(String.format("Close index = %s failure.", Arrays.asList(indexNames)), e);
        }
    }

    public void closeIndexAsync(String[] indexNames) {
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(indexNames);
        clientConnector.getRestHighLevelClient().indices().closeAsync(closeIndexRequest, new ActionListener<CloseIndexResponse>() {
            @Override
            public void onResponse(CloseIndexResponse closeIndexResponse) {
                if (closeIndexResponse.isAcknowledged()) {
                    logger.info(String.format("Close index = %s success.", Arrays.asList(indexNames)));

                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(String.format("Close index = %s failure.", Arrays.asList(indexNames)), e);
            }
        });
    }

    public void openIndexSync(String[] indexNames) {
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(indexNames);
        try {
            OpenIndexResponse openIndexResponse = clientConnector.getRestHighLevelClient().indices().open(openIndexRequest);
            if (openIndexResponse.isAcknowledged()) {
                logger.info(String.format("Open index = %s success.", Arrays.asList(indexNames)));
            }
        } catch (IOException | ElasticsearchException e) {
            logger.error(String.format("Open index = %s failure.", Arrays.asList(indexNames)), e);
        }
    }

    public void openIndexAsync(String[] indexNames) {
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(indexNames);
        clientConnector.getRestHighLevelClient().indices().openAsync(openIndexRequest, new ActionListener<OpenIndexResponse>() {
            @Override
            public void onResponse(OpenIndexResponse openIndexResponse) {
                if (openIndexResponse.isAcknowledged()) {
                    logger.info(String.format("Open index = %s success.", Arrays.asList(indexNames)));
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(String.format("Open index = %s failure.", Arrays.asList(indexNames)), e);
            }
        });
    }

}
