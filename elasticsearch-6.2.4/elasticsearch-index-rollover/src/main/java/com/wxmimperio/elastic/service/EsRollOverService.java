package com.wxmimperio.elastic.service;

import com.alibaba.fastjson.JSONObject;
import com.wxmimperio.elastic.connector.RestClientConnector;
import com.wxmimperio.elastic.entity.IndexMeta;
import com.wxmimperio.elastic.exception.EsException;
import com.wxmimperio.elastic.utils.EsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;

@Service
public class EsRollOverService {

    private final static Logger logger = LoggerFactory.getLogger(EsRollOverService.class);

    private RestClientConnector clientConnector;

    @Autowired
    public EsRollOverService(RestClientConnector clientConnector) {
        this.clientConnector = clientConnector;
    }

    @PostConstruct
    public void rolloverIndices(List<String> wistList, String suffix, JSONObject condition) throws IOException, EsException {
        if (!EsUtils.allowAllocation(clientConnector)) {
            logger.error("The index operation cannot be performed when determining whether the allocation is turned off.");
            return;
        }
        List<IndexMeta> metaList = EsUtils.getMarkIndicesMeta(clientConnector, wistList, suffix);
        int total = metaList.size();
        int rolled = 0;
        for (IndexMeta meta : metaList) {
            boolean rollOvered = EsUtils.rolloverIndex(clientConnector, meta, suffix, condition);
            if (rollOvered) {
                rolled++;
                // Set the last update time of the data
                EsUtils.setFrozenDate(clientConnector, meta, suffix);
            }
        }
        logger.info(String.format("Total index size = %s, rolled index size = %s", total, rolled));
    }
}
