package com.wxmimperio.elastic.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import com.wxmimperio.elastic.config.Method;
import com.wxmimperio.elastic.connector.RestClientConnector;
import com.wxmimperio.elastic.entity.IndexMeta;
import com.wxmimperio.elastic.exception.EsException;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class EsUtils {

    private static final Logger logger = LoggerFactory.getLogger(EsUtils.class);
    private static final Header JSON_HEADER = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8");
    private static final boolean DRY_RUN = false;
    private static final String BODY = "body";
    private static final String ENDPOINT = "endpoint";
    private static final String METHOD = "method";
    private static final String FROZEN_DATE = "frozen_date";
    private RestClientConnector clientConnector;

    @Autowired
    public EsUtils(RestClientConnector clientConnector) {
        this.clientConnector = clientConnector;
    }

    /**
     * The index operation cannot be performed when determining whether the allocation is turned off.
     *
     * @param client
     * @return
     * @throws Exception
     */
    public static boolean allowAllocation(RestClientConnector client) throws IOException, EsException {
        JSONObject settings = JSON.parseObject(executeCommand(client.getLowLevelClient(), Method.GET.name(), "_cluster/settings", ""));
        try {
            if ("none".equals(settings.getJSONObject("persistent").getJSONObject("cluster").getJSONObject("routing")
                    .getJSONObject("allocation").getString("enable"))) {
                return false;
            }
            if ("none".equals(settings.getJSONObject("transient").getJSONObject("cluster").getJSONObject("routing")
                    .getJSONObject("allocation").getString("enable"))) {
                return false;
            }
        } catch (NullPointerException e) {
            logger.warn("Es can not set allocation params, default is all.");
        }
        return true;
    }

    /**
     * Get all meta information with aliased indexes.
     *
     * @param client
     * @param suffix
     * @return
     * @throws IOException
     * @throws EsException
     */
    public static List<IndexMeta> getMarkIndicesMeta(RestClientConnector client, List<String> whiteList, String suffix) throws IOException, EsException {
        String text = executeCommand(client.getLowLevelClient(), Method.GET.name(), "/_cat/aliases/*$?v", "");
        return CommonUtils.parseCatText(text).stream().filter((json) -> {
            JSONObject indexData = (JSONObject) json;
            String alias = indexData.getString("alias").replace("$", "");
            return whiteList.contains("all") || whiteList.contains(alias);
        }).map(json -> {
            JSONObject indexData = (JSONObject) json;
            String currentIndexName = indexData.getString("index");
            String alias = indexData.getString("alias").replace("$", "");
            String rollOverName = alias + "-" + suffix;
            JSONObject meta = null;
            try {
                JSONObject currentMappings = JSON.parseObject(executeCommand(client.getLowLevelClient(), "GET", "/" + currentIndexName + "/_mappings", ""));
                if (currentMappings.containsKey("mappings")
                        && currentMappings.getJSONObject("mappings").containsKey("data")
                        && currentMappings.getJSONObject("mappings").getJSONObject("data").containsKey("_meta")) {
                    meta = currentMappings.getJSONObject("mappings").getJSONObject("data").getJSONObject("_meta");
                }
            } catch (IOException | EsException e) {
                logger.error(String.format("Can not get %s mappings", currentIndexName), e);
            }
            return new IndexMeta(alias, currentIndexName, rollOverName, meta);
        }).filter((meta) -> !StringUtils.isEmpty(meta.getCurrentIndexName()) && !StringUtils.isEmpty(meta.getAlias())).collect(Collectors.toList());
    }

    public static boolean checkIndexExists(RestClientConnector client, String indexName) {
        try {
            return JSON.parseObject(executeCommand(client.getLowLevelClient(), "GET", indexName, "")).containsKey(indexName);
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean rolloverIndex(RestClientConnector client, IndexMeta indexMeta, String suffix, JSONObject resetCondition) throws IOException, EsException {
        if (checkIndexExists(client, indexMeta.getRollOverIndexName())) {
            logger.error(String.format("Index %s already exists, can not named roll over name", indexMeta.getRollOverIndexName()));
            return false;
        }
        logger.info(String.format("==== Start rollover %s", indexMeta.toString()));
        JSONObject mappings = getMappings(client.getLowLevelClient(), indexMeta.getCurrentIndexName());
        JSONObject settings = getSettings(client.getLowLevelClient(), indexMeta.getCurrentIndexName());
        Map<String, String> params = new ImmutableMap.Builder<String, String>()
                .put("suffix", suffix)
                .put("alias", indexMeta.getAlias())
                .put("mappings", mappings.toJSONString())
                .put("settings", settings.toJSONString())
                .put("conditions", resetCondition == null || resetCondition.isEmpty() ? indexMeta.getConditions().toJSONString() : resetCondition.toJSONString())
                .put("dry_run", String.valueOf(DRY_RUN))
                .build();
        JSONObject result = JSON.parseObject(executeTemplate(client.getLowLevelClient(), "roll_over", params));
        if (result.getBooleanValue("rolled_over") || (result.getBooleanValue("dry_run") && result.getString("conditions").contains("true"))) {
            logger.info(String.format("==== Finish rollover!!! Index = %s, Result = %s", indexMeta.getCurrentIndexName(), result));
        } else {
            logger.error(String.format("Roll over index %s error = %s", indexMeta.getCurrentIndexName(), result));
        }
        return result.getBooleanValue("rolled_over");
    }

    public static void setFrozenDate(RestClientConnector client, IndexMeta indexMeta, String date) throws IOException, EsException {
        String oldName = indexMeta.getCurrentIndexName();
        int count = JSON.parseObject(
                executeCommand(client.getLowLevelClient(), Method.GET.name(), oldName + "/_count", null)
        ).getIntValue("count");
        JSONObject oldMeta = indexMeta.getMeta();
        oldMeta.put(FROZEN_DATE, count == 0 ? "010101" : date);
        JSONObject json = new JSONObject().fluentPut("_meta", oldMeta);
        executeCommand(client.getLowLevelClient(), Method.PUT.name(), oldName + "/_mapping/data", json.toJSONString());
    }

    public static String executeTemplate(RestClient client, String templateName, Map<String, String> params) throws IOException, EsException {
        Map<String, String> template = parseTemplate(templateName, params);
        System.out.println(template);
        return executeCommand(client, template.get(METHOD), template.get(ENDPOINT), template.get(BODY));
        //return new JSONObject().toString();
    }

    private static Map<String, String> parseTemplate(String templateName, Map<String, String> params) throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(Objects.requireNonNull(EsUtils.class.getClassLoader().getResourceAsStream("template/" + templateName))))) {
            StringTokenizer stk = new StringTokenizer(br.readLine());
            String method = stk.nextToken();
            String endpoint = replace(stk.nextToken(""), params).trim();
            StringBuilder body = new StringBuilder();
            String s = null;
            while ((s = br.readLine()) != null) {
                body.append(s);
            }
            return ImmutableMap.of(METHOD, method, ENDPOINT, endpoint, BODY, replace(body.toString(), params));
        }
    }

    private static String executeCommand(RestClient client, String method, String path, String requestBody) throws IOException, EsException {
        logger.info(String.format("Method = %s, Command = %s, requestBody = %s", method, path, requestBody));
        return getResponse(client.performRequest(
                method, path, Collections.emptyMap(), getHttpEntity(method, requestBody), JSON_HEADER
        ));
    }

    private static HttpEntity getHttpEntity(String method, String requestBody) {
        if (HttpHead.METHOD_NAME.equalsIgnoreCase(method) || HttpOptions.METHOD_NAME.equalsIgnoreCase(method) || HttpTrace.METHOD_NAME.equalsIgnoreCase(method)) {
            return null;
        } else {
            return new StringEntity(Optional.ofNullable(requestBody).orElse(""), "UTF-8");
        }
    }

    private static String getResponse(Response response) throws IOException, EsException {
        if (null == response.getEntity()) {
            throw new EsException(String.format("Method %s , url = %s, exe error, code = %s", response.getRequestLine().getMethod(), response.getRequestLine().getUri(), response.getStatusLine().toString()));
        } else {
            return EntityUtils.toString(response.getEntity());
        }
    }

    private static JSONObject getMappings(RestClient client, String index) throws IOException, EsException {
        JSONObject mappings = ((JSONObject) JSON.parseObject(
                executeCommand(client, Method.GET.name(), "/" + index + "/_mappings", "")).values().iterator().next()
        ).getJSONObject("mappings");
        Optional.ofNullable(mappings.getJSONObject("data").getJSONObject("properties").getJSONObject("event_time")).filter((json) -> json.containsKey("type") && "date".equals(json.getString("type"))).ifPresent((json) -> json.put("format", "yyyy-MM-dd HH:mm:ss,SSSZ||yyyy-MM-dd HH:mm:ss||yyyyMMdd HHmmss||yyyy-MM-dd||epoch_millis"));
        return mappings;
    }

    private static JSONObject getSettings(RestClient client, String index) throws IOException, EsException {
        JSONObject settings = ((JSONObject) JSON.parseObject(
                executeCommand(client, Method.GET.name(), "/" + index + "/_settings", "")).values().iterator().next()
        ).getJSONObject("settings");
        JSONObject indexSettings = settings.getJSONObject("index");
        indexSettings.remove("provided_name");
        indexSettings.remove("creation_date");
        indexSettings.remove("uuid");
        indexSettings.remove("version");
        List<String> tokens = CommonUtils.getTokens(executeCommand(client, Method.GET.name(), "/_cat/indices/" + index, ""));
        double size = CommonUtils.getSize(tokens.get(8));
        int shards = size > CommonUtils.getSize("1gb") ? 5 : (size > CommonUtils.getSize("50mb") ? 2 : 1);
        indexSettings.put("number_of_shards", shards);
        return settings;
    }

    private static String replace(String source, Map<String, String> params) {
        for (Map.Entry<String, String> entry : params.entrySet()) {
            source = source.replace("${" + entry.getKey() + "}", entry.getValue());
        }
        return source;
    }

}
