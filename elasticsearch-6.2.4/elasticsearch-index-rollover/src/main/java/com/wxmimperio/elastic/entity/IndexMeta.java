package com.wxmimperio.elastic.entity;

import com.alibaba.fastjson.JSONObject;

public class IndexMeta {

    private String alias;
    private String currentIndexName;
    private String rollOverIndexName;
    // 最大索引size = 1gb
    private String maxSize = "50gb";
    // 最大保存天数 1天
    private String maxAge = "7d";
    // 最大文档数 10
    private long maxDocs = 10000000L;
    // 索引元数据信息
    private JSONObject meta;

    public IndexMeta() {
    }

    public IndexMeta(String alias, String currentIndexName, String rollOverIndexName, JSONObject meta) {
        this.alias = alias;
        this.currentIndexName = currentIndexName;
        this.rollOverIndexName = rollOverIndexName;
        this.meta = meta;
        if (meta != null && !meta.isEmpty()) {
            if (meta.containsKey("max_size")) {
                this.maxSize = meta.getString("max_size");
            }
            if (meta.containsKey("max_age")) {
                this.maxAge = meta.getString("max_age");
            }
            if (meta.containsKey("max_docs")) {
                this.maxDocs = meta.getLong("max_docs");
            }
        } else {
            this.meta = getConditions();
        }
    }

    public JSONObject getConditions() {
        JSONObject condition = new JSONObject();
        condition.put("max_age", maxAge);
        condition.put("max_docs", maxDocs);
        condition.put("max_size", maxSize);
        return condition;
    }

    public String getAlias() {
        return alias;
    }

    public String getCurrentIndexName() {
        return currentIndexName;
    }

    public String getRollOverIndexName() {
        return rollOverIndexName;
    }

    public String getMaxSize() {
        return maxSize;
    }

    public String getMaxAge() {
        return maxAge;
    }

    public long getMaxDocs() {
        return maxDocs;
    }

    public JSONObject getMeta() {
        return meta;
    }

    @Override
    public String toString() {
        return "IndexMeta{" +
                "alias='" + alias + '\'' +
                ", currentIndexName='" + currentIndexName + '\'' +
                ", rollOverIndexName='" + rollOverIndexName + '\'' +
                ", maxSize='" + maxSize + '\'' +
                ", maxAge='" + maxAge + '\'' +
                ", maxDocs=" + maxDocs +
                ", meta=" + meta +
                '}';
    }
}
