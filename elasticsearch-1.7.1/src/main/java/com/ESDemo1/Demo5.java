package com.ESDemo1;

import com.ESDemo1.javabean.AppleBean;
import com.ESDemo1.utils.JsonUtil;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 * Created by wxmimperio on 2015/9/28.
 * 过滤操作集合
 */
public class Demo5 {
    private Client client;

    public Demo5() {
        this("localhost");
    }

    public Demo5(String IPAddress) {
        //开启嗅探模式
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.sniff", true)
                .build();
        //集群连接超时设置
        client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(IPAddress, 9300));
        System.out.println("连接成功");
    }

    public void test1() {
        String data2 = JsonUtil.obj2JsonData(new AppleBean("blue", 30, 20.3f));
        String data3 = JsonUtil.obj2JsonData(new AppleBean("yellow hello", 40, 89.3f));
        String data4 = JsonUtil.obj2JsonData(new AppleBean("yellow", 10, 5.3f));
        String data5 = JsonUtil.obj2JsonData(new AppleBean("blue", 99, 7.3f));
        String data6 = JsonUtil.obj2JsonData(new AppleBean("pink", 200, 15.3f));

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.add(client.prepareIndex("myinfo1", "info1", "111").setSource(data2));
        bulkRequestBuilder.add(client.prepareIndex("myinfo1", "info1", "222").setSource(data3));
        bulkRequestBuilder.add(client.prepareIndex("myinfo2", "info2", "333").setSource(data4));
        bulkRequestBuilder.add(client.prepareIndex("myinfo2", "info2", "444").setSource(data5));
        bulkRequestBuilder.add(client.prepareIndex("myinfo2", "info2", "555").setSource(data6));

        BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();

        if (bulkResponse.hasFailures()) {
            System.out.println("批量添加失败");
        } else {
            System.out.println("批量添加成功");
        }
    }

    /* TermFilter
     * 用于在指定的字段中设定要获取的关键词
     */
    public void TermFilter() {
        SearchResponse searchResponse = client.prepareSearch("myinfo2")
                .setTypes("info2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(FilterBuilders.termFilter("color", "blue"))
                .setFrom(0)
                .setSize(10)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is TermFilter");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /* ExistsFilter
     * 选出含有指定字段的结果集列表
     */
    public void ExistsFilter() {
        SearchResponse searchResponse = client.prepareSearch("myinfo2")
                .setTypes("info2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(FilterBuilders.existsFilter("size"))
                .setFrom(0)
                .setSize(10)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is ExistsFilter");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /* QueryFilter
     * 可以把queryString子句包含在queryFilter中
     * 并作为setPostFilter()的参数
     */
    public void QueryFilter() {
        SearchResponse searchResponse = client.prepareSearch()
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(FilterBuilders.queryFilter(QueryBuilders.queryStringQuery("+yellow+blue -hello")))
                .setFrom(0)
                .setSize(10)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is QueryFilter");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /* RangeFilter
     * 用于对指定范围内容的选择过滤
     * 用from、to方法设定范围的起点和终点
     * includeLower(true or false)、includeUpper(true or false)可以设定是否允许大小写
     * gte、gt、lte、lt分别表示大于等于、大于、小于等于、小于
     */
    public void RangeFilter() {
        SearchResponse searchResponse = client.prepareSearch("myinfo2")
                .setTypes("info2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(FilterBuilders.rangeFilter("size")
                        .from(0).to(20))
                .setSize(10)
                .setFrom(0)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is RangeFilter");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /* TypeFilter
     * 返回指定类型的所有文档
     * 当查询被指定向到多个索引或者一个有大量不同数据类型的索引上时，该过滤器是有用的
     */
    public void TypeFilter() {
        SearchResponse searchResponse = client.prepareSearch("myinfo2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setPostFilter(FilterBuilders.typeFilter("info2"))
                .setFrom(0)
                .setSize(10)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is TypeFilter");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    public void NotFilter() {
        SearchResponse searchResponse = client.prepareSearch("myinfo2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setPostFilter(FilterBuilders.notFilter(
                        FilterBuilders.rangeFilter("size").from(0).to(20)
                ))
                .setFrom(0)
                .setSize(10)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is NotFilter");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    public void OrFilter() {
        SearchResponse searchResponse = client.prepareSearch("myinfo2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setPostFilter(FilterBuilders.orFilter(
                        //第一个条件
                        FilterBuilders.termFilter("color", "blue"),
                        //第二个条件
                        FilterBuilders.termFilter("size", 10)
                ))
                .setFrom(0)
                .setSize(10)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is OrFilter");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    public void AndFilter() {
        SearchResponse searchResponse = client.prepareSearch("myinfo2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setPostFilter(FilterBuilders.andFilter(
                        //第一个条件
                        FilterBuilders.prefixFilter("color", "blue"),
                        //第二个条件
                        FilterBuilders.termFilter("size", 99)
                ))
                .setFrom(0)
                .setSize(10)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is AndFilter");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    public static void main(String[] args) {

        Demo5 demo5 = new Demo5();
        demo5.test1();
        demo5.TermFilter();
        demo5.ExistsFilter();
        demo5.QueryFilter();
        demo5.RangeFilter();
        demo5.TypeFilter();
        demo5.NotFilter();
        demo5.OrFilter();
        demo5.AndFilter();
    }
}
