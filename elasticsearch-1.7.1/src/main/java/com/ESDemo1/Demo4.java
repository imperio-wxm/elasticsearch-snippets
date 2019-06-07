package com.ESDemo1;

import com.ESDemo1.javabean.AppleBean;
import com.ESDemo1.utils.JsonUtil;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 * Created by wxmimperio on 2015/9/28.
 * 查询操作集合
 */
public class Demo4 {
    private Client client;

    public Demo4() {
        this("localhost");
    }

    public Demo4(String IPAddress) {
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

    /* MultiSearch
     * 该接口虽然可以同时执行不同过的查询
     * 但无法对最终结果自动分页，而且存在重复内容
     */
    public void MultiSearch() {

        //设定查询项和返回集大小
        SearchRequestBuilder srb1 = client.prepareSearch()
                .setQuery(QueryBuilders.queryStringQuery("color:yellow"))
                .setSize(3);
        SearchRequestBuilder srb2 = client.prepareSearch()
                .setQuery(QueryBuilders.termQuery("color", "blue"))
                .setSize(4);

        MultiSearchResponse sr = client.prepareMultiSearch()
                .add(srb1)
                .add(srb2)
                .execute()
                .actionGet();

        //外层循环得到结果集
        System.out.println("This is MultiSearch");
        for (MultiSearchResponse.Item item : sr.getResponses()) {
            //内层循环完整显示
            SearchResponse searchResponse = item.getResponse();
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                System.out.println(hit.getSourceAsString());
            }
            System.out.println("==========================");
        }
    }

    /* MatchQuery
     * 能够使用某一字段的值对文档进行检索
     */
    public void MatchQuery() {
        SearchResponse searchResponse = client.prepareSearch("myinfo1")
                .setTypes("info1")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery("color", "yellow"))
                .setFrom(0)
                .setSize(10)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is MatchQuery");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /* MatchAllQuery
     * 用于匹配文档中的所有字段
     * 相当于关系数据库中的select * from
     */
    public void MatchAllQuery() {
        SearchResponse searchResponse = client.prepareSearch()
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchAllQuery())
                .setFrom(0)
                .setSize(10)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is MatchAllQuery");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /* MultiMatchQuery
     * MultiMatchQuery中的字段列表参数只有一个时，同MatchQuery
     * 而字段有field1、field2...多个时用MultiMatchQuery
     */
    public void MultiMatchQuery() {
        SearchResponse searchResponse = client.prepareSearch("myinfo1")
                .setTypes("info1")
                .setQuery(QueryBuilders.multiMatchQuery("yellow", "color"))
                .setFrom(0)
                .setSize(10)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is MultiMatchQuery");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /* BoolQuery
     * bool组合查询
     */
    public void BoolQuery() {
        SearchResponse searchResponse = client.prepareSearch("myinfo1")
                .setTypes("info1")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("color", "blue"))
                        .mustNot(QueryBuilders.termQuery("size", 99))
                        .should(QueryBuilders.termQuery("price", 7.3f)))
                .setSize(10)
                .setFrom(0)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is BoolQuery");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /* WildcardQuery
     * 是在指定的字段中检索含有通配符的查询词
     */
    public void WildcardQuery() {
        SearchResponse searchResponse = client.prepareSearch()
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.wildcardQuery("color", "*hello"))
                .setFrom(0)
                .setSize(10)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is WildcardQuery");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /* queryStringQuery
     * 支持Lucene所有的查询语法
     */
    public void QueryStringQuery() {
        SearchResponse searchResponse = client.prepareSearch()
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.queryStringQuery("+blue+pink -yellow"))
                .setFrom(0)
                .setSize(10)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is queryStringQuery");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /* MoreLikeThis
     * 查询到与所提供的文本相似的文档
     */
    public void MoreLikeThis() {
        SearchResponse searchResponse = client.prepareSearch("myinfo2")
                .setTypes("info2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.moreLikeThisQuery("color")
                        .likeText("blue yellow pink white").boost(1.0f).minTermFreq(1))
                .setSize(10)
                .setFrom(0)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is MoreLikeThis");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    public static void main(String[] args) {
        Demo4 demo4 = new Demo4();
        demo4.test1();

        demo4.MultiSearch();
        demo4.MatchQuery();
        demo4.MatchAllQuery();
        demo4.MultiMatchQuery();
        demo4.BoolQuery();
        demo4.WildcardQuery();
        demo4.QueryStringQuery();
        demo4.MoreLikeThis();
    }
}
