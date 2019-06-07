package com.ESDemo1;

import com.ESDemo1.javabean.AppleBean;
import com.ESDemo1.utils.JsonUtil;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.common.text.Text;
import java.util.Map;

/**
 * Created by wxmimperio on 2015/9/29.
 * 统计分析
 */
public class Demo6 {

    private Client client;

    public Demo6() {
        this("localhost");
    }

    public Demo6(String IPAddress) {
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
        String data3 = JsonUtil.obj2JsonData(new AppleBean("yellow", 40, 89.3f));
        String data4 = JsonUtil.obj2JsonData(new AppleBean("yellow", 10, 5.3f));
        String data5 = JsonUtil.obj2JsonData(new AppleBean("blue", 99, 7.3f));
        String data6 = JsonUtil.obj2JsonData(new AppleBean("pink", 200, 15.3f));
        String data7 = JsonUtil.obj2JsonData(new AppleBean("yellow", 10, 19.5f));

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.add(client.prepareIndex("myinfo1", "info1", "111").setSource(data2));
        bulkRequestBuilder.add(client.prepareIndex("myinfo1", "info1", "222").setSource(data3));
        bulkRequestBuilder.add(client.prepareIndex("myinfo2", "info2", "333").setSource(data4));
        bulkRequestBuilder.add(client.prepareIndex("myinfo2", "info2", "444").setSource(data5));
        bulkRequestBuilder.add(client.prepareIndex("myinfo2", "info2", "555").setSource(data6));
        bulkRequestBuilder.add(client.prepareIndex("myinfo2", "info2", "666").setSource(data7));

        BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();

        if (bulkResponse.hasFailures()) {
            System.out.println("批量添加失败");
        } else {
            System.out.println("批量添加成功");
        }
    }

    /* Facets（已经不用）
     * termsFacet和DateHistogramFacet用法
     */
    public void Facets() {
        //在查询中构建terms facets统计，统计在color中的词频
        SearchResponse searchResponse = client.prepareSearch("myinfo2")
                .setTypes("info2")
                .setQuery(QueryBuilders.matchAllQuery())
                .addFacet(FacetBuilders.termsFacet("facet1").field("color"))
                .execute().actionGet();

        TermsFacet termsFacet = (TermsFacet) searchResponse.getFacets().facetsAsMap().get("facet1");

        System.out.println("This is Facets");
        for (TermsFacet.Entry entry : termsFacet.getEntries()) {
            System.out.println(entry.getTerm() + "===" + entry.getCount());
        }
    }


    /* Aggregations
     * ES1.0以上版本用
     */
    public void Aggregations() {
        //在color字段中对yellow的统计
        //在父统计中完成对size的统计
        //在子统计中完成对price的统计
        SearchResponse searchResponse = client.prepareSearch("myinfo2")
                .setTypes("info2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("color", "yellow"))
                .addAggregation(AggregationBuilders.terms("sizeAgg").field("size")
                        .subAggregation(AggregationBuilders.terms("priceAgg").field("price")))
                .setSize(100)
                .setFrom(0)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is Aggregations");
        //得到父类统计结果
        Terms termsSize = searchResponse.getAggregations().get("sizeAgg");
        for (Terms.Bucket bucket : termsSize.getBuckets()) {
            System.out.println(bucket.getKey() + "===" + bucket.getDocCount());

            //得到子类统计结果
            Terms termsColor = bucket.getAggregations().get("priceAgg");
            for (Terms.Bucket bucket1 : termsColor.getBuckets()) {
                System.out.println("\t" + bucket1.getKey() + "===" + bucket1.getDocCount());
            }
        }
    }

    /* 基于Scoll方法的检索结果和分页
     */
    public void Scoll() {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("myinfo2")
                .setTypes("info2")
                .setSearchType(SearchType.SCAN)
                .setScroll(TimeValue.timeValueMinutes(1))//设置过期时间
                .setFrom(0)
                .setSize(3)
                .setExplain(true);//对文档的打分情况进行说明
        //到每一个数据分片上去检索数据
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        //获取检索结果的context
        String sid = searchResponse.getScrollId();

        System.out.println("This is Scoll");
        while (true) {
            //检验此次检索的生命周期
            searchResponse = client.prepareSearchScroll(sid)
                    .setScroll(TimeValue.timeValueMinutes(1))
                    .execute().actionGet();

            for (SearchHit hit : searchResponse.getHits().getHits()) {
                System.out.println(hit.getSourceAsString());
            }
            if (searchResponse.getHits().getHits().length == 0) {
                break;
            }
            System.out.println("====================");
        }
    }

    /* 高亮显示检索词
     * addHighlightedField
     * setHighlighterPreTags、setHighlighterPostTags添加HTML前后缀
     */
    public void AddHighlightedField() {
        SearchResponse searchResponse = client.prepareSearch("myinfo2")
                .setTypes("info2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("color", "yellow"))
                .addHighlightedField("color")//确定高亮字段
                .setHighlighterPreTags("<span style=\"color:red\">")
                .setHighlighterPostTags("</span>")
                .setFrom(0)
                .setSize(10)
                .setExplain(true)
                .execute().actionGet();

        System.out.println("This is AddHighlightedField");
        for (SearchHit hit : searchResponse.getHits()) {
            Map<String, HighlightField> result = hit.getHighlightFields();
            //从设定的高亮区域中取出字段
            HighlightField highlightColor = result.get("color");

            String color = "";
            if (highlightColor != null) {
                Text[] colorTexts = highlightColor.fragments();

                for (Text text : colorTexts) {
                    color += text;
                }
            }
            System.out.println(color);
        }
    }

    public static void main(String[] args) {
        Demo6 demo6 = new Demo6();
        demo6.test1();
        demo6.Facets();
        demo6.Aggregations();
        demo6.Scoll();
        demo6.AddHighlightedField();
    }
}
