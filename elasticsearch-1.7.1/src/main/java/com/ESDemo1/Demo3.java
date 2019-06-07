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
 * 检索操作
 */
public class Demo3 {
    private Client client;

    public Demo3() {
        this("localhost");
    }

    public Demo3(String IPAddress) {
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

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.add(client.prepareIndex("myinfo1", "info1", "111").setSource(data2));
        bulkRequestBuilder.add(client.prepareIndex("myinfo1", "info1", "222").setSource(data3));
        bulkRequestBuilder.add(client.prepareIndex("myinfo2", "info2", "333").setSource(data4));

        BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();

        if (bulkResponse.hasFailures()) {
            System.out.println("批量添加失败");
        } else {
            System.out.println("批量添加成功");
        }
    }

    public void test2() {
        SearchResponse searchResponse = client.prepareSearch("myinfo1","myinfo2").setTypes("info1","info2")
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("color","yellow"))
                .setPostFilter(FilterBuilders.rangeFilter("size").from(20).to(60))
                .setFrom(0)
                .setSize(100)
                .setExplain(true)
                .execute().actionGet();

        //遍历查询结果
        for(SearchHit hit:searchResponse.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }
    public static void main(String[] args) {
        Demo3 demo3 = new Demo3();
        demo3.test1();
        demo3.test2();
    }
}
