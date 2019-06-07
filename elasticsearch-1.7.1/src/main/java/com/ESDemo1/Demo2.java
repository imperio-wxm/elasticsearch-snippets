package com.ESDemo1;

import com.ESDemo1.javabean.AppleBean;
import com.ESDemo1.utils.JsonUtil;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Created by wxmimperio on 2015/9/25.
 * 批量索引文档
 */
public class Demo2 {
    private Client client;

    public Demo2() {
        this("localhost");
    }

    public Demo2(String IPAddress) {
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

            if (bulkResponse.iterator().hasNext()) {
                System.out.println(bulkResponse.iterator().next().getIndex());
                System.out.println(bulkResponse.iterator().next().getType());
                System.out.println(bulkResponse.iterator().next().getId());
            }
            //计数
            test2(client);
        }
    }

    //计数统计
    public void test2(Client client) {
        CountResponse countResponse = client.prepareCount("myinfo1")
                .setQuery(QueryBuilders.termQuery("color", "yellow")).execute().actionGet();
        //返回被查询命中的索引文档数量
        System.out.println(countResponse.getCount());
    }

    public static void main(String[] args) {
        Demo2 demo2 = new Demo2();
        demo2.test1();
    }
}
