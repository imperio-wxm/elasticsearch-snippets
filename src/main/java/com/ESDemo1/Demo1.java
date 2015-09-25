package com.ESDemo1;

import com.ESDemo1.javabean.AppleBean;
import com.ESDemo1.utils.JsonUtil;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * Created by wxmimperio on 2015/9/25.
 */
public class Demo1 {
    private Client client;

    public Demo1() {
        this("localhost");
    }

    public Demo1(String IPAddress) {
        //开启嗅探模式
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.sniff", true)
                .build();
        //集群连接超时设置
        client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(IPAddress, 9300));
        System.out.println("连接成功");
    }

    public void test1() {
        String data1 = JsonUtil.obj2JsonData(new AppleBean("red", 25, 18.3f));

        /* 准备索引
         * 常用API
         * prepareIndex()
         * prepareIndex(String index, String type)
         * prepareIndex(String index, String type, String id)
         */
        IndexResponse response = client.prepareIndex("myinfo1", "info1").setSource(data1).execute().actionGet();

        /* 获取文档索引 _source
         * 常用API
         * get(GetRequest request)、get(GetRequest request,ActionListener<GetRequest>listener)
         * prepareGet()、prepareGet(String index, String type, String id)
         * multiGet(MultiGetRequest request)、multiGet(MultiGetRequest request,ActionListener<MultiGetRequest>listener)
         * parpareMultiGet()
         */
        GetResponse getResponse = client.prepareGet("myinfo1", "info1", "AVADYFrKItc_nLsmMzK8").execute().actionGet();

        String _index = response.getIndex();
        String _type = response.getType();
        String _id = response.getId();
        long _version = response.getVersion();

        System.out.println(_index + "\n" + _type + "\n" + _id + "\n" + _version);
        System.out.println(getResponse.getSource());
    }

    public static void main(String[] args) {
        Demo1 demo1 = new Demo1();
        demo1.test1();
    }
}
