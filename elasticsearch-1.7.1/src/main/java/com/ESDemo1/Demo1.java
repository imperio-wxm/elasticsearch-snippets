package com.ESDemo1;

import com.ESDemo1.javabean.AppleBean;
import com.ESDemo1.utils.JsonUtil;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import java.util.Set;

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
         * get(GetRequest request)、get(GetRequest request,ActionListener<GetResponse>listener)
         * prepareGet()、prepareGet(String index, String type, String id)
         * multiGet(MultiGetRequest request)、multiGet(MultiGetRequest request,ActionListener<MultiGetResponse>listener)
         * parpareMultiGet()
         */
        GetResponse getResponse = client.prepareGet("myinfo1", "info1", "AVADYFrKItc_nLsmMzK8").execute().actionGet();


      /*UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("myinfo1");
        updateRequest.type("info1");
        updateRequest.id("AVADYFrKItc_nLsmMzK8");

        try {
            updateRequest.doc(
                    XContentFactory.jsonBuilder().startObject()
                            .field("color", "green")
                            .field("size", 10)
                            .field("price", 100.00f)
                            .endObject()
            );
            client.update(updateRequest).get();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }*/

        /* 删除索引文档
         * 常用API
         * prepareDelete()、prepareDelete(String index, String type, String id)
         * delete(DeleteRequest request)、delete(DeleteRequest request, ActionListener<DeleteResponse>listener)
         * deleteByQuery(DeleteByQueryRequest request)、deleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse>listener)
         */
        DeleteResponse deleteResponse = client.prepareDelete("myinfo1", "info1", "AVADYFrKItc_nLsmMzK8").execute().actionGet();

        String _index = response.getIndex();
        String _type = response.getType();
        String _id = response.getId();
        long _version = response.getVersion();

        System.out.println(_index + "\n" + _type + "\n" + _id + "\n" + _version);
        System.out.println(getResponse.getSource());

        boolean isFound = deleteResponse.isFound();
        //返回索引是否存在
        System.out.println(isFound);
        Set headers = deleteResponse.getHeaders();
        //返回头响应
        System.out.println(headers);
    }

    public static void main(String[] args) {
        Demo1 demo1 = new Demo1();
        demo1.test1();
    }
}
