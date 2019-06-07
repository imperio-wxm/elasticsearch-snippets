package com.ESDemo1.utils;
import com.ESDemo1.javabean.AppleBean;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * Created by wxmimperio on 2015/9/25.
 */
public class JsonUtil {
    public static String obj2JsonData(AppleBean appleBean) {
        String jsonData = null;
        try {
            //javabeab序列化为json
            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
            jsonBuilder.startObject()
                    .field("color",appleBean.getColor())
                    .field("size",appleBean.getSize())
                    .field("price",appleBean.getprice())
                    .endObject();
            jsonData = jsonBuilder.string();
            System.out.println(jsonData);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return jsonData;
    }
}
