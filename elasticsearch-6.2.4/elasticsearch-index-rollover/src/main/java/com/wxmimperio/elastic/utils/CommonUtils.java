package com.wxmimperio.elastic.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.wxmimperio.elastic.exception.EsException;

import java.util.List;
import java.util.StringTokenizer;

public class CommonUtils {

    public static List<String> getTokens(String s) {
        List<String> list = Lists.newArrayList();
        StringTokenizer stk = new StringTokenizer(s);
        while (stk.hasMoreTokens()) {
            list.add(stk.nextToken().trim());
        }
        return list;
    }

    public static JSONArray parseCatText(String text) {
        JSONArray result = new JSONArray();
        String[] lines = text.split("\n", -1);
        if (lines.length < 1) {
            return result;
        }
        List<String> headers = getTokens(lines[0]);
        for (int i = 1; i < lines.length; i++) {
            List<String> values = getTokens(lines[i]);
            result.add(constructJSON(headers, values));
        }
        return result;
    }

    private static JSONObject constructJSON(List<String> keys, List<String> values) {
        JSONObject json = new JSONObject();
        for (int i = 0; i < keys.size(); i++) {
            json.put(keys.get(i), values.size() > i ? values.get(i) : "");
        }
        return json;
    }

    public static double getSize(String s) throws EsException {
        if (s.endsWith("kb")) {
            return Double.parseDouble(s.substring(0, s.length() - 2)) * 1024;
        } else if (s.endsWith("mb")) {
            return Double.parseDouble(s.substring(0, s.length() - 2)) * 1024 * 1024;
        } else if (s.endsWith("gb")) {
            return Double.parseDouble(s.substring(0, s.length() - 2)) * 1024 * 1024 * 1024;
        } else if (s.endsWith("tb")) {
            return Double.parseDouble(s.substring(0, s.length() - 2)) * 1024 * 1024 * 1024 * 1024;
        } else if (s.endsWith("b")) {
            return Double.parseDouble(s.substring(0, s.length() - 1));
        } else {
            throw new EsException("invalid size " + s);
        }
    }
}
