package com.fibabanka.dt.kafka.util;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class GoldenGateJsonParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoldenGateJsonParser.class);

    public static String parseMetaAsJson(String text) {
        return text;
    }

    public static String parseTableAsJson(String text) {
        JSONObject jsonObject = null;
        JSONObject jsonObjectTable = null;
        try {
            jsonObject = new JSONObject(text);
            jsonObjectTable = new JSONObject(jsonObject.get("after").toString());
        } catch (JSONException e) {
            LOGGER.error("Error occurred when JSON parsing operation. Data: " + text
                    + " has been tried to be parsed.", e);
        }
        return jsonObjectTable.toString();
    }

    public static String parseTableAsStringJson(String text) {
        JSONObject jsonObject = null;
        JSONObject jsonObjectTable = null;
        JSONObject jsonObjectNew = new JSONObject();
        try {
            jsonObject = new JSONObject(text);
            jsonObjectTable = new JSONObject(jsonObject.get("after").toString());

            Iterator<String> tableKeys = jsonObjectTable.keys();
            while (tableKeys.hasNext()) {
                String key = tableKeys.next();
                jsonObjectNew.put(key, jsonObjectTable.get(key).toString());
            }

        } catch (JSONException e) {
            LOGGER.error("Error occurred when JSON parsing operation. Data: " + text
                    + " has been tried to be parsed.", e);
        }
        return jsonObjectNew.toString();
    }

    public static ArrayList<Map<String, String>> parseAsArrayList(String text) {
        ArrayList<Map<String, String>> total = new ArrayList<>();
        Map<String, String> meta = new HashMap<>();
        Map<String, String> table = new HashMap<>();
        try {
            JSONObject jsonObject = new JSONObject(text);

            //meta table iteration
            Iterator<String> metaKeys = jsonObject.keys();
            while (metaKeys.hasNext()) {
                String key = metaKeys.next();
                if (!key.toLowerCase().equals("after")) {
                    meta.put(key.toLowerCase(), jsonObject.get(key).toString().replace("\t", " "));
                }
            }

            // oracle table
            JSONObject jsonObjectTable = new JSONObject(jsonObject.get("after").toString());
            Iterator<String> tableKeys = jsonObjectTable.keys();
            while (tableKeys.hasNext()) {
                String key = tableKeys.next();
                table.put(key.toLowerCase(), jsonObjectTable.get(key).toString().replace("\t", " "));
            }
            total.add(meta);
            total.add(table);
        } catch (JSONException e) {
            LOGGER.error("Error occurred when JSONObject operation", e);

        }
        return total;
    }
}