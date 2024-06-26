package com.flinkstreamingdemo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple;

import java.sql.Timestamp;
import java.util.ArrayList;

public class IotJsonParsing {
    public static ArrayList<ArrayList<Object>> parsing(String iotData) {
        JSONObject obj = JSON.parseObject(iotData);
        ArrayList<ArrayList<Object>> res = new ArrayList<>();

        String deviceCode = (String) obj.get("deviceCode");
        JSONArray devices =JSONArray.parseArray(obj.getString("devices"));
        for(int i = 0; i < devices.size(); i++) {

            JSONObject device = devices.getJSONObject(i);
            String deviceId = device.getString("deviceId");
            JSONArray iotDataArray = JSONArray.parseArray(device.getString("iotDataArray"));

            for(int j = 0; j < channels.size(); j++){
                JSONObject channel = channels.getJSONObject(j);
                String iotDataArrayId = channel.getString("iotDataArrayId");
                Timestamp createTime = channel.getTimestamp("createTime");
                String iotDataArrayString = channels.getString(j);

                ArrayList<Object> temp = new ArrayList<>();
                temp.add(0, deviceCode + '_' + deviceId + '_' + iotDataArrayId);
                temp.add(1, iotDataArrayString);
                temp.add(2, createTime);

                res.add(i, temp);
            }

        }

        return res;
    }

    public static void main(String[] args){
        String energyJson = "";
        ArrayList<ArrayList<Object>> res = parsing(energyJson);
        for (ArrayList<Object> r: res) {
            System.out.println(r.get(0));
            System.out.println(r.get(1));
            System.out.println(r.get(2));
        }
    }
}
