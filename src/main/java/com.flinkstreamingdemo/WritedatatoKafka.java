package com.flinkstreamingdemo;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class WritedatatoKafka {
    //本地的kafka机器列表
    public static final String BROKER_LIST = "ip:port";
    //kafka的topic
    public static final String TOPIC_NAME = "topic_name";
    //kafka的partition分区
    public static final Integer partition = 0;

    //序列化的方式
    public static final String CONST_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    //反序列化
    public static final String CONST_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";


    public static void writeToKafka() throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", CONST_SERIALIZER);
        props.put("value.serializer", CONST_SERIALIZER);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //转换成JSON
        String energyJson = "";
        String test = JSON.toJSONString(energyJson);

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, partition,
                null, test);
        //发送到缓存
        producer.send(record);
        // System.out.println("向kafka发送数据:" + userJson);
        System.out.println("向kafka发送数据:" + test);
        //立即发送
        producer.flush();

    }

    public static void main(String[] args) {
        while(true) {
            try {
                //每三秒写一条数据
                TimeUnit.SECONDS.sleep(3);
                writeToKafka();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
