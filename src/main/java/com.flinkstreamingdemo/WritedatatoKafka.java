package com.xiaopeng;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class WritedatatoKafka {
    //本地的kafka机器列表
    public static final String BROKER_LIST = "10.192.11.83:9092";
    //kafka的topic
    public static final String TOPIC_USER = "USER";
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
        Random r = new Random();

        //构建User对象，在name为hyzs后边加个随机数
        int randomInt = r.nextInt();
        User user = new User();
        user.setName("hyzs" + randomInt);
        user.setId(randomInt);
        //转换成JSON
        String userJson = JSON.toJSONString(user);
        String energyJson = "{\"gwCode\":\"1234567891\",\"devices\":[{\"deviceId\":1,\"channels\":[{\"positiveActiveElectricEnergy\":215928,\"positiveReactiveEnergy\":84181,\"apparentElectricEnergy\":250257,\"reverseActiveEnergy\":0,\"reverseReactiveEnergy\":30988,\"channelId\":1,\"createTime\":1652422897000}]}]}";
        String test = JSON.toJSONString(energyJson);

        //包装成kafka发送的记录
//        ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_USER, partition,
//                null, userJson);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_USER, partition,
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
