package com.xiaopeng;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class EnergyStreaming {

    private static final Properties prop = new Properties();
    private static final String BOOTSTRAP = "10.192.11.83:9092";
    private static final String GROUP_ID = "ems_energy";
    private static final String ZOOKEEPER = "10.192.11.83:2181";
    // kafka的partition分区
    private static final Integer partition = 0;
    // 序列化的方式
    public static final String CONST_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    // 反序列化
    public static final String CONST_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    static {
        prop.put("bootstrap.servers", BOOTSTRAP);
        prop.put("zookeeper.connect", ZOOKEEPER);
        prop.put("group.id", GROUP_ID);
        prop.put("key.deserializer", CONST_DESERIALIZER);
        prop.put("value.deserializer", CONST_DESERIALIZER);
        prop.put("auto.offset.reset", "latest");
        prop.put("max.poll.records", "500");
        prop.put("auto.commit.interval.ms", "1000");
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        // 需要消费的能源topics列表，能源种类分别对应电力、水、天然气、压缩空气
        List<String> topics = new LinkedList<>();
        topics.add("ems_electric_energy");
        topics.add("ems_water_meter");
        topics.add("ems_natural_gas");
        topics.add("ems_compressed_air");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                topics, new SimpleStringSchema(), prop);
        consumer.setStartFromLatest();

        DataStream<String> stream = env.addSource(consumer);
        env.enableCheckpointing(5000);

        stream.print("从kafka接收到的消息");

        SingleOutputStreamOperator<String> energyMySQL = stream.map((MapFunction<String, String>) s -> JSON.parseObject(s, String.class));

        energyMySQL.addSink(new WriteMysqlSink());

        try {
            env.execute("electric energy parsing to mysql job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
