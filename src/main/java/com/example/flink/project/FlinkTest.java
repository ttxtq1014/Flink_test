package com.example.flink.project;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * description: TODO
 * author L
 * date 2024/5/9 17:19
 */
public class FlinkTest {
    public static void main(String[] args) {
        //创建流对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为1
        env.setParallelism(1);

        //Kafka配置信息：properties,参数配置类
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id","my-group");

        //KafkaSource构造器
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")//地址
                .setTopics("first")//主题
                .setStartingOffsets(OffsetsInitializer.earliest())//从最早偏移量开始消费
                .setValueOnlyDeserializer(new SimpleStringSchema())//反序列化器，将kafka中的消息转换为字符串
                .build();
        //将 KafkaSource 添加到 Flink 程序中，并设置不生成水印的 WatermarkStrategy。这里的 "Kafka Source" 是该数据源的名称，用于标识
        DataStreamSource<String> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka Source");
        streamSource.print("flink");

//        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
//                .setBootstrapServers("hadoop102:9092")//地址
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                        .setTopic("second")
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build()
//                )//设置主题，简单序列化字符串
//                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)//消息投递级别，至少一次投递
//                .build();
//        streamSource.sinkTo((Sink<String, ?, ?, ?>) kafkaSink);//返回给Kafka

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
