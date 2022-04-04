package com.atguigu.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @AUTHOR: Maynard
 * @DATE: 2022/04/04 10:01
 **/

public class Flink03_Source_Kakfa {
    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop162:9092,hadoop163:9092,hadoop164:9092");
        properties.setProperty("group.id","Flink03_Source_Kafka");
        properties.setProperty("auto.offset.reset","latest");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //使用官方连接依赖中的FlinkKafkaConsumer创建kafka消费者来从kafka中获取数据
        env.addSource(
                new FlinkKafkaConsumer<>(
                        "test",
                        new SimpleStringSchema(StandardCharsets.UTF_8),
                        properties)
        )
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String WordLine, Collector<String> out) throws Exception {
                        for (String word : WordLine.split(" ")) {
                            out.collect(word);
                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String word) throws Exception {
                        return Tuple2.of(word,1L);
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1)
                .print();

        env.execute();
    }
}
