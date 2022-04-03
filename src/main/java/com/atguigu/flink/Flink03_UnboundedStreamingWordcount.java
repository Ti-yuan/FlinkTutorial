package com.atguigu.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @AUTHOR: Maynard
 * @DATE: 2022/04/03 23:59
 **/

public class Flink03_UnboundedStreamingWordcount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lineDSS = env.socketTextStream("hadoop162", 9999);

        SingleOutputStreamOperator<Tuple2<String, Long>> data = lineDSS
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String WordLine, Collector<String> out) throws Exception {
                        for (String word : WordLine.split(" ")) {
                            out.collect(word);
                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String word) throws Exception {
                        return Tuple2.of(word, 1L);
                    }
                });

        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = data.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);
        result.print();

        env.execute();
    }
}
