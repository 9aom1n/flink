package day03.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: Gm
 * @Date: 2021/8/6 12:48
 */

public class Flink05_Transform_KeyBy {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度为1
        env.setParallelism(1);

        //获取无界数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/word.txt");
        //3. flatMap -> 将数据按照空格切分成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDStream = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        });

        //4.将相同key的数据聚合到一起
        KeyedStream<Tuple2<String, Long>, String> wordKeyByDStream = wordToOneDStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return stringLongTuple2.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> wordToSum = wordKeyByDStream.sum(1);
        wordToSum.print();

        env.execute();

    }
}
