package day01;

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
 * @Author: Gm
 * @Date: 2021/8/6 12:29
 */

public class Flink02_Stream_Bounded_WordCount {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度为1
        env.setParallelism(1);

        //2. 获取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("input\\word.txt");

        //3. flatMap -> 将数据按照空格切出每一个单词
        SingleOutputStreamOperator<String> wordDStream = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        //4. map -> 将单词组成Tuple元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDStream = wordDStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                return Tuple2.of(s, 1L);
            }
        });


        //5. 将相同key的数据聚合到一块
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
