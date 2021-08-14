package day04;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: Gm
 * @Date: 2021/8/10 16:13
 */

public class Flink01_RuntimeMode_WordCount {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置运行时模式
        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //并行度设置为1
        executionEnvironment.setParallelism(1);

        //获取数据
        DataStreamSource<String> dataStreamSource = executionEnvironment.socketTextStream("hadoop102", 9999);

        //flatMap -> 将数据按照空格切分成二元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDStream = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] split = s.split(" ");
                for (String word : split) {
                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        });

        //将相同的key的数据聚合到一起
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = wordToOneDStream.keyBy(0);

        // 累加操作
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        sum.print();
        executionEnvironment.execute();
    }
}
