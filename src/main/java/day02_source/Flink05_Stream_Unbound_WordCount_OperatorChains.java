package day02_source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: Gm
 * @Date: 2021/8/9 11:24
 */

public class Flink05_Stream_Unbound_WordCount_OperatorChains {
    public static void main(String[] args) {
        //1.获取流的执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取流的执行环境方式可以查看ui界面
        StreamExecutionEnvironment localEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //设置并行度为1
        localEnv.setParallelism(1);

        //获取无界数据
        DataStreamSource<String> dataStreamSource = localEnv.socketTextStream("hadoop102", 9999);

        //flatMap
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        });


    }
}
