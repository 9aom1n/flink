package day03.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Gm
 * @Date: 2021/8/9 19:19
 */

public class Flink12_Repartition {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<String> map = dataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        }).setParallelism(2);

        //keyBy
        KeyedStream<String, String> keyedStream = map.keyBy(s -> s);

        //shuffle
        DataStream<String> shuffle = map.shuffle();

        //rebalance
        DataStream<String> rebalance = map.rebalance();

        //rescale
        DataStream<String> rescale = map.rescale();

        map.print().setParallelism(2);
        keyedStream.print("keyedStream");
        shuffle.print("shuffle");
        rebalance.print("rebalance");
        rescale.print("rescale");
        env.execute();
    }
}
