package day04;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Gm
 * @Date: 2021/8/10 11:32
 */

public class Flink02_Project_PV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.readTextFile("input\\UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> userBehaviorDStream = dataStreamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] split = s.split(",");
                return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
            }
        });

        SingleOutputStreamOperator<UserBehavior> filterDStream = userBehaviorDStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                if ("pv".equals(userBehavior.getBehavior())) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> mapDStream = filterDStream.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
                return Tuple2.of(userBehavior.getBehavior(), 1L);
            }
        });

        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = mapDStream.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        sum.print();
        env.execute();

    }
}
