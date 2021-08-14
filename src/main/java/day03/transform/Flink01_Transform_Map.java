package day03.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Gm
 * @Date: 2021/8/9 9:02
 */

public class Flink01_Transform_Map {
    public static void main(String[] args) throws Exception {
        //1. 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取数据
        DataStreamSource<Integer> dataStreamSource = env.fromElements(1, 2, 3, 4, 5);

        //进行map操作
        SingleOutputStreamOperator<Integer> map = dataStreamSource.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                return integer + 1;
            }
        });
        map.print();
        env.execute();
    }
}
