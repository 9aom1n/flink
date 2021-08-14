package day03.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Gm
 * @Date: 2021/8/9 9:11
 */

public class Flink04_Transform_Filter {
    public static void main(String[] args) throws Exception {
        //1.创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<Integer> dataStreamSource = env.fromElements(1, 2, 3, 4);

        //3.将传入的数据 过滤掉奇数
        SingleOutputStreamOperator<Integer> filter = dataStreamSource.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer % 2 == 0;
            }
        });

        filter.print();
        env.execute();
    }
}
