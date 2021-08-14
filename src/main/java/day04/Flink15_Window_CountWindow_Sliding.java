package day04;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: Gm
 * @Date: 2021/8/10 19:57
 */

public class Flink15_Window_CountWindow_Sliding {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //从端口获取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        //将数据转化为JavaBean
        SingleOutputStreamOperator<WaterSensor> flatMap = dataStreamSource.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String s, Collector<WaterSensor> collector) throws Exception {
                String[] split = s.split(" ");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                collector.collect(waterSensor);
            }
        });

        //
        KeyedStream<WaterSensor, Tuple> keyedStream = flatMap.keyBy("id");

        //TODO 开启一个基于元素个数的滚动窗口
        WindowedStream<WaterSensor, Tuple, GlobalWindow> countWindow = keyedStream.countWindow(5,2);

        SingleOutputStreamOperator<WaterSensor> sum = countWindow.sum("vc");
        sum.print();
        env.execute();
    }
}
