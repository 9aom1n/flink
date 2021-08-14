package day04;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: Gm
 * @Date: 2021/8/10 19:57
 */

public class Flink20_Window_NoKeyWindow {
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
//        KeyedStream<WaterSensor, Tuple> keyedStream = flatMap.keyBy("id");


        //todo 在keyBy之前使用窗口是不分key的
        AllWindowedStream<WaterSensor, TimeWindow> windowAll = flatMap.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(20)));

        windowAll.process(new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                out.collect(elements.toString());
            }
        }).print();

        env.execute();
    }
}
