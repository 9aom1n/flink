package day05;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: Gm
 * @Date: 2021/8/11 18:23
 */

public class Flink02_WaterMark_Bounded {
    public static void main(String[] args) throws Exception {
        //创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        /**
         * todo 1.多并行度的情况下，向下游传递Watermark的时候是以广播的方式传递的
         *      2. 总是以最小的那个WaterMark为准
         *      3. 当WaterMark值没有增长的时候不会向下游传递，* 生成不变
         */
        env.setParallelism(2);

        //从端口获取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        //将数据转化为JAVABEAN
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(" ");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                return waterSensor;
            }
        });

        //todo 指定WaterMark以及时间时间 -允许固定延迟
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                }));

        //keyBy 按照id进行分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(waterSensor -> waterSensor.getId());

        //开窗开一个基于时间时间的滚动窗口
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "This key:" + s + "window:[" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") There are a total:" + elements.spliterator().estimateSize();
                out.collect(msg);
            }
        });
        process.print();
        env.execute();
    }
}
