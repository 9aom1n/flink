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
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 使用侧输出流（sideOutput）来处理窗口关闭之后的迟到数据
 *
 * @Author: Gm
 * @Date: 2021/8/12 10:12
 */

/**
 * todo 允许吃到数据+侧输出流的作用：尽量快速的提供一个近似准确的结果，为了保证时效性，
 *                                然后加上允许迟到的数据+侧输出流得到最终的数据，这样
 *                                也不用维护大量的窗口，性能也就会好很多
 */
public class Flink08_WaterMark_Window_AllowedLateness_Output {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(2);

        //从端口获取数据
        DataStreamSource<String> dataStreamSource = executionEnvironment.socketTextStream("localhost", 9999);

        //将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                return waterSensor;
            }
        });

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDStream.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000;
            }
        }));

        //按照id分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(r -> r.getId());

        //创建 一个基于事件时间的滚动窗口
        //允许窗口晚两秒关闭，允许迟到的时间为2s
        //todo sideOutputLateData(new OutputTag<WaterSensor>("output")): 将关窗之后来的数据输入到侧输出流“output”
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5))).allowedLateness(Time.seconds(2)).sideOutputLateData(new OutputTag<WaterSensor>("output"){});

        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "This key:" + s + "--window:[" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") There are a total:" + elements.spliterator().estimateSize();
                out.collect(msg);
            }
        });
        process.print("正常：");
        process.getSideOutput(new OutputTag<WaterSensor>("output"){}).print("迟到：");
        executionEnvironment.execute();
    }
}
