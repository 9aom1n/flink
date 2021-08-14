package day05;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
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
 * @Date: 2021/8/11 19:33
 */

public class Flink05_WaterMark_Custom_Period {
    public static void main(String[] args) throws Exception {
        //创建一个流的执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //从端口获取流的数据
        DataStreamSource<String> dataStreamSource = environment.socketTextStream("localhost", 9999);

        //将数据转化为javaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                return waterSensor;
            }
        });

        //自定义周期型生成的WaterMark
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDStream.assignTimestampsAndWatermarks(new WatermarkStrategy<WaterSensor>() {
                    @Override
                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new MyPeriod(Duration.ofSeconds(2));
                    }
                }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {

                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 10000;
                    }
                })
        );

        //按照id分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(r -> r.getId());

        //开窗 开启一个基于事件时间的滚动窗口
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "This key:" + s + "--window:[" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") There are a total:" + elements.spliterator().estimateSize();
                out.collect(msg);
            }
        });

        process.print();
        environment.execute();
    }

    public static class MyPeriod implements WatermarkGenerator<WaterSensor> {

        private long maxTimestamp;

        private long outOfOrdernessMillis;

        public MyPeriod(Duration maxOutOfOrderness) {
            this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
            this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
        }

        /**
         * 在每一个事件中调用，允许水印生成器检查记住事件时间戳，或者而根据时间本身发出水印
         *
         * @param event
         * @param eventTimestamp
         * @param output
         */
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            //与每个时间所携带的时间戳做对比，获取最大的时间戳
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        }

        /**
         * 周期型的生成WaterMark默认200ms
         *
         * @param output
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("生成WaterMark：" + (maxTimestamp - outOfOrdernessMillis - 1));
            output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
        }
    }
}
