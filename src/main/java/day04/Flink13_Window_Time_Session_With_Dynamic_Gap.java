package day04;

import bean.WaterSensor;
import org.apache.commons.collections.Factory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: Gm
 * @Date: 2021/8/10 19:44
 */

public class Flink13_Window_Time_Session_With_Dynamic_Gap {
    public static void main(String[] args) throws Exception {
        //创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        
        //从端口获取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);
        
        //将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = dataStreamSource.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String s, Collector<WaterSensor> collector) throws Exception {
                String[] words = s.split(" ");
                WaterSensor waterSensor = new WaterSensor(words[0], Long.parseLong(words[1]), Integer.parseInt(words[2]));
                collector.collect(waterSensor);
            }
        });

        //将相同的单词聚合到一个分区
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDStream.keyBy("id");

        //开启一个基于时间的会话窗口（动态间隔）
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<WaterSensor>() {
            @Override
            public long extract(WaterSensor element) {
                return element.getTs() * 1000;
            }
        }));

        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "窗口：[" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") " + "一共有" + elements.spliterator().estimateSize() + "条数据";
                out.collect(msg);
            }
        });

        process.print();
        env.execute();
    }
}
