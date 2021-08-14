package day04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: Gm
 * @Date: 2021/8/10 19:14
 */

public class Flink12_Window_Time_Session {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //从端口获取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        //将数据转换为Tuple2
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDStream = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] split = s.split(" ");
                for (String word : split) {
                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        });
        //将同一个单词聚合到一个分区
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = wordToOneDStream.keyBy(0);

        //todo 开启一个基于时间的滚动窗口
        WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> window = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {

            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                String msg = "窗口：[" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") " + "一共有" + elements.spliterator().estimateSize() + "条数据";
                out.collect(msg);
            }
        });
        process.print();
        window.sum(1).print();
        env.execute();
    }
}
