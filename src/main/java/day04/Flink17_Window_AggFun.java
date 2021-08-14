package day04;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: Gm
 * @Date: 2021/8/10 19:57
 */

public class Flink17_Window_AggFun {
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

        //开启一个基于时间的滚动窗口
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(20)));

        //todo 使用窗口函数 -> 增量聚合函数 reduceFun 输入输出的类型要一致
        SingleOutputStreamOperator<Long> aggregate = window.aggregate(new AggregateFunction<WaterSensor, Long, Long>() {
            //创建累加器
            @Override
            public Long createAccumulator() {
                System.out.println("初始化累加器");
                return 0L;
            }

            //累加操作
            @Override
            public Long add(WaterSensor waterSensor, Long aLong) {
                System.out.println("累加操作。。。");
                return waterSensor.getVc() + aLong;
            }

            //返回结果
            @Override
            public Long getResult(Long aLong) {
                System.out.println("返回结果");
                return aLong;
            }

            @Override
            public Long merge(Long a, Long b) {
                return a + b;
            }
        });
        aggregate.print();
        env.execute();
    }
}
