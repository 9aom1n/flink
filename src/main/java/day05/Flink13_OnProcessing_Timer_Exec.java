package day05;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author: Gm
 * @Date: 2021/8/12 13:37
 */

public class Flink13_OnProcessing_Timer_Exec {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //从端口获取数据
        DataStreamSource<String> dataStreamSource = environment.socketTextStream("localhost", 9999);

        //将数据转化为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                return waterSensor;
            }
        });

        //keyBy 按照id进行分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDStream.keyBy(r -> r.getId());

        /**
         * 监控水位传感器的水位值
         * 如果水位值在五秒钟之内（定时时间是5s）
         * 连续上升（定一个变量用来保存上一次的水位，看是否上升，如果当前水位与上一次水位相等或者小于则不让定时器报警，删除定时器）
         * 则报警，（定时器，重置定时器，为了让下次来的数据知道定时器有没有注册）
         * 并将报警信息输出到侧输出流
         */

        SingleOutputStreamOperator<String> process = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            //用来保存上一次的水位值
            private Integer lastVc = Integer.MIN_VALUE;

            //用来保存定时器的时间
            private Long timer = Long.MIN_VALUE;

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                if (value.getVc() > lastVc) {
                    //证明水位上升
                    if (timer == Long.MIN_VALUE) {
                        //证明没有注册过定时器
                        timer = ctx.timerService().currentProcessingTime() + 10000;
                        System.out.println("注册一个定时器" + timer);
                        ctx.timerService().registerProcessingTimeTimer(timer);
                    }
                } else {
                    //水位没有上升
                    System.out.println("删除定时器" + timer);
                    //删除定时器
                    ctx.timerService().deleteProcessingTimeTimer(timer);
                    //重置定时器
                    timer = Long.MIN_VALUE;
                }
                //将当前的水位赋值给lastVc
                lastVc = value.getVc();
                out.collect(value.toString());
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                ctx.output(new OutputTag<String>("output") {
                }, "报警！！！连续五秒水位上升");
                //重置定时器
                timer = Long.MIN_VALUE;
            }
        });

        process.print("Main:");
        process.getSideOutput(new OutputTag<String>("output"){}).print("Warning:");
        environment.execute();
    }
}
