package day04;

import bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @Author: Gm
 * @Date: 2021/8/10 18:18
 */

public class Flink07_Project_AppAnalysis {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //通过自定义的数据源获取数据
        DataStreamSource<MarketingUserBehavior> dataStreamSource = env.addSource(new AppMarketingDataSource());

        SingleOutputStreamOperator<Tuple2<String, Long>> behaviorToOneDStream = dataStreamSource.map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(MarketingUserBehavior marketingUserBehavior) throws Exception {
                return Tuple2.of(marketingUserBehavior.getBehavior(), 1L);
            }
        });

        //将相同的行为分组
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = behaviorToOneDStream.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        sum.print();

        env.execute();

    }

    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            Integer i = 0;
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior((long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(200);
                i++;
                if (i > 100) {
                    canRun = false;
                }
            }
        }

        @Override
        public void cancel() {
            canRun = false;

        }
    }
}
