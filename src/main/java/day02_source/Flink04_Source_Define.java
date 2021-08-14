package day02_source;

import bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.net.Socket;

/**
 * @Author: Gm
 * @Date: 2021/8/7 17:01
 */

public class Flink04_Source_Define {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


    }

    public static class MySource implements SourceFunction<WaterSensor> {
        private String host;
        private int port;
        private volatile boolean isRuning = true;
        private Socket socket;

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {

            // 实现一个从socket读取数据的source

        }

        @Override
        public void cancel() {

        }
    }
}
