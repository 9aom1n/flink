package day03.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Gm
 * @Date: 2021/8/9 11:39
 */

public class Flink03_Transform_RichMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/word.txt");


        SingleOutputStreamOperator<String> map = dataStreamSource.map(s -> s);

        map.print();

        env.execute();
    }


    public static class MyMap extends RichMapFunction<String, String> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open");
        }

        @Override
        public String map(String s) throws Exception {
            System.out.println(getRuntimeContext().getTaskNameWithSubtasks());
            return s + 1;
        }

        @Override
        public void close() throws Exception {
            System.out.println("close");
        }
    }

}

