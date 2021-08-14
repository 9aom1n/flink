package day02_source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Gm
 * @Date: 2021/8/7 15:33
 */

public class Flink_Source_Hdfs {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取文件
        DataStreamSource<String> dataStreamSource = env.readTextFile("hdfs://hadoop102:8020/input/input/1.txt");

        dataStreamSource.print();

        env.execute();
    }
}
