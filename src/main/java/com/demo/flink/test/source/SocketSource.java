package com.demo.flink.test.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyf
 * @date 2023/6/20
 * @desc socket数据源——示例
 *
 * nc -lk 7788
 */
public class SocketSource {

    public static void main(String[] args) throws Exception {
        //构建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度，默认为4
        executionEnvironment.setParallelism(1);
        DataStreamSource<String> sockertStreamSource =  executionEnvironment.socketTextStream("localhost",7788);

        sockertStreamSource.print("sockertStreamSource");

        executionEnvironment.execute();
    }
}
