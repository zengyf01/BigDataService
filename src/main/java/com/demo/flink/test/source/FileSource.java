package com.demo.flink.test.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyf
 * @date 2023/6/15
 * @desc 文件数据源——示例
 */
public class FileSource {

    public static void main(String[] args) throws Exception{
        //构建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度（默认为4）
        executionEnvironment.setParallelism(1);

        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("/Users/zyf/project/my-project/FlinkService/src/main/resources/User");
        stringDataStreamSource.print("stringDataStreamSource");

        executionEnvironment.execute();


    }
}
