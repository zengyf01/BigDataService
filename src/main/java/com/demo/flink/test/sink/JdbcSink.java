package com.demo.flink.test.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author zyf
 * @date 2023/6/19
 * @desc 输出数据到数据库——示例
 */
public class JdbcSink {

    public static void main(String[] args) {
        //构建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度（默认为4）
        executionEnvironment.setParallelism(1);
        //从文件获取数据
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("/Users/zyf/project/my-project/FlinkService/src/main/resources/User");

        stringDataStreamSource.addSink(new MyCustomeMysqlSkinkFuncation());
    }


    public static  class MyCustomeMysqlSkinkFuncation extends RichSinkFunction<String> {

        //创建mysql连接
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        //关闭mysql连接
        @Override
        public void close() throws Exception {
            super.close();
        }


        @Override
        public void invoke(String value, Context context) throws Exception {
            super.invoke(value, context);
        }
    }
}
