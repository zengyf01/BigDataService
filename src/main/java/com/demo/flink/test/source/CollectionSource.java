package com.demo.flink.test.source;

import com.demo.flink.test.bean.UserTest;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * @author zyf
 * @date 2023/6/15
 * @desc 集合数据源——示例
 */
public class CollectionSource {

    public static void main(String[] args) throws Exception{
            //构建执行环境
            StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
            //设置并行度，默认为4
            executionEnvironment.setParallelism(1);

            //设置dataStream集合数据
            DataStreamSource<UserTest> userDataStreamSource = executionEnvironment.fromCollection(Arrays.asList(new UserTest("张三", 15, 202305,BigDecimal.valueOf(new Long(15000L))), new UserTest("李四", 20,202305,BigDecimal.valueOf(new Long(15000L)))));
            //打印流数据
            userDataStreamSource.print("userDataStreamSource");

            executionEnvironment.execute();
    }
}
