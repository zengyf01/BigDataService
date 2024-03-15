package com.demo.flink.test.transformation;

import com.demo.flink.test.bean.UserTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;

/**
 * @author zyf
 * @date 2023/6/16
 * @desc 转换算子——示例（汇总员工总工时）
 */
public class TransformationSum {


    public static void main(String[] args) throws Exception {

        //构建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度（默认为4）
        executionEnvironment.setParallelism(1);
        //读取文件数据
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("/Users/zyf/project/my-project/FlinkService/src/main/resources/User");


        //map转换——转换数据长度
        SingleOutputStreamOperator<UserTest> mapDataStreamSource =  stringDataStreamSource.map(new MapFunction<String, UserTest>() {
            @Override
            public UserTest map(String s) throws Exception {

                return new UserTest(s.split(",")[0],Integer.valueOf((String) s.split(",")[1]), Integer.valueOf((String) s.split(",")[2]),BigDecimal.valueOf(Long.parseLong( s.split(",")[3])));
            }
        });

        //按姓名（name）分组
        KeyedStream<UserTest, Tuple> keyByDataStreamSource = mapDataStreamSource.keyBy("name");
        //分组后按工时字段（workingHours）sum
        SingleOutputStreamOperator<UserTest> sumDataStreamSource = keyByDataStreamSource.sum("workingHours");
        sumDataStreamSource.print("sum");

        executionEnvironment.execute();


    }
}
