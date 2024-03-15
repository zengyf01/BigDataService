package com.demo.flink.test.transformation;

import com.demo.flink.test.bean.UserTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;

/**
 * @author zyf
 * @date 2023/6/17
 * @desc 转换算子——示例 (汇总员工截止到当前总工时、总工资)
 */
public class TransformationReduce {


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

                return new UserTest(s.split(",")[0],Integer.valueOf((String) s.split(",")[1]), Integer.valueOf((String) s.split(",")[2]), BigDecimal.valueOf(Long.parseLong( s.split(",")[3])));
            }
        });

        //按姓名（name）分组
        KeyedStream<UserTest, Tuple> keyByDataStreamSource = mapDataStreamSource.keyBy("name");

        //分组后reduce处理（
        SingleOutputStreamOperator<UserTest> reduceDataStreamSource = keyByDataStreamSource.reduce(new ReduceFunction<UserTest>() {
            @Override
            public UserTest reduce(UserTest stateData, UserTest currentData) throws Exception {
                //将上一个reduce过的值(stateData)和当前值（currentData）计算，产生新的值
                return new UserTest(currentData.getName(),stateData.getWorkingHours()+currentData.getWorkingHours(),Math.max(stateData.getTime(),currentData.getTime()),stateData.getWages().add(currentData.getWages()));
            }
        });

        reduceDataStreamSource.print("reduceDataStreamSource");

        executionEnvironment.execute();


    }
}
