package com.demo.flink.test.transformation;

import com.demo.flink.test.bean.UserTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;


/**
 * @author zyf
 * @date 2023/6/16
 * @desc 分区——示例
 */
public class Transformationpartition {


    public static void main(String[] args) throws Exception {

        //构建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度（默认为4）
        executionEnvironment.setParallelism(2);
        //读取文件数据
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("/Users/zyf/project/my-project/FlinkService/src/main/resources/User");

        //  global：元素往第一个分区
        stringDataStreamSource.global().print("global");

        //shuffle:随机分区元素
        stringDataStreamSource.shuffle().print("shuffle");

        //rebalance: 元素均匀分区
        stringDataStreamSource.rebalance().print("rebalance");

        SingleOutputStreamOperator<UserTest> mapDataStreamSource =  stringDataStreamSource.map(new MapFunction<String, UserTest>() {
            @Override
            public UserTest map(String s) throws Exception {

                return new UserTest(s.split(",")[0],Integer.valueOf((String) s.split(",")[1]), Integer.valueOf((String) s.split(",")[2]), BigDecimal.valueOf(Long.parseLong( s.split(",")[3])));
            }
        });

        //keyBy: 根据指定地址hash后取模分区
        mapDataStreamSource.keyBy("name").print("keyBy");


        executionEnvironment.execute();


    }
}
