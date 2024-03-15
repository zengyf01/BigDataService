package com.demo.flink.test.transformation;

import com.demo.flink.test.bean.UserTest;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;

/**
 * @author zyf
 * @date 2023/6/16
 * @desc 富函数——示例（汇总员工总工时）
 * 富函数：支持open和close调用，支持获取上下午信息
 */
public class TransformationRichFunction {


    public static void main(String[] args) throws Exception {

        //构建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度（默认为4）
        executionEnvironment.setParallelism(1);
        //读取文件数据
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("/Users/zyf/project/my-project/FlinkService/src/main/resources/User");


        //map转换——转换数据长度
        DataStream<UserTest> richFuncationDataStreamSource =  stringDataStreamSource.map(new RichMapFunction<String, UserTest>() {
            //一般用来初始化状态、数据库连接
            @Override
            public void open(Configuration parameters) throws Exception {
                //super.open(parameters);
                System.out.println("建立数据库连接");
            }
            //一般用来清理状态、短开数据库连接
            @Override
            public void close() throws Exception {
                //super.close();
                System.out.println("断开数据库连接");
            }
            @Override
            public UserTest map(String s) throws Exception {
                //可以获取运行上下午对象
                RuntimeContext runtimeContext = this.getRuntimeContext();
                return new UserTest(s.split(",")[0],Integer.valueOf((String) s.split(",")[1]), Integer.valueOf((String) s.split(",")[2]),BigDecimal.valueOf(Long.parseLong( s.split(",")[3])));
            }
        });

        richFuncationDataStreamSource.print("");

        executionEnvironment.execute();


    }
}
