//package com.demo.flink.test.table;
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.TableDescriptor;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//
///**
// * @author zyf
// * @date 2023/7/11
// * @desc
// */
//public class TableTest {
//
//    public static void main(String[] args) throws Exception{
//        //构建执行环境
//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//        //设置并行度（默认为4）
//        executionEnvironment.setParallelism(1);
//
//        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
//
//
//        tableEnvironment.createTable();
//
//        executionEnvironment.execute();
//    }
//}
