package com.demo.flink.test.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zyf
 * @date 2023/6/16
 * @desc 基础转换算子——示例
 */
public class TransformationBase {


    public static void main(String[] args) throws Exception {

        //构建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度（默认为4）
        executionEnvironment.setParallelism(1);
        //读取文件数据
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("/Users/zyf/project/my-project/FlinkService/src/main/resources/User");


        /**map转换——转换数据长度*/
        SingleOutputStreamOperator<Integer> mapDataStreamSource =  stringDataStreamSource.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });

        mapDataStreamSource.print("mapDataStreamSource");

        /**flatMap转换——按逗号分割数据*/
        SingleOutputStreamOperator flatMapDataStreamSource = stringDataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String item : s.split(",")) {
                    collector.collect(item);
                }
            }
        });

        flatMapDataStreamSource.print("flatMapDataStreamSource");

        /**filter转换——过滤不满足条件的数据*/
        SingleOutputStreamOperator filterDataStreamSource = stringDataStreamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {

                return s.startsWith("李四");
            }
        });
        filterDataStreamSource.print("filterDataStreamSource");



        executionEnvironment.execute();


    }
}
