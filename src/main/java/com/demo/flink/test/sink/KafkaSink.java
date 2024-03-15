//package com.demo.flink.test.sink;
//
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
//
///**
// * @author zyf
// * @date 2023/6/18
// * @desc 输出数据到kafka——示例
// */
//public class KafkaSink {
//
//
//    public static void main(String[] args) throws Exception{
//        //构建执行环境
//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//        //设置并行度（默认为4）
//        executionEnvironment.setParallelism(1);
//        //从文件获取数据
//        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("/Users/zyf/project/my-project/FlinkService/src/main/resources/User");
//
//        //把数据往kafka输出
//        stringDataStreamSource.addSink(new FlinkKafkaProducer011<String>("localhost:9092", "topic", new SimpleStringSchema()));
//
//
//
//        executionEnvironment.execute();
//    }
//}
