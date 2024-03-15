//package com.demo.flink.test.source;
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
//
//import java.util.Properties;
//
///**
// * @author zyf
// * @date 2023/6/18
// * @desc kafka数据源——示例
// */
// */
//public class KafkaSource {
//
//    public static void main(String[] args) throws Exception{
//        //构建执行环境
//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//        //设置并行度（默认为4）
//        executionEnvironment.setParallelism(1);
//
//        //从kafka读取数据
//        DataStreamSource<String> kafkaDataStreamSource = executionEnvironment.addSource(new FlinkKafkaConsumer011<String>("topic",new SimpleStringSchema(),new Properties()));
//
//        kafkaDataStreamSource.print("kafka");
//
//        executionEnvironment.execute();
//
//    }
//}
