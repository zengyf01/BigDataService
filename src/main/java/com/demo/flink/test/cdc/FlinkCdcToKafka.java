package com.demo.flink.test.cdc;

import com.alibaba.fastjson2.JSONObject;
import com.demo.flink.test.bean.CdcResultBO;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyf
 * @date 2023/8/16
 * @desc 采集业务数据并将数据发送到Kafka
 */
public class FlinkCdcToKafka {


    /**
     * 测试步骤：
     * 1、开启 MySQL Binlog 并重启 MySQL
     * 2、启动 Flink 集群 : ./bin/start-cluster.sh
     * 3、./bin/flink run -c com.atguigu.FlinkCDC flink-1.0-SNAPSHOT-jar-with-dependencies.jar
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        /**创建执行环境*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**使用 CDC Source 从 MySQL 读取数据*/
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("12345678")
                .databaseList("business_db")
                .tableList("business_db.t_user")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        //5.打印数据
        mysqlDS.print();

        /**将数据转成user对象*/
        SingleOutputStreamOperator<String> uds = mysqlDS.map(new MyMap());


        /**定义Kafka Sink*/
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
        //配置 kafka 的地址和端口
        .setBootstrapServers("localhost:9092")
        //配置消息序列化器信息 Topic名称、消息序列化器类型
        .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder().setTopic("test").setValueSerializationSchema(new SimpleStringSchema()).build())
        //必填项：配置容灾保证级别设置为 至少一次
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();


        /**将用户信息发送到Kafka*/
        uds.sinkTo(kafkaSink);

        /**执行任务*/
        env.execute("采集业务数据到Kafka（FlinkCdcToKafka）");

    }


    /**
     * 将cdc数据转为user对象
     */
    public static class MyMap implements MapFunction<String, String> {
        @Override
        public String map(String s) throws Exception {
            return  JSONObject.toJSONString(JSONObject.parseObject(s).to(CdcResultBO.class));
        }
    }


}
