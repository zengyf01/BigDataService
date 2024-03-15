package com.demo.flink.test.cdc;

import com.alibaba.fastjson2.JSONObject;
import com.demo.flink.test.bean.CdcResultBO;
import com.demo.flink.test.bean.User;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @author zyf
 * @date 2023/8/16
 * @desc 将Kafka业务数据写到Mysql
 */
public class FlinkKafkaToMysql {


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

        /**定义Kafka Source*/
        Properties properties = new Properties();
        //告诉程序我们要接收那台机器上生产的数据
        properties.setProperty("bootstrap.servers","localhost:9092");
        //告诉程序开启分区,已经分区名称
//        properties.setProperty("group.id","qwer");
        //告诉程序我需要开启一个偏移量的记录,并且是从头开始读的
//        properties.setProperty("auto.offset.reset","earliest");
        //告诉kafka你不要自动提交偏移量了,每次搜自动提交偏移量
        properties.setProperty("enable.auto.commit","false");

        //如果FlinkKafkaConsumer没有开启checkpoint功能,为了不重复读取
        //这种方式无法实现Exactly-Once(只执行一次)
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer("test", new SimpleStringSchema(), properties);

        DataStreamSource<String> kafkaSource = env.addSource(flinkKafkaConsumer);


        /**转换数据*/
        SingleOutputStreamOperator<CdcResultBO> uds = kafkaSource.map(new MyMap());

        uds.print("uds");

        /**写到大数据Mysql库*/
        uds.addSink(new FlinkCdc.MyRichSinkFunction());


        /**执行任务*/
        env.execute("将Kafka业务数据写到Mysql（FlinkKafkaToMysql）");
    }



    /**
     * 将cdc数据转为user对象
     */
    public static class MyMap implements MapFunction<String, CdcResultBO> {
        @Override
        public CdcResultBO map(String s) throws Exception {
            return  cdcDataTdCdcResultBO(s);
        }
    }


    /**
     * 将业务库user数据写到大数据库
     */
    public static class MyRichSinkFunction extends RichSinkFunction<CdcResultBO> {

        Connection conn = null;
        PreparedStatement ps = null;
        String url = "jdbc:mysql://localhost:3306/bigdata_db";
        String username = "root";
        String password = "12345678";

        /**
         * 创建mysql连接（首次sink时执行，且创建到销毁只会执行一次）
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("创建连接");
            // 获取mysql 连接
            conn = DriverManager.getConnection(url, username, password);
            // 关闭自定提交
            conn.setAutoCommit(false);
        }

        /**
         * 执行mysql语句（数据输出时执行，每一个数据输出时，都会执行此方法）
         *
         * @param cdcResultBO
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(CdcResultBO cdcResultBO, Context context) throws Exception {
            if ("d".equals(cdcResultBO.getOp())) {
                User user = JSONObject.from(cdcResultBO.getBefore()).to(User.class);
                String sql = "DELETE FROM  t_user WHERE id=?";
                ps = conn.prepareStatement(sql);
                ps.setString(1,user.getId());
            } else {
                User user = JSONObject.from(cdcResultBO.getAfter()).to(User.class);
                String sql = "replace into t_user values(?,?,?,?)";
                ps = conn.prepareStatement(sql);
                ps.setString(1, user.getId());
                ps.setString(2, user.getName());
                ps.setInt(3, user.getAge());
                ps.setString(4, user.getAddress());
            }
            // 执行语句
            ps.execute();
            // 提交
            conn.commit();
        }

        /**
         * 关闭mysql连接（sink关闭时执行，且创建到销毁只会执行一次）
         *
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            System.out.println("关闭连接");
            if (conn != null) {
                conn.close();
            }
            if (ps != null) {
                ps.close();
            }
        }
    }



    /**
     * CDC数据转换
     * @param
     * @return
     */
    private static CdcResultBO cdcDataTdCdcResultBO(String cdcDataStr){
        CdcResultBO cdcResultBO = JSONObject.parseObject(cdcDataStr).to(CdcResultBO.class);
        return cdcResultBO;

    }

}
