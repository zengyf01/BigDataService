package com.demo.flink.test.cdc;

//import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
//import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
//import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
//import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;

import com.alibaba.fastjson2.JSONObject;
import com.demo.flink.test.bean.CdcResultBO;
import com.demo.flink.test.bean.User;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author zyf
 * @date 2023/8/16
 * @desc 采集业务数据并将数据写到大数据库
 */
public class FlinkCdc {


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

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
//                1, // 尝试重启的次数
//                Time.of(1, TimeUnit.SECONDS) // 间隔
//        ));


//        //2.Flink-CDC 将读取 binlog 的位置信息以状态的方式保存在 CK,如果想要做到断点续传,需要从 Checkpoint 或者 Savepoint 启动程序
//        //2.1 开启 Checkpoint,每隔 5 秒钟做一次 CK
//        env.enableCheckpointing(5000L);
//        //2.2 指定 CK 的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //2.3 设置任务关闭的时候保留最后一次 CK 数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 指定从 CK 自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
//        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));
//        //2.6 设置访问 HDFS 的用户名
//        System.setProperty("HADOOP_USER_NAME", "atguigu");


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


        //4.使用 CDC Source 从 MySQL 读取数据
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        //5.打印数据
        mysqlDS.print();

        //将数据转成user对象
        SingleOutputStreamOperator<CdcResultBO> uds = mysqlDS.map(new MyMap());

        //写到大数据库mysql sink
        uds.addSink(new MyRichSinkFunction());

        //6.执行任务
        env.execute("业务数据（business_db）-采集");

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
