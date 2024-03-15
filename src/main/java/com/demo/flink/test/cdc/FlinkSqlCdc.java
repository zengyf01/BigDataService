package com.demo.flink.test.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zyf
 * @date 2023/8/16
 * @desc
 */
public class FlinkSqlCdc {
    public static void main(String[] args) throws  Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.创建 Flink-MySQL-CDC 的 Source
        tableEnv.executeSql("CREATE TABLE user_info (" +
                " id INT," +
                " name STRING," +
                " phone_num STRING" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'localhost'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '12345678'," +
                " 'database-name' = '-business_db'," +
                " 'table-name' = 't_user'" +
                ")"
        );
        tableEnv.executeSql("select * from user_info").print();
        env.execute();
    }
}
