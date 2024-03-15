//package com.demo.flink.test.sink;
//
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
//import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
//import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
//import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
//
///**
// * @author zyf
// * @date 2023/6/18
// * @desc 输出数据到Redis——示例
// */
//public class RedisSink {
//
//    public static void main(String[] args) {
//        //构建执行环境
//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//        //设置并行度（默认为4）
//        executionEnvironment.setParallelism(1);
//        //从文件获取数据
//        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("/Users/zyf/project/my-project/FlinkService/src/main/resources/User");
//
//        //配置redis连接信息
//        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("locolhost").setPort(6379).build();
//
//        //把数据输出到redis
//        stringDataStreamSource.addSink(new org.apache.flink.streaming.connectors.redis.RedisSink<>(config, new MyRedisSinkMapper()));
//
//    }
//
//    /**
//     * 自定义redis输出源
//     */
//    public static class MyRedisSinkMapper implements RedisMapper<String> {
//
//        //定义redis命令 ：如 set test命令
//        @Override
//        public RedisCommandDescription getCommandDescription() {
//
//            return new RedisCommandDescription(RedisCommand.SET,null);
//        }
//
//        //返回key
//        @Override
//        public String getKeyFromData(String s) {
//            return s;
//        }
//
//        //返回值
//        @Override
//        public String getValueFromData(String s) {
//            return s;
//        }
//    }
//}
