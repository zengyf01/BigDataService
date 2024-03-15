package com.demo.flink.test.delay;

import com.demo.flink.test.bean.UserTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;

/**
 * @author zyf
 * @date 2023/6/25
 * @desc 允许延迟——示例
 */
public class AllowedLateness {

    public static void main(String[] args) {
        //构建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度，默认为4
        executionEnvironment.setParallelism(1);
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //监听sockert数据
        DataStreamSource<String> sockertStreamSource =  executionEnvironment.socketTextStream("localhost",8899);

        //将sockert数据转换成User
        SingleOutputStreamOperator<UserTest> mapDataStreamSource =  sockertStreamSource.map(new MapFunction<String, UserTest>() {
            @Override
            public UserTest map(String s) throws Exception {
                return new UserTest(s.split(",")[0],Integer.valueOf((String) s.split(",")[1]), Integer.valueOf((String) s.split(",")[2]), BigDecimal.valueOf(Long.parseLong( s.split(",")[3])));
            }
        });

        //定义侧输出流标签
        OutputTag lateTag = new OutputTag<UserTest>("late");

        //分组、定义10秒时间窗、允许验收1分钟、设置侧输出流标签、统计
        SingleOutputStreamOperator<UserTest> sum = mapDataStreamSource.keyBy("name").timeWindow(Time.seconds(10)).allowedLateness(Time.minutes(1)).sideOutputLateData(lateTag).sum("workingHours");

        sum.getSideOutput(lateTag).print("late");
        }
}
