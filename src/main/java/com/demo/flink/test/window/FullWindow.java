package com.demo.flink.test.window;

import com.demo.flink.test.bean.UserTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.math.BigDecimal;
import java.util.*;

/**
 * @author zyf
 * @date 2023/6/20
 * @desc 全量窗口——示例  nc -lk 8899
 */
public class FullWindow {




    public static void main(String[] args) throws Exception{
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



        //设置10秒为一个窗口，统计窗口内相同用户个数
        SingleOutputStreamOperator<Tuple3<String,String, Integer>> apply = mapDataStreamSource.keyBy("name").timeWindow(Time.seconds(20))
                .apply(new WindowFunction<UserTest, Tuple3<String,String, Integer>, Tuple, TimeWindow>() {
            @Override
            //tuple：key的字段, timeWindow：窗口对象, iterable：窗口所有数据集, collector：输出)
            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<UserTest> iterable, Collector<Tuple3<String,String, Integer>> collector) throws Exception {
                Iterator<UserTest> iterator =  iterable.iterator();
                Integer count = 0;
                while (iterator.hasNext()) {
                    iterator.next();
                    count ++;
                }
                collector.collect(new Tuple3<String,String, Integer>(tuple.getField(0), String.valueOf(timeWindow.getEnd()),count));
            }
        });
        //打印窗口数据
        apply.print("apply");
        executionEnvironment.execute();



    }
}


