package com.demo.flink.test.window;

import com.demo.flink.test.bean.UserTest;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;

/**
 * @author zyf
 * @date 2023/6/20
 * @desc 计数窗口——示例  nc -lk 8899
 */
public class CountWindow {




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

        //计算用户每月平均工时，平均工时=总工时/月数
        //窗口大小为10，每2个数据滑动一次
        SingleOutputStreamOperator aggregate = mapDataStreamSource.keyBy("name").countWindow(10,2).aggregate(new AggregateFunction<UserTest, Tuple2<Double,Integer>, Double>() {
            //初始化聚合对象
            @Override
            public Tuple2<Double, Integer> createAccumulator() {
                return new Tuple2<>(0.0, 0);
            }
            //进来的数据处理：累计数据
            @Override
            public Tuple2<Double, Integer> add(UserTest user, Tuple2<Double, Integer> doubleIntegerTuple2) {
                return new Tuple2<>(doubleIntegerTuple2.f0 + user.getWorkingHours(), doubleIntegerTuple2.f1 + 1);
            }

            //返回结果：平均工时=总工时/月数
            @Override
            public Double getResult(Tuple2<Double, Integer> doubleIntegerTuple2) {
                return doubleIntegerTuple2.f0 / doubleIntegerTuple2.f1;
            }

            @Override
            public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
            }
        });

        aggregate.print("aggregate");
        executionEnvironment.execute();



    }
}


