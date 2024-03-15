package com.demo.flink.test.processfunction;

import com.demo.flink.test.bean.UserTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

/**
 * @author zyf
 * @date 2023/7/4
 * @desc 处理函数（如果我们想要访问事件的时间戳，或者当前的水位线信息，都是完全做不到的。处理函数提供了一个“定时服务”（TimerService），
 * 我们可以通过它访问流中的事件（event）、时间戳（timestamp）、水位线（watermark），甚至可以注册“定时事件”。而且处理函数继承了
 * AbstractRichFunction 抽象类，所以拥有富函数类的所有特性，同样可以访问状态（state）和其他运行时信息。）
 */
public class ProcessFunction {


    /**
     * 示例需求：连续10秒工时上涨则告警
     * @param args
     */
    public static void main(String[] args) {
        //构建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度（默认为4）
        executionEnvironment.setParallelism(1);
        //读取文件数据
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("/Users/zyf/project/my-project/FlinkService/src/main/resources/User");
        //转为user
        SingleOutputStreamOperator<UserTest> mapDataStreamSource =  stringDataStreamSource.map(new MapFunction<String, UserTest>() {
            @Override
            public UserTest map(String s) throws Exception {

                return new UserTest(s.split(",")[0],Integer.valueOf((String) s.split(",")[1]), Integer.valueOf((String) s.split(",")[2]), BigDecimal.valueOf(Long.parseLong( s.split(",")[3])));
            }
        });

        // 要用定时器，必须基于 KeyedStream
        mapDataStreamSource.keyBy("id").process(new KeyedProcessFunction<Tuple, UserTest, Object>() {
           //定义定时触发的操作
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Tuple, UserTest, Object>.OnTimerContext ctx, Collector<Object> out) throws Exception {
                out.collect("定时器触发，触发时间：" + timestamp);

            }
            //定义处理操作
            @Override
            public void processElement(UserTest userTest, KeyedProcessFunction<Tuple, UserTest, Object>.Context context, Collector<Object> out) throws Exception {
                Long currTs = context.timerService().currentProcessingTime();
                out.collect("数据到达，到达时间：" + currTs);
                //注册一个 10 秒后的定时器
                context.timerService().registerEventTimeTimer(currTs + 10 * 1000L);
            }
        });

    }
}
