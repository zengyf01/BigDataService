package com.demo.flink.test.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author zyf
 * @date 2023/6/18
 * @desc 自定义数据源——示例
 */
public class CustomSource {

    public static void main(String[] args) throws Exception{
        //构建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度（默认为4）
        executionEnvironment.setParallelism(1);

        //引用自定义数据源
        DataStreamSource<String>  customDataStreamSource = executionEnvironment.addSource(new MySourceFuncation());

        customDataStreamSource.print("customDataStreamSource");

        executionEnvironment.execute();
    }


    /**
     * 自定义数据源 (循环生产数据，当调用cancel方法才结束生产数据)
     */
     public static class MySourceFuncation implements SourceFunction<String> {
        boolean isWhile = true;
        Integer name = 1;
        @Override
        public void run(SourceContext sourceContext) throws Exception {
            while (isWhile){
                sourceContext.collect("我是：" + name );
                name ++;
            }
        }

         @Override
         public void cancel() {
             isWhile = false;
         }
     }
}
