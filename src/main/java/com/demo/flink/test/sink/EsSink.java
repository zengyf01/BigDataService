package com.demo.flink.test.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.source.event.RequestSplitEvent;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author zyf
 * @date 2023/6/18
 * @desc 输出数据到Redis——示例
 */
public class EsSink {

    public static void main(String[] args) {
        //构建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度（默认为4）
        executionEnvironment.setParallelism(1);
        //从文件获取数据
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("/Users/zyf/project/my-project/FlinkService/src/main/resources/User");

        //定义es连接信息
        HttpHost httpHost = new HttpHost("loclhost",9200);

        //把数据输出到es
        stringDataStreamSource.addSink(new ElasticsearchSink.Builder<String>(Arrays.asList(httpHost), new ElasticsearchSinkFunction<String>() {
            @Override
            public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                //定义写入数据
                s = "我是essink测试数据：" + s;
                //定义idnex请求
                IndexRequest indexRequest = Requests.indexRequest().index("test").type("test").source(s);

                //发生index请求
                requestIndexer.add(indexRequest);
            }
        }).build());

    }

}
