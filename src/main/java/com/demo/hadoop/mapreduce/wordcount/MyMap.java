package com.demo.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


/**
 * @author zyf
 * @date 2024/3/3
 * @desc MapReduce wordCount 案例
 *
 * LongWritable 行号类型
 * Text 行内容类型
 * Text 输出key类型
 * LongWritable 输出value类型
 */
public class MyMap extends Mapper<LongWritable, Text,Text,LongWritable> {

    private Text outK = new  Text();
    private LongWritable outV = new LongWritable(1);


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        outK.set(value.toString());
        context.write(outK,outV);
    }
}
