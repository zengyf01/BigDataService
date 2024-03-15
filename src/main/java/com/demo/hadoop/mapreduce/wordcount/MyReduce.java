package com.demo.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * @author zyf
 * @date 2024/3/5
 * @desc
 */
public class MyReduce extends Reducer<Text, LongWritable,Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        Text outK = new Text();
        int sum = 0 ;
        for (LongWritable v : values) {
            sum += v.get();
        }
        LongWritable outV = new LongWritable(sum);
        outK.set(key);
        context.write(outK,outV);
    }
}
