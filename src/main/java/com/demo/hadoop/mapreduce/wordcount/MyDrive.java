package com.demo.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author zyf
 * @date 2024/3/5
 * @desc
 */
public class MyDrive {

    public static void main(String[] args) throws Exception{

        //1. 创建一个Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
//        //设置HDFS NameNode的地址
//        conf.set("fs.defaultFS","hdfs://hadoop-1:9870/");
//        // 指定MapReduce运行在Yarn上
//        conf.set("mapreduce.framework.name","yarn");
//        // 指定mapreduce可以在远程集群运行
//        conf.set("mapreduce.app-submission.cross-platform","true");
//        //指定Yarnresourcemanager的位置
//        conf.set("yarn.resourcemanager.hostname","hadoop-2");


        //2. 关联jar
        job.setJarByClass(MyDrive.class);
        //3. 关联Mapper 和 Reducer 类
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        //4. 设置Mapper的输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //5. 设置最终输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //分区
//        job.setPartitionerClass();
//        job.setNumReduceTasks();

        //设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("/Users/zyf/project/my-project/BigDataService/shuguo.txt"));
         FileOutputFormat.setOutputPath(job,new Path("/Users/zyf/project/my-project/BigDataService/output")); // 输出路径不能存在，如果已经存在就报异常.
//        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 提交job
        job.waitForCompletion(true);

    }
}
