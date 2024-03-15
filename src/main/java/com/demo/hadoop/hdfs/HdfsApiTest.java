package com.demo.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author zyf
 * @date 2024/3/2
 * @desc HDFS API调用
 */
public class HdfsApiTest {

    private FileSystem fs ;

    private String uri;

    @Before
    public  void innit() throws Exception{
        // 创建配置对象
        Configuration conf = new Configuration();
        // 定义统一资源标识符（uri：uniform resource identifier)
         uri = "hdfs://hadoop-1:8020";
        // 创建文件系统对象（基于HDFS的文件系统）
        fs = FileSystem.get(new URI(uri), conf);
    }

    @After
    public void close( ) throws Exception {
        fs.close();
    }

    @Test
    public  void createNewFile() throws Exception{
        // 创建路径对象（指向文件）
        Path path = new Path(this.uri + "/myFile/hadoop1.txt");
        // 基于路径对象创建文件
        boolean result = fs.createNewFile(path);
        // 根据返回值判断文件是否创建成功
        if (result) {
            System.out.println("文件[" + path + "]创建成功！");
        } else {
            System.out.println("文件[" + path + "]创建失败！");
        }
    }



}
