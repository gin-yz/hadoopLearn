package com.cjs.hadoopLearn.hdfs_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class GetFileSystem {
    private FileSystem fs =null;

    /*
     * 获取文件系统：方式１
     * */
    @Test
    public void getFileSystem1() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop1:8020");
        System.setProperty("HADOOP_USER_NAME", "root");
        fs = FileSystem.get(conf);
        System.out.println(fs);
    }

    /*
    * 方式２
    * */
    @Test
    public void getFileSystem2() throws URISyntaxException, IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        fs = FileSystem.get(new URI("hdfs://hadoop1:8020"),new Configuration());
        System.out.println(fs);
    }

    /*
    * 方式３
    * */
    @Test
    public void getFileSystem3() throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop1:8020");
        fs = FileSystem.newInstance(conf);
        System.out.println(fs.toString());
    }

    /*
    * 方式４
    * */
    @Test
    public void getFileSystem4() throws URISyntaxException, IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        fs = FileSystem.newInstance(new URI("hdfs://hadoop1:8020"),new Configuration());
        System.out.println(fs.toString());
    }
}
