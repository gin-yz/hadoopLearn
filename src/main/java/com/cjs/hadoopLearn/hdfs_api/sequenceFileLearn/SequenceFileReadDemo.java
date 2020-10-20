/*
* 读取相应文件
*
* */

package com.cjs.hadoopLearn.hdfs_api.sequenceFileLearn;// cc SequenceFileReadDemo Reading a SequenceFile

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;
import java.util.TreeMap;

// vv SequenceFileReadDemo
public class SequenceFileReadDemo {
  
  public static void main(String[] args) throws IOException {
    String uri = new String("hdfs://hadoop1:8020/tmp/seqfile/hehe5.txt");
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);



    SequenceFile.Reader reader = null;
    try {
      reader = new SequenceFile.Reader(fs, path, conf);
      //反射得到ｋｅｙ,vlaue数据实例
      Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);

      long position = reader.getPosition();
      while (reader.next(key, value)) {
        //显示同步点位置
        String syncSeen = reader.syncSeen() ? "*" : "";
        System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
        position = reader.getPosition(); // beginning of next record
      }


    } finally {
      IOUtils.closeStream(reader);
    }
  }
}
// ^^ SequenceFileReadDemo