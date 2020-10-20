/*
* SequenceFile可在ｈｄｆｓ端创造一个流文件．将本地文件写入ｈｄｆｓ中
* 可将小文件合并后写入ｈｄｆｓ
* 或存储日志文件以及文本文件
* 通过此上传的文件可使用hdfs dfs -text <path> 查看
* */

package com.cjs.hadoopLearn.hdfs_api.sequenceFileLearn;// cc SequenceFileWriteDemo Writing a SequenceFile

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;

// vv SequenceFileWriteDemo
public class SequenceFileWriteDemo {
  
  private static final String[] DATA = {
    "One, two, buckle my shoe",
    "Three, four, shut the door",
    "Five, six, pick up sticks",
    "Seven, eight, lay them straight",
    "Nine, ten, a big fat hen"
  };
  
  public static void main(String[] args) throws IOException, InterruptedException {
    String uri = new String("hdfs://hadoop1:8020/tmp/seqfile/hehe2.txt");
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);

    IntWritable key = new IntWritable();
    Text value = new Text();
    SequenceFile.Writer writer = null;
    try {
      writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
      
      for (int i = 0; i < 100; i++) {
        key.set(100 - i);
        value.set(DATA[i % DATA.length]);
        System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
        writer.append(key, value);
      }
    } finally {
      IOUtils.closeStream(writer);
    }
  }
}
// ^^ SequenceFileWriteDemo
