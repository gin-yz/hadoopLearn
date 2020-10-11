/*
* seek()方法可以定位到文件的任意位置，如下面展示的是将文件输出两次
* skip()相对于当前位置定位到一个新位置
* */
package com.cjs.hadoopLearn.hdfs_api.bookLearn;// cc FileSystemDoubleCat Displays files from a Hadoop filesystem on standard output twice, by using seek

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

// vv FileSystemDoubleCat
public class FileSystemDoubleCat {

  public static void main(String[] args) throws Exception {
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    FSDataInputStream in = null;
    try {
      in = fs.open(new Path(uri));
      IOUtils.copyBytes(in, System.out, 4096, false);
      in.seek(0); // go back to the start of the file
      IOUtils.copyBytes(in, System.out, 4096, false);
    } finally {
      IOUtils.closeStream(in);
    }
  }

}
// ^^ FileSystemDoubleCat
