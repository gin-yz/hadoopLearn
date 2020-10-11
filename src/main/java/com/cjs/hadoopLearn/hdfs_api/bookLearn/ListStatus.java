package com.cjs.hadoopLearn.hdfs_api.bookLearn;// cc ListStatus Shows the file statuses for a collection of paths in a Hadoop filesystem

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.net.URI;

// vv ListStatus
public class ListStatus {

  public static void main(String[] args) throws Exception {
    String uri = "hdfs://hadoop1:8020";
    args = new String[]{"/"};
    Configuration conf = new Configuration();
    System.setProperty("HADOOP_USER_NAME", "root");
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    
    Path[] paths = new Path[args.length];
    for (int i = 0; i < paths.length; i++) {
      paths[i] = new Path(args[i]);
    }
    
    FileStatus[] status = fs.listStatus(paths);
    for (FileStatus fileStatus : status) {
      System.out.println(fileStatus);
    }

    System.out.println();
    Path[] listedPaths = FileUtil.stat2Paths(status); //stat2Paths－＞将ｓｔａｔ转化为ｐａｔｈ
    for (Path p : listedPaths) {
      System.out.println(p);
    }

  }
}
// ^^ ListStatus
