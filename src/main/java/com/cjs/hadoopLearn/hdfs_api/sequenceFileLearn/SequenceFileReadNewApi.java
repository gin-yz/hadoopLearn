package com.cjs.hadoopLearn.hdfs_api.sequenceFileLearn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SequenceFileReadNewApi {
    public static void main(String[] args) throws IOException {
        String uri = new String("hdfs://hadoop1:8020/tmp/hehe.txt");
        Path path = new Path(uri);

        try (SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(path), //设置SequenceFile文件路径
                SequenceFile.Reader.bufferSize(1024 * 8))) { //设置bufferSize
            IntWritable key = new IntWritable();
            Text value = new Text();
            while (reader.next(key, value)) {
                System.out.println("------------key=" + key + "----------");
                System.out.println(value);
                System.out.println("-------------------------------");
            }
        }
    }
}
