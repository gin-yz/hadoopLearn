package com.cjs.hadoopLearn.hdfs_api.sequenceFileLearn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;

import java.io.IOException;

public class SequenceFileWriteNewApi {

    private static final String[] DATA = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };

    public static void main(String[] args) throws IOException {
        String uri = new String("hdfs://hadoop1:8020/tmp/seqfile/hehe5.txt");
        Path path = new Path(uri);


        try(SequenceFile.Writer writer = SequenceFile.createWriter(new Configuration(),
                SequenceFile.Writer.file(path),//设置文件名
                SequenceFile.Writer.keyClass(IntWritable.class),//设置keyclass
                SequenceFile.Writer.valueClass(Text.class),//设置valueclass
                SequenceFile.Writer.appendIfExists(false),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK,new BZip2Codec()) //设置block+gzip的压缩方式
        )){
            //循环外定义key和value，避免重复定义，因为序列化时只是会序列化对应的内容
            IntWritable key = new IntWritable();
            Text value = new Text();
            for (int i = 0; i < 100; i++) {
                key.set(100 - i);
                value.set(DATA[i % DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                writer.append(key, value);
            }
        }
    }
}
