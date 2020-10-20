/*
* 将ｈｄｆｓ同一文件夹下的文件合并到x
*
* */


package com.cjs.hadoopLearn.hdfs_api.sequenceFileLearn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.IOException;
import java.util.Arrays;

public class MergeFileUserSequence {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop1:8020");
        System.setProperty("HADOOP_USER_NAME", "root");
        FileSystem fs = FileSystem.get(conf);

        String inputDir = "hdfs://hadoop1:8020/tmp/seqfile/";
        Path path = new Path(inputDir);
        //最后生成的sequenceFile文件
        Path outFile = new Path("hdfs://hadoop1:8020/tmp/sequecenfile/out/seq_file");
        //声明一个byte[]用于后面存放小文件内容
        byte[] buffer;
        //获取inputDir目录下的所有文件
        FileStatus[] fileStatusArr = fs.listStatus(path);
        //构造writer, 并使用try获取资源, 最后自动关闭资源
        try(SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(outFile),//设置文件名
                SequenceFile.Writer.keyClass(Text.class),//设置keyclass
                SequenceFile.Writer.valueClass(Text.class),//设置valueclass
                SequenceFile.Writer.appendIfExists(false),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK,new BZip2Codec()) //设置block+gzip的压缩方式
        )){
            //循环外定义key和value，避免重复定义，因为序列化时只是会序列化对应的内容
            Text key = new Text();
            Text value = new Text();
            for (FileStatus fileStatus : fileStatusArr){
                System.out.println("the file name is "+fileStatus.getPath());
                //利用FileSystem打开文件
                FSDataInputStream fsDataIn = fs.open(fileStatus.getPath());
                System.out.println(fileStatus.getPath());
                //根据文件大小来定义byte[]的长度
                buffer = new byte[((int) fileStatus.getLen())];
                //将文件内容读入到buffer这个byte[]里
                int read = fsDataIn.read(buffer);
                System.out.println(read);
                key.set(fileStatus.getPath().toString());
                value.set(buffer);
                //通过append方法写入到SequenceFile
                writer.append(key, value);
            }

        }
    }
}
