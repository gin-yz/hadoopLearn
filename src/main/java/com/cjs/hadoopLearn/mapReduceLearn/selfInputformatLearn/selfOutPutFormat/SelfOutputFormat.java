package com.cjs.hadoopLearn.mapReduceLearn.selfInputformatLearn.selfOutPutFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;


public class SelfOutputFormat extends RecordWriter<Text, BytesWritable> {
    private final FSDataOutputStream fsDataOutputStream;
    public SelfOutputFormat(FSDataOutputStream fsDataOutputStream) {
        this.fsDataOutputStream = fsDataOutputStream;
    }

    @Override
    public void write(Text key, BytesWritable value) throws IOException, InterruptedException {
        String fenGe=" ";
        Charset charSet= StandardCharsets.UTF_8;
        System.out.println("key="+key.toString());
        //输出key
        fsDataOutputStream.write(key.toString().getBytes(charSet),0,key.toString().getBytes(charSet).length);
        //输出key和value的分隔符
        fsDataOutputStream.write("\n".getBytes(charSet),0,"\n".getBytes(charSet).length);
//        fsDataOutputStream.write(fenGe.getBytes(charSet),0,fenGe.getBytes(charSet).length);
        //输出value
        fsDataOutputStream.write(new String(value.getBytes()).getBytes(charSet),0,new String(value.getBytes()).length());
        fsDataOutputStream.write("\n".getBytes(charSet),0,"\n".getBytes(charSet).length);
        fsDataOutputStream.flush();

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if(this.fsDataOutputStream!=null) this.fsDataOutputStream.close();
    }
}
