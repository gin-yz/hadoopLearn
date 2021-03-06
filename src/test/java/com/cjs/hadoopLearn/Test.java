package com.cjs.hadoopLearn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Test {
    public static void main(String[] args) throws IOException {
//        byte[] data = "hello, world!".getBytes(StandardCharsets.UTF_8);

        Writable intWritable = new LongWritable(65536);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

//        intWritable.write(dataOutputStream);

        dataOutputStream.write(new byte[]{0,1,0,0});
        System.out.println(Arrays.toString(outputStream.toByteArray()));

    }

}
