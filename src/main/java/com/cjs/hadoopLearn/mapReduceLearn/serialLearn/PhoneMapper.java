package com.cjs.hadoopLearn.mapReduceLearn.serialLearn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class PhoneMapper extends Mapper<LongWritable, Text,Text,PhoneData> {
    private static final PhoneData phoneData = new PhoneData();


    private final Text phone = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split("\t");
        System.out.println(Arrays.toString(strings));
        phoneData.setUpData(Long.parseLong(strings[4].trim()));
        phoneData.setDownData(Long.parseLong(strings[6].trim()));
        phone.set(strings[1]);
        context.write(phone,phoneData);
    }
}
