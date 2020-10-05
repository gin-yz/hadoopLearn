package com.cjs.hadoopLearn.mapReduceLearn.softReduceLearn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneMapper extends Mapper<LongWritable, Text, PhoneData, Text> {
    private final PhoneData phoneData = new PhoneData();
    private final Text phoneKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] phoneStrs = value.toString().split("\t");
        phoneKey.set(phoneStrs[1]);
        phoneData.setUpLoadData(Long.parseLong(phoneStrs[phoneStrs.length - 3]));
        phoneData.setDownLoadData(Long.parseLong(phoneStrs[phoneStrs.length - 2]));
        context.write(phoneData, phoneKey);

    }
}
