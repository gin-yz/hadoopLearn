package com.cjs.hadoopLearn.mapReduceLearn.joinLearn.otherMethod;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OtherMapper extends Mapper<LongWritable, Text, IntWritable,OtherBean> {
    private String inputPath;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        this.inputPath = inputSplit.getPath().toString();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (inputPath.startsWith("order")){
            String[] split = value.toString().split("\t");

        }
    }
}
