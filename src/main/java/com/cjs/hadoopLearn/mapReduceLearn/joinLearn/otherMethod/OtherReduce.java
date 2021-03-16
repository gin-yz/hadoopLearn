package com.cjs.hadoopLearn.mapReduceLearn.joinLearn.otherMethod;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OtherReduce extends Reducer<IntWritable,OtherBean, Text, NullWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        super.setup(context);

    }

    @Override
    protected void reduce(IntWritable key, Iterable<OtherBean> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);

    }
}
