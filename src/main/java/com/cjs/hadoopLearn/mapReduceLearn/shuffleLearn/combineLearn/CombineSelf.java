package com.cjs.hadoopLearn.mapReduceLearn.shuffleLearn.combineLearn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CombineSelf extends Reducer<Text, IntWritable,Text, IntWritable> {

    private IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        System.out.println(key);
        for (IntWritable value : values) {
            sum+=value.get();
        }
        v.set(sum);
        System.out.println(sum);
        context.write(key,v);
    }
}
