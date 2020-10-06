package com.cjs.hadoopLearn.mapReduceLearn.shuffleLearn.combineLearn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text text = new Text();
    private final IntWritable intWritable = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        System.out.println(key.get());

        String[] str = value.toString().split(" ");
        for (String s : str) {
            if (!s.equals("")) {
                text.set(s);
                context.write(text, intWritable);
            }
        }
    }
}
