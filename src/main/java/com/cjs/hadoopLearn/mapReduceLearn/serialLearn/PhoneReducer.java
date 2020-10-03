package com.cjs.hadoopLearn.mapReduceLearn.serialLearn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneReducer extends Reducer<Text, PhoneData, Text, Text> {
    private final Text text = new Text();
    @Override
    protected void reduce(Text key, Iterable<PhoneData> values, Context context) throws IOException, InterruptedException {
        for (PhoneData value : values) {
            text.set(value.toString());
            context.write(key,text);
        }
    }
}
