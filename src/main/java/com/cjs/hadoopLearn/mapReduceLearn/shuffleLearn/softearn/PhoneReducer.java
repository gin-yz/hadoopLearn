package com.cjs.hadoopLearn.mapReduceLearn.shuffleLearn.softearn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneReducer extends Reducer<PhoneData, Text, Text, Text> {
    private final Text value = new Text();

    @Override
    protected void reduce(PhoneData key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long updata = 0;
        long downdata = 0;
        value.set(key.toString());
        System.out.println(key.toString());
        for (Text data : values) {
            context.write(data, value);
            System.out.println(key.toString());
            System.out.println(value);
        }
    }
}
