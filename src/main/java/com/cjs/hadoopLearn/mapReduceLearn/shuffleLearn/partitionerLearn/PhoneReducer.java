package com.cjs.hadoopLearn.mapReduceLearn.shuffleLearn.partitionerLearn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneReducer extends Reducer<Text, PhoneData, Text, Text> {
    private final Text value = new Text();
    private final PhoneData phoneData = new PhoneData();

    @Override
    protected void reduce(Text key, Iterable<PhoneData> values, Context context) throws IOException, InterruptedException {
        long updata = 0;
        long downdata = 0;
        for (PhoneData data : values) {
            updata += data.getUpLoadData();
            downdata += data.getDownLoadData();
        }

        phoneData.setUpLoadData(updata);
        phoneData.setDownLoadData(downdata);

        value.set(phoneData.toString());

        context.write(key,value);
    }
}
