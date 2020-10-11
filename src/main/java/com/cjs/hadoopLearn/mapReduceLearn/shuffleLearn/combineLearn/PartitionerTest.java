/*
* 此文件仅做测试用，和ｃｏｍｂｉｎｅ无关
* */

package com.cjs.hadoopLearn.mapReduceLearn.shuffleLearn.combineLearn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerTest extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        int result = 4;
        String substring = text.toString().substring(0, 1);
        int parseInt = Integer.parseInt(substring);
        if (parseInt == 1) {
            result = 0;
        } else if (parseInt == 2) {
            result = 1;
        } else if (parseInt == 3) {
            result = 2;
        } else if (parseInt == 4) {
            result = 3;
        }

        return result;
    }
}
