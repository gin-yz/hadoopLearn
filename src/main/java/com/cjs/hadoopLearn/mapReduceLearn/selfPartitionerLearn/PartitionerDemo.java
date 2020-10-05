package com.cjs.hadoopLearn.mapReduceLearn.selfPartitionerLearn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//Partitioner中的参数为ｍａｐ中输出的参数,分区在ｍａｐ之后ｒｅｄｕｃｅ之前，且有多少个分区就有多少个ｒｅｄｕｃｅ
public class PartitionerDemo extends Partitioner<Text,PhoneData> {
    @Override
    public int getPartition(Text text, PhoneData phoneData, int i) {

        String preNum = text.toString().substring(0,3);
        int partition = 4;
        if ("136".equals(preNum)) {
            partition = 0;
        }else if ("137".equals(preNum)) {
            partition = 1;
        }else if ("138".equals(preNum)) {
            partition = 2;
        }else if ("139".equals(preNum)) {
            partition = 3;
        }

        return partition;
    }
}
