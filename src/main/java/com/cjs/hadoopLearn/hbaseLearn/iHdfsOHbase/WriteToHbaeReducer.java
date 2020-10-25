package com.cjs.hadoopLearn.hbaseLearn.iHdfsOHbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class WriteToHbaeReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> { //写入ｈｂａｓｅ中，直接写NullWritable
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {

        for (Put value : values) {
            context.write(NullWritable.get(),value);
        }

    }
}
