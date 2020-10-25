package com.cjs.hadoopLearn.hbaseLearn.iHdfsOHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;

public class ReadFromHDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> { //输出的ImmutableBytesWritable:为ｒｏｗｋｅｙ的值
//    private String tableName = null;
//
//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        URI[] cacheFiles = context.getCacheFiles();
//        tableName = cacheFiles[0].getPath();
////        Configuration configuration = context.getConfiguration();
////        configuration.get("tableName");
//    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        String rowKey = split[0];
        String familyColumn = split[1];
        String column = split[2];
        String columnValue = split[3];

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(column), Bytes.toBytes(columnValue));

        ImmutableBytesWritable rowkeySerial = new ImmutableBytesWritable(Bytes.toBytes(rowKey));

        context.write(rowkeySerial, put);

    }
}
