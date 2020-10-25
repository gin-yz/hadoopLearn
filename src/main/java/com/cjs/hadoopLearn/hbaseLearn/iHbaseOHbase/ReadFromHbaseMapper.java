package com.cjs.hadoopLearn.hbaseLearn.iHbaseOHbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ReadFromHbaseMapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Put put = new Put(key.get());
        for (Cell cell : value.rawCells()) {
            if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){ //只复制ｎａｍｅ的列到另外一个表
                put.add(cell);
                context.write(key,put);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context);
    }
}
