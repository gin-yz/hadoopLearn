package com.cjs.hadoopLearn.hbaseLearn.iHbaseOHdfs;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class HbaseToHadoopMapper extends TableMapper<Text, NullWritable> {

    private Text value;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        value = new Text();
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        StringBuilder stringBuffer = new StringBuilder();
        stringBuffer.append(new String(key.get(), StandardCharsets.UTF_8));
        stringBuffer.append(" ,");
        for (Cell cell : value.rawCells()) {
            stringBuffer.append(Bytes.toString(CellUtil.cloneQualifier(cell)));
            stringBuffer.append(":");
            stringBuffer.append(Bytes.toString(CellUtil.cloneValue(cell)));
            stringBuffer.append(" ,");
        }
        this.value.set(stringBuffer.toString());
        System.out.println(stringBuffer.toString());
        context.write(this.value,NullWritable.get());
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
