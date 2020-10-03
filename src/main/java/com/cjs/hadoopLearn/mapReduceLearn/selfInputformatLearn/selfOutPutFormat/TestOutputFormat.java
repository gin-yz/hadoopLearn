package com.cjs.hadoopLearn.mapReduceLearn.selfInputformatLearn.selfOutPutFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TestOutputFormat extends FileOutputFormat<Text, BytesWritable> {
    @Override
    public RecordWriter<Text, BytesWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.newInstance(taskAttemptContext.getConfiguration());
        FSDataOutputStream fsDataOutputStream= fs.create(new Path("file:///home/cjs/tmp_dir/hdfs/output10.txt"));
        return new SelfOutputFormat(fsDataOutputStream);
    }
}
