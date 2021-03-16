package com.cjs.hadoopLearn.mapReduceLearn.inputOutputLearn.selfInputformatLearn;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WholeFileInputformat extends FileInputFormat<Text, BytesWritable>{

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		System.out.println(split);
		System.out.println(context);

		WholeRecordReader recordReader = new WholeRecordReader();
		recordReader.initialize(split, context);
		
		return recordReader;
	}

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
}
