package com.cjs.hadoopLearn.mapReduceLearn.wordCountDemo.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordcountDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		args = new String[] { "e:/input/inputword", "e:/output6" };

		Configuration conf = new Configuration();
		// 开启map端输出压缩
		conf.setBoolean("mapreduce.map.output.compress", true);
		// 设置map端输出压缩方式
		conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

		// 1 获取Job对象
		Job job = Job.getInstance(conf);

		// 2 设置jar存储位置
		job.setJarByClass(WordcountDriver.class);

		// 3 关联Map和Reduce类
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);

		// 4 设置Mapper阶段输出数据的key和value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5 设置最终数据输出的key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 如果不设置InputFormat，它默认用的是TextInputFormat.class,这个会按照一个文件一个文件的切片，如果一个文件小于１２８ＭＢ的话，也是属于一个切片
		//默认按照一行一行的读
//		 job.setInputFormatClass(CombineTextInputFormat.class);
		// 虚拟存储切片最大值设置4m
		// CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

		// 虚拟存储切片最大值设置20m
		// CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

		// job.setNumReduceTasks(2);

		// job.setCombinerClass(WordcountCombiner.class);

		// job.setCombinerClass(WordcountReducer.class);

		// 设置reduce端输出压缩开启
		FileOutputFormat.setCompressOutput(job, true);

		// 设置压缩的方式
//		FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

//使用SequenceFile时输出压缩设置
		//设置压缩
//		FileOutputFormat.setCompressOutput(job, true);
//设置用哪种算法进行压缩
//		FileOutputFormat.setOutputCompressorClass(job, Lz4Codec.class);
//必须通过SequenceFileOutputFormat.setOutputCompressionType来指定SequenceFile文件的压缩类型
//		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);


		// 6 设置输入路径和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7 提交job
		// job.submit();
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}
}
