package com.cjs.hadoopLearn.mapReduceLearn.shuffleLearn.combineLearn;

import com.cjs.hadoopLearn.mapReduceLearn.shuffleLearn.partitionerLearn.PartitionerDemo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{WordCountDriver.class.getResource("hello.txt").toString(),WordCountDriver.class.getResource(".").toString()+"output"};
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCountDriver.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置ｃｏｍｂｉｎｅｒ
        job.setCombinerClass(CombineSelf.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        // 7设置每个切片InputSplit中划分三条记录
//        NLineInputFormat.setNumLinesPerSplit(job, 3);
//
//        // 8使用NLineInputFormat处理记录数
//        job.setInputFormatClass(NLineInputFormat.class);

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);

    }
}
