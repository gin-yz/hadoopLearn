package com.cjs.hadoopLearn.mapReduceLearn.wordCountDemo.selfwordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{WordCountDriver.class.getResource("./hello").toString(),WordCountDriver.class.getResource(".").toString()+"/result"};

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCountDriver.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

         //如果不设置InputFormat，它默认用的是TextInputFormat.class
//        job.setInputFormatClass(CombineTextInputFormat.class);
         //虚拟存储切片最大值设置4m
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        //设置reduceTask的数量
//        job.setNumReduceTasks(4);

        System.exit(b ? 0 : 1);

    }
}
