package com.cjs.hadoopLearn.mapReduceLearn.shuffleLearn.partitionerLearn;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PhoneDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String inputURL = PhoneDriver.class.getResource("phoneData.txt").toString();
        String outputURL = PhoneDriver.class.getResource(".").toString() + "/output";

        args = new String[]{inputURL, outputURL};
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(PhoneData.class);

        job.setMapperClass(PhoneMapper.class);
        job.setReducerClass(PhoneReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneData.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 8 指定自定义数据分区
        job.setPartitionerClass(PartitionerDemo.class);

        // 9 同时指定相应数量的reduce task
        job.setNumReduceTasks(5);


        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }
}
