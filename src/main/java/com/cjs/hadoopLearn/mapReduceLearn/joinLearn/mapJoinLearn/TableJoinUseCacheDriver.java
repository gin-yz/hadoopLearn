package com.cjs.hadoopLearn.mapReduceLearn.joinLearn.mapJoinLearn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

public class TableJoinUseCacheDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        // 0 根据自己电脑路径重新配置
        args = new String[] { TableJoinUseCacheDriver.class.getResource("order.txt").toString(),TableJoinUseCacheDriver.class.getResource(".").toString()+"/output" };

        // 1 获取job信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置加载jar包路径
        job.setJarByClass(TableJoinUseCacheDriver.class);

        // 3 关联map
        job.setMapperClass(TableJoinUseCacheMap.class);

        // 4 设置最终输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 5 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 加载缓存数据
        job.addCacheFile(TableJoinUseCacheDriver.class.getResource("pd.txt").toURI());

        // 7 Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
        job.setNumReduceTasks(0);

        // 8 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
