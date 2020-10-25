package com.cjs.hadoopLearn.hbaseLearn.iHdfsOHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class IHdfsOHbaseDriver implements Tool {
    private Configuration configuration = null;

    @Override
    public int run(String[] strings) throws Exception {

        //创建 Job 任务
        Job job = Job.getInstance(configuration, "随便取一个job的名字");
        job.setJarByClass(IHdfsOHbaseDriver.class);
        Path inPath = new Path(strings[0]);
        FileInputFormat.addInputPath(job, inPath);

        //设置 Mapper
        job.setMapperClass(ReadFromHDFSMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置 Reducer
        TableMapReduceUtil.initTableReducerJob("cjstest", WriteToHbaeReducer.class, job);

        //设置 Reduce 数量，最少 1 个
        job.setNumReduceTasks(1);
        boolean isSuccess = job.waitForCompletion(true);
//        if (!isSuccess) {
//            throw new IOException("Job running with error");
//        }

        return isSuccess ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"/tmp/inputdata.txt"};
        Configuration configuration = new Configuration();

        int run = ToolRunner.run(configuration, new IHdfsOHbaseDriver(), args);

        System.exit(run);

    }
}
