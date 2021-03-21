package com.cjs.hadoopLearn.hbaseLearn.iHbaseOHdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HbaseToHadoopDriver implements Tool {
    private Configuration configuration;
    @Override
    public int run(String[] strings) throws Exception {
        //得到 Configuration
        Configuration conf = this.getConf();
        //创建 Job 任务
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(HbaseToHadoopDriver.class);

        //配置 scan,可以指定表的过滤选项
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);

        //设置 Mapper，注意导入的是 mapreduce 包下的，不是 mapred 包下的，后者是老版本
        TableMapReduceUtil.initTableMapperJob(
                strings[0], //数据源的表名
                scan, //scan 扫描控制器
                HbaseToHadoopMapper.class,//设置 Mapper 类
                Text.class,//设置 Mapper 输出 key 类型
                NullWritable.class,//设置 Mapper 输出 value 值类型
                job//设置给哪个 JOB
        );

        //设置 Reducer
//        TableMapReduceUtil.initTableReducerJob(strings[1] //输出的表名
//                , WriteToHbaeReducer.class, job);
        job.setReducerClass(HbaseToHadoopreducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        //设置 Reduce 数量
        job.setNumReduceTasks(1);

        boolean isSuccess = job.waitForCompletion(true);
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
        Configuration configuration = new Configuration();

        int run = ToolRunner.run(configuration, new HbaseToHadoopDriver(), args);

        System.exit(run);

    }
}
