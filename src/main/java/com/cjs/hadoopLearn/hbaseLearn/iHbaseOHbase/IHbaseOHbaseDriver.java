package com.cjs.hadoopLearn.hbaseLearn.iHbaseOHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IHbaseOHbaseDriver implements Tool {
    private Configuration configuration;
    @Override
    public int run(String[] strings) throws Exception {
        //得到 Configuration
        Configuration conf = this.getConf();

        //创建 Job 任务
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(IHbaseOHbaseDriver.class);

        //配置 scan,可以指定表的过滤选项
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);

        //设置 Mapper，注意导入的是 mapreduce 包下的，不是 mapred 包下的，后者是老版本
        TableMapReduceUtil.initTableMapperJob(
                strings[0], //数据源的表名
                scan, //scan 扫描控制器
                ReadFromHbaseMapper.class,//设置 Mapper 类
                ImmutableBytesWritable.class,//设置 Mapper 输出 key 类型
                Put.class,//设置 Mapper 输出 value 值类型
                job//设置给哪个 JOB
        );

        //设置 Reducer
        TableMapReduceUtil.initTableReducerJob(strings[1], WriteToHbaeReducer.class, job);

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

        int run = ToolRunner.run(configuration, new IHbaseOHbaseDriver(), args);

        System.exit(run);

    }
}
