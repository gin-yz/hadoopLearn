package com.cjs.hadoopLearn.hdfs_api;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class HDFS_CURD {

    FileSystem fs = null;
    @Before
    public void getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop1:8020");
        System.setProperty("HADOOP_USER_NAME", "root");
        fs = FileSystem.get(conf);
    }

    @After
    public void destory() throws IOException {
        fs.close();
    }

    /*
    * 遍历文件
    * */
    @Test
    public void listMyFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"), true);

        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            //返回文件的路进
            System.out.println(next.getPath());
            //文件的ｂｌｏｃｋ大小
            System.out.println(next.getBlockSize());
            //文件的权限
            System.out.println(next.getPermission());
            //文件的内容长度
            System.out.println(next.getLen());
            //文件块信息
            BlockLocation[] blockLocations = next.getBlockLocations();
            for (BlockLocation b1:blockLocations){
                //文件在几个ｄａｔａｎｏｄｅ中
                System.out.println(Arrays.toString(b1.getHosts()));
                System.out.println(b1.getLength());
                System.out.println(b1.getOffset());
            }

        }

    }


    /*
    * 文件夹操作
    * */
    @Test
    public void mkdirs() throws IOException {
        boolean mkdirs = fs.mkdirs(new Path("/test/test1"));
        boolean mkdirs1 = fs.mkdirs(new Path("/test/test2"));

        //重命名
        boolean rename = fs.rename(new Path("/test/test1"), new Path("/test/test3"));

        //非空文件夹至必须为ｔｕｒｅ
        boolean delete = fs.delete(new Path("/test/test2"),true);

        System.out.println(mkdirs);
        System.out.println(mkdirs1);
        System.out.println(rename);
        System.out.println(delete);

    }

    /*
    * 将本地文件上传至ｈｄｆｓ,下载使用copyToLocalFile方法
    * */
    @Test
    public void addFileToHdfs() throws IOException {
        Path localpath = new Path("file:///home/cjs/tmp_dir/hdfs");

        Path hdfspath = new Path("/test/test3");

        fs.copyFromLocalFile(localpath,hdfspath);

    }

    /*
    * 小文件的合并
    * */
    @Test
    public void mergeSmallFile() throws IOException {
        //获取hdfs大文件输出流
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/test/big_txt.txt")); //在ｈｄｆｓ系统上创建一个大文件输入流
        //获取一个本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        //获取本地文件夹下所有文件详情
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("file:///home/cjs/tmp_dir/hdfs"));
        //遍历每个文件，获取每个文件输入流
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());
            //将小文件复制进大文件
            IOUtils.copy(inputStream,fsDataOutputStream);
            IOUtils.closeQuietly(inputStream);
        }
        //关闭流
        IOUtils.closeQuietly(fsDataOutputStream);
        localFileSystem.close();
    }



}
