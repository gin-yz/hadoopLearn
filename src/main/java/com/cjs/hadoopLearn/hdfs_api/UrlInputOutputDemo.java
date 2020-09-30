package com.cjs.hadoopLearn.hdfs_api;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

public class UrlInputOutputDemo {

    @Test
    public void UrlHDFS() throws IOException {
        //注册ｕｒｌ
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        //获取ｈｄｆｓ文件输入流
        InputStream inputStream = new URL("hdfs://hadoop1:8020/hello.txt").openStream();
        //获取本地文件输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("/home/cjs/tmp_dir/hello.txt"));
        //文件的拷贝
        IOUtils.copy(inputStream,fileOutputStream);
        //关流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(fileOutputStream);

    }
}
