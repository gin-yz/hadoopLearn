/*
 * 将本地文件写入到ｈｄｆｓ中，其中ｐｒｏｇｒｅｓｓ是回调方法，每次写完后进行回调，通知其他程序已经做完
 * create方法为需要写入的且当前不存在的文件创建父目录，若不希望这样．可使用exists()方法检查目录是否存在
 * */

package com.cjs.hadoopLearn.hdfs_api.bookLearn;// cc FileCopyWithProgress Copies a local file to a Hadoop filesystem, and shows progress

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

// vv FileCopyWithProgress
public class FileCopyWithProgress {
    public static void main(String[] args) throws Exception {
        String localSrc = args[0];
        String dst = args[1];

        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fs.create(new Path(dst), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096, true);
    }

}
// ^^ FileCopyWithProgress
