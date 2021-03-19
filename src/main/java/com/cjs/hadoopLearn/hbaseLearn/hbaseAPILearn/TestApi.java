/**
 * 务必使用JDK1.8!!!!!
 */

package com.cjs.hadoopLearn.hbaseLearn.hbaseAPILearn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class TestApi {
    private Configuration conf;
    private Connection connection;
    private HBaseAdmin admin;

    //    static {
//        //使用 HBaseConfiguration 的单例方法实例化
//        conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "192.168.126.130,192.168.126.131,192.168.126.129");
////        conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//    }
    @Before
    public void inintConf() throws IOException {
        //使用 HBaseConfiguration 的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.126.130,192.168.126.131,192.168.126.129");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        connection = ConnectionFactory.createConnection(conf);
        admin = (HBaseAdmin) connection.getAdmin();//判断表是否存在
    }

    @After
    public void destory() throws IOException {
        admin.close();
        connection.close();
    }

    //判断表是否存在
    @Test
    public void isTableExist() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        String tableName = "student1";
        //在 HBase 中管理、访问表需要先创建 HBaseAdmin 对象
//        HBaseAdmin admin = new HBaseAdmin(conf);
        boolean b = admin.tableExists(tableName);
        System.out.println(b);
    }

    //创建表
    @Test
    public void createTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        String tableName = "cjsdsg"; //表名
        String[] columnFamily = new String[]{"info","other","hehe"}; //列族
        if (admin.tableExists(tableName)) {
            System.out.println("表" + tableName + "已存在");
            //System.exit(0);
        } else {
            //创建表属性对象,表名需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : columnFamily) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                hColumnDescriptor.setVersions(1, 3);
                descriptor.addFamily(hColumnDescriptor);
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功！");
            admin.close();
        }
    }

    //删除表
    @Test
    public void dropTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        String tableName = "cjsdsg";
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功！");
        } else {
            System.out.println("表" + tableName + "不存在！");
        }
    }


}
