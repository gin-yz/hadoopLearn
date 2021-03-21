/**
 * 务必使用JDK1.8!!!!!
 */

package com.cjs.hadoopLearn.hbaseLearn.hbaseAPILearn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
        String tableName = "testNP:cjsdsg"; //NameSpace:表名,不加ｎａｍｅｓｐａｃｅ的话为默认
        String[] columnFamily = new String[]{"info", "other", "hehe"}; //列族
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

    //插入数据
    @Test
    public void addRowData() throws IOException {
        String tableName = "cjsdsg";
        String rowKey = "428";
        String columnFamily = "info";
        String column = "name";
        String value = "dsg";
        //创建 HTable 对象
        HTable hTable = new HTable(conf, tableName);
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向 Put 对象中组装数据
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("插入数据成功");
    }

    //创建命名空间
    @Test
    public void createNameSpace() {
        String nameSpace = "testNP";
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(nameSpace);
        NamespaceDescriptor namespaceDescriptor = builder.build();
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //删除表
    public void dropTable(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功！");
        } else {
            System.out.println("表" + tableName + "不存在！");
        }
    }

    //插入数据
    public void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        //创建 HTable 对象
        HTable hTable = new HTable(conf, tableName);
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向 Put 对象中组装数据
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("插入数据成功");
    }

    //注意删除打标记问题，查看笔记
    @Test
    public void deleteMultiRow() throws IOException {
        String tableName = "student";
        String[] rows = {"666"};
        HTable hTable = new HTable(conf, tableName);
        List<Delete> deleteList = new ArrayList<Delete>();
        for (String rowKey : rows) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
    }

    //新ａｐｉ，得到ｔａｂｌｅ后再操作
    @Test
    public void deleteTest() throws IOException {
        String tableName = "student";
        String rowKey = "666";
        Table table = this.connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumns(Bytes.toBytes("info"),Bytes.toBytes("name"));
        table.delete(delete);
        table.close();

    }

    //获取table表的所有数据
    @Test
    public void getAllRows() throws IOException {
        String tableName = "student";
        HTable hTable = new HTable(conf, tableName);
        //得到用于扫描 region 的对象
        Scan scan = new Scan();
        scan.setMaxVersions(3);
        //使用 HTable 得到 resultcanner 实现类的对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells(); //列族
            for (Cell cell : cells) {
                //得到 rowkey
                System.out.println(" 行 键 :" +
                        Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println(" 列 族 " +
                        Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(" 列 :" +
                        Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(" 值 :" +
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    //获得某一列
    public void getRow(String tableName, String rowKey) throws
            IOException {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(" 行 键 :" +
                    Bytes.toString(result.getRow()));
            System.out.println(" 列 族 " +
                    Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(" 列 :" +
                    Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(" 值 :" +
                    Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }

    //获取某一行指定“列族:列”的数据
    public void getRowQualifier(String tableName, String rowKey, String family, String qualifier) throws IOException {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(" 行 键 :" +
                    Bytes.toString(result.getRow()));
            System.out.println(" 列 族 " +
                    Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(" 列 :" +
                    Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(" 值 :" +
                    Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }
}
