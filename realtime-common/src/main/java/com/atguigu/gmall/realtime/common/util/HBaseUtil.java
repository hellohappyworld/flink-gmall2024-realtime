package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import redis.clients.jedis.StreamInfo;

import java.io.IOException;

/**
 * 创建者：gml
 * 创建日期：2024-04-02
 * 功能描述：HBase工具类
 * 来源：https://www.bilibili.com/video/BV1dv421y7eu?p=29&vd_source=b6440733352819cc788f24606ec23fa3
 * https://www.bilibili.com/video/BV1dv421y7eu?p=30&vd_source=b6440733352819cc788f24606ec23fa3
 * https://www.bilibili.com/video/BV1dv421y7eu/?p=36&vd_source=b6440733352819cc788f24606ec23fa3
 */
public class HBaseUtil {

    /**
     * 创建HBase的同步连接
     *
     * @return
     */
    public static Connection getConnection() {
        //使用conf
//        Configuration conf = new Configuration();
//        conf.set("hbase.zookeeper.quorum", Constant.HBASE_Z00KEEPER_QUORUM);
//        Connection connection = ConnectionFactory.createConnection(conf);

        //使用配置文件 hbase-site.xml
        //idea搜索类快捷键：Ctrl+N
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭连接
     *
     * @param connection
     */
    public static void closeConnection(Connection connection) {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建表格
     *
     * @param connection HBase的同步连接
     * @param namespace  命名空间
     * @param table      表名
     * @param families   列族名，多个
     * @throws IOException 获取admin连接异常
     */
    public static void createTable(Connection connection, String namespace, String table, String... families) throws IOException {
        if (families == null || families.length == 0) {
            System.out.println("创建HBase至少有一个列族");
            return;
        }

        //1、获取admin
        Admin admin = connection.getAdmin();
        //2、创建表格描述
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, table));
        for (String family : families) {
            //获取列族描述
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }
        //3、使用admin调用方法创建表格
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            System.out.println("当前表格已经存在，不需要重复创建" + namespace + ":" + table);
        }
        //4、关闭admin
        try {
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除表格
     *
     * @param connection HBase的同步连接
     * @param namespace  命名空间
     * @param table      表名
     * @throws IOException 获取admin连接异常
     */
    public static void dropTable(Connection connection, String namespace, String table) throws IOException {
        //1、获取admin
        Admin admin = connection.getAdmin();
        //2、调用方法删除表格
        try {
            admin.disableTable(TableName.valueOf(namespace, table));
            admin.deleteTable(TableName.valueOf(namespace, table));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //3、关闭admin
        try {
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 写数据到HBase的方法
     *
     * @param connection 一个同步连接
     * @param namespace  命名空间
     * @param tableName  表名
     * @param rowKey     主键
     * @param family     列族名
     * @param data       列名和列值的json
     * @throws IOException
     */
    public static void putCells(Connection connection, String namespace, String tableName, String rowKey, String family, JSONObject data) throws IOException {
        //1、获取table
        Table table = connection.getTable(TableName.valueOf(tableName));
        //2、创建写入对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //循环填充列
        for (String column : data.keySet()) {
            String columnValue = data.getString(column);
            if (columnValue != null) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(columnValue));
            }
        }
        //3、写入数据
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //4、关闭table
        table.close();
    }

    /**
     * 删除整行数据
     *
     * @param connection 一个同步连接
     * @param namespace  命名空间名称
     * @param tableName  表名
     * @param rowKey     主键
     * @throws IOException
     */
    public static void deleteCells(Connection connection, String namespace, String tableName, String rowKey) throws IOException {
        //1、获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        //2、创建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //3、调用方法删除数据
        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //4、关闭table
        table.close();
    }

}
