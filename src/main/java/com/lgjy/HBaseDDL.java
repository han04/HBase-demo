package com.lgjy;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class HBaseDDL {
    public static Connection  connection = HBaseConnection.connection;

    /**
     * 创建‘命名空间’
     * @param namespace 命名空间名称
     */
    public static void createNamespace(String namespace) throws IOException {
        //1.获取admin
        //admin 连接是轻量级的 ，  不是线程安全的
        //不推荐 池化 或者 缓存 这个连接
        Admin admin = connection.getAdmin();
        //2.调用方法创建命名空间
        //2.1 创建Namespace描述的builder
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);

        //2.2给namespace添加需求
        builder.addConfiguration("user","lgjy");

        //2.3使用builder构建出对应完整参数的对象 完成创建
        try {
            admin.createNamespace(builder.build());
        } catch (IOException e) {
            System.out.println("Namespace already exists~~~~~");
        }

        //3.关闭admin
        admin.close();
    }

    /**
     * 判断table是否存在
     * @param namespace
     * @param tableName
     * @return  true表示存在
     */
    public static boolean isTableExists(String namespace ,String tableName) throws IOException {
        //1.获取 admin
        Admin admin = connection.getAdmin();
        //2.使用方法判断table是否存在
        boolean b = false;
        try {
            b = admin.tableExists(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //3.关闭admin连接
        admin.close();
        //4..返回结果
        return b;

    }

    public static void main(String[] args) throws IOException {
        //测试创建namespace
        createNamespace("test_ns");
        //测试table是否存在
        System.out.println(isTableExists("bigdata12", "student"));
        System.out.println("Others");

        //关闭HBase连接
        HBaseConnection.closeConnection();
    }
}
