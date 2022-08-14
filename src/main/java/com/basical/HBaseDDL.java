package com.basical;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

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

    /**
     * 创建表格
     * @param namespace
     * @param tableName
     * @param columnFamilies
     */
    public static void createTable(String namespace, String tableName, String... columnFamilies) throws IOException {
        if(columnFamilies.length == 0){
            System.out.println("至少有一个列族~");
            return;
        }

        if(isTableExists(namespace,tableName)){
            System.out.println("表格已存在~");
            return;
        }

        Admin admin = connection.getAdmin();
        //调用方法创建表格
        //1 创建table Descriptor的builder
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName
                .valueOf(namespace,tableName));
        //2.添加参数
        for(String columnFamily : columnFamilies){
            //3.创建列族描述 ColumnFamilyDescriptor
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
            //4.当前列族添加参数
            columnFamilyDescriptorBuilder.setMaxVersions(5);

            //5.创建 添加完参数的列族描述
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }
        //创建 对应的表格描述
        try {
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("success");
        } catch (IOException e) {
            System.out.println("table already exists");
            throw new RuntimeException(e);
        }
        //关闭admin
        admin.close();

    }

    /**
     *  修改表格中一个columnFamily的version
     * @param namespace
     * @param tableName
     * @param columnFamily
     * @param version
     */
    public static void modifyTable(String namespace, String tableName, String columnFamily, int version) throws IOException {
        //判断该table是否存在
        if (!isTableExists(namespace, tableName)) {
            System.out.println("Table doesn't exist~~");
            return;
        }
        //1.获取admin
        Admin admin = connection.getAdmin();
        try {
            //2.调用方法修改表格
            //2.0 获取之前的TableDescriptor
            TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(namespace, tableName));

            //2.1 创建一个TableDescriptorBuilder
            //如果使用填写TableName的方法，相当于创建一个新的Table descriptor builder没有之前的信息
            //所以使用TableDescriptorBuilder来调用方法
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);
            //2.2对应的builder进行数据更改
            ColumnFamilyDescriptor columnFamily1 = descriptor.getColumnFamily(Bytes.toBytes(columnFamily));

            //创建ColumnFamilyDescriptorBuilder
            //填写旧的
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(columnFamily1);
            //修改对应的版本
            columnFamilyDescriptorBuilder.setMaxVersions(version);

            tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());
            admin.modifyTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        admin.close();
    }

    /**
     * 删除表格
     * @param namespace
     * @param tableName
     * @return true表示删除成功
     */
    public static boolean deleteTable(String namespace, String tableName) throws IOException {
        if (!isTableExists(namespace, tableName)) {
            System.out.println("this table does exist~");
            return false;
        }

        Admin admin = connection.getAdmin();
        try {
            TableName tableName1 = TableName.valueOf(namespace, tableName);

            admin.disableTable(tableName1);
            admin.deleteTable(tableName1);

        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();
        return true;
    }

    public static void main(String[] args) throws IOException {
        //测试创建namespace
        //createNamespace("test_ns");
        //测试table是否存在
        //System.out.println(isTableExists("bigdata12", "student"));
        //测试创建表格
        createTable("test_ns","student33","info1","msg");

        System.out.println("Others");

        //关闭HBase连接
        HBaseConnection.closeConnection();
    }
}
