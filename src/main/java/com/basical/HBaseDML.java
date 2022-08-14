package com.basical;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDML {
    //静态属性
    public static Connection connection = HBaseConnection.connection;

    /**
     * 插入数据
     *
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @param value
     */
    public static void putCell(String namespace, String tableName, String rowKey, String columnFamily, String columnName, String value) throws IOException {
        //1.获取Table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        //2.调用方法插入
        //2.1创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //2.2给put对象添加数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //3.关闭table
        table.close();
    }

    /**
     * 读取数据  读取对应的一行的某一列
     *
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     */
    public static void getCell(String namespace, String tableName, String rowKey, String columnFamily, String columnName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        //目前get一整行

        //读取特定列 需要添加参数
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        //设置读取的版本
        get.readAllVersions();

        try {
            //读取数据 得到result对象
            Result result = table.get(get);
            //处理数据
            Cell[] cells = result.rawCells();

            //处理数据的方法...
            System.out.println("我在处理数据中");

            for (Cell cell : cells) {
                String value = new String(CellUtil.cloneValue(cell));
                System.out.println(value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //关闭table
        table.close();
    }

    /**
     * 扫描数据
     * @param namespace
     * @param tableName
     * @param startRow 包含
     * @param stopRow  不包含
     */
    public static void scanRows(String namespace, String tableName, String startRow, String stopRow) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        //2.创建 scan对象
        Scan scan = new Scan();
        //此时会扫描整张表

        //添加起止row
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));
        //可以加第二个boolean参数 控制是否包含该row
        try {
            //读取多行数据 获得scanner
            ResultScanner scanner = table.getScanner(scan);

            //result记录一行数据 cell数组
            //ResultScanner记录多行数据 result数组
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();

                for (Cell cell : cells) {
                    System.out.print(new String(CellUtil.cloneRow(cell)) +"-"+ new String(CellUtil.cloneFamily(cell)) +"-"+
                            new String(CellUtil.cloneQualifier(cell)) +"-"+ new String(CellUtil.cloneValue(cell))+"\t");
                }
                System.out.println("");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.close();

    }

    public static void main(String[] args) throws IOException {
//        putCell("bigdata","student","2001","info","name","zhangsan");
//        putCell("bigdata","student","2001","info","name","lisi");
//        putCell("bigdata","student","2001","info","name","wangwu");
//        getCell("bigdata","student","2001","info","name");
        scanRows("bigdata","student","1001","2004");
        System.out.println("Others~");
        connection.close();
    }
}
