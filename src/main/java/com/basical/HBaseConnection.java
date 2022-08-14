package com.basical;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;


public class HBaseConnection {

    public static Connection connection = null;
    static {
        //创建连接
        // 默认使用同步连接
        try {
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void closeConnection() throws IOException {
        if(connection != null){
            connection.close();
        }
    }

    public static void main(String[] args) throws IOException {

//        //创建连接配置对象
//        Configuration conf = new Configuration();
//        //添加配置参数
//        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
//        //创建连接
//        connection = ConnectionFactory.createConnection(conf);
//        CompletableFuture<AsyncConnection> asyncConnection = ConnectionFactory.createAsyncConnection(conf);
//
//        //4.使用连接
//        System.out.println(connection);
//
//        //5.关闭连接
//        connection.close();

        //直接使用连接
        //不要在main线程里单独创建
        System.out.println(HBaseConnection.connection);
        //用封装好的关闭连接
        HBaseConnection.closeConnection();
    }
}
