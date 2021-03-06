package com.flinkstreamingdemo;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;

public class DbUtils {
    private static DruidDataSource dataSource;

    public static Connection getConnection() throws Exception {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://ip:port/database");
        dataSource.setUsername("username");
        dataSource.setPassword("password");
        //设置初始化连接数，最大连接数，最小闲置数
        dataSource.setInitialSize(10);
        dataSource.setMaxActive(50);
        dataSource.setMinIdle(5);
        //返回连接
        return  dataSource.getConnection();
    }
}
