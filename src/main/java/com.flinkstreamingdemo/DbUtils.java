package com.xiaopeng;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;

public class DbUtils {
    private static DruidDataSource dataSource;

    public static Connection getConnection() throws Exception {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://10.192.15.60:4306/xp_ems_t");
        dataSource.setUsername("XP_Admin");
        dataSource.setPassword("XP_AdminXP_Admin");
        //设置初始化连接数，最大连接数，最小闲置数
        dataSource.setInitialSize(10);
        dataSource.setMaxActive(50);
        dataSource.setMinIdle(5);
        //返回连接
        return  dataSource.getConnection();
    }
}
