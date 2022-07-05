package com.atguigu.util;

import com.atguigu.common.Constant;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author shogunate
 * @description TODO
 * @date 2022/7/5 11:48
 */
public class JdbcUtil {

    public static Connection getPhoenixConnection() {
        Connection conn = null;
        String driver = Constant.PHOENIX_DRIVER;
        String url = Constant.PHOENIX_URL;

        try {
            conn = getJdbcConnection(driver, url, null, null);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Invalid phoenix driver");
        } catch (SQLException e) {
            throw new RuntimeException("Check zookeeper or phoenix service is on");
        }

        return conn;
    }

    private static Connection getJdbcConnection(String driver, String url, String user, String password) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        return DriverManager.getConnection(url, user, password);
    }
}
