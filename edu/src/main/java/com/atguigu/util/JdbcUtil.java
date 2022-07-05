package com.atguigu.util;

import com.atguigu.common.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author shogunate
 * @description JdbcUtil
 * @date 2022/7/5 11:48
 */
public class JdbcUtil {

    public static Connection getPhoenixConnection() {
        Connection conn;
        String driver = Constant.PHOENIX_DRIVER;
        String url = Constant.PHOENIX_URL;

        try {
            conn = getJdbcConnection(driver, url, null, null);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Invalid Phoenix driver");
        } catch (SQLException e) {
            throw new RuntimeException("Check Zookeeper or Phoenix service is on and hbase-site.xml");
        }

        return conn;
    }

    private static Connection getJdbcConnection(String driver, String url, String user, String password) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        return DriverManager.getConnection(url, user, password);
    }

    public static <T> List<T> queryList(Connection conn,
                                        String sql,
                                        Object[] args,
                                        Class<T> tClass, Boolean... isUnderLineToLowerCamel) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {
        boolean isToCamel = false;

        if (isUnderLineToLowerCamel.length > 0) {
            isToCamel = true;
        }

        ArrayList<T> result = new ArrayList<>();
        PreparedStatement ps = conn.prepareStatement(sql);

        for (int i = 0; args != null && i < args.length; i++) {
            ps.setObject(i + 1, args[i]);
        }
        ResultSet resultSet = ps.executeQuery();

        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {
            T t = tClass.newInstance();
            for (int i = 1; i <= columnCount; i++) {
                String columnLabel = metaData.getColumnLabel(i);
                Object columnValue = resultSet.getObject(columnLabel);

                if (isToCamel) columnLabel = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnLabel);

                BeanUtils.setProperty(t, columnLabel, columnValue);
            }
            result.add(t);
        }
        return result;
    }

//    public static void main(String[] args) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException {
//        List<TableProcess> list = queryList(getJdbcConnection("com.mysql.jdbc.Driver", "jdbc:mysql://hadoop302:3306/gmall_config?useSSL=false", "root", "123456"),
//            "select * from table_process_edu_dim",
//            null,
//            TableProcess.class,
//            true
//        );
//
////        List<JSONObject> list = queryList(getPhoenixConnection(), "select * from dim_course_info where id = ?", new Object[]{"46"}, JSONObject.class);
//
//        for (Object object : list) {
//            System.out.println(object);
//        }
//    }
}



















