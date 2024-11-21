package com.atguigu.gmall.realtime.common.util;


import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.shaded.guava30.com.google.common.base.CaseFormat;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static com.atguigu.gmall.realtime.common.constant.Constant.*;

/**
 * 创建者：gml
 * 创建日期：2024-04-08
 * 功能描述：jdbc连接数据库工具类
 */
public class JdbcUtil {
    public static Connection getMysqlConnection() throws ClassNotFoundException, SQLException {
        //获取 jdbc 链接
        //1、加载驱动
        Class.forName(MYSQL_DRIVER);
        return DriverManager.getConnection(MYSOL_URL, MYSQL_USER_NAME, MYSQL_PASSWORD);
    }

    /**
     * 执行一个查询语句，把查询到的结果封装到T类型的对象中
     *
     * @param conn               mysql链接
     * @param querySql           查询的sql语句：必须是查询语句
     * @param tClass             T类
     * @param isUnderlineToCamel 判断是否转小驼峰
     * @param <T>                每行封装的类型
     * @return 查询得到的多行结果
     * @throws Exception
     */
    public static <T> List<T> queryList(Connection conn, String querySql, Class<T> tClass, boolean... isUnderlineToCamel) throws Exception {
        boolean defaultIsUToC = false;
        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }
        ArrayList<T> result = new ArrayList<>();
        //1、预编译
        PreparedStatement preparedStatement = conn.prepareStatement(querySql);
        //2、执行查询 获得结果集
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        //3、解析结果集，把数据封装到一个List集合汇总
        while (resultSet.next()) {
            //遍历到一行数据，把这行数据封装到一个T类型的对象中
            T t = tClass.newInstance();//使用反射创建一个T类型的对象
            //遍历这一行的每一列数据
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                //获取列名
                String name = metaData.getColumnLabel(i);
                //获取列值
                Object value = resultSet.getObject(name);

                if (defaultIsUToC) {// 需要下划线转驼峰： a_a => aA a_aaaa_aa => aAaaaAa
                    name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name);
                }
                // t.name=value BeanUtils会将value写入到T类(java bean)的name属性中
                BeanUtils.setProperty(t, name, value);
            }
            result.add(t);
        }
        return result;
    }

    public static void closeConnection(Connection conn) throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }
}