package utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yycstart
 * @create 2021-02-09 15:39
 */
public class MySQLUtil {

    public static <T> List<T> queryList(String sql, Class<T> clazz, Boolean underScoreToCamel) {

        //获取MySQL连接
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall_test?characterEncoding=utf-8&useSSL=false", "root", "123456");

            //创建SQL执行语句
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            //获取查询结果中的元数据信息
            ResultSetMetaData metaData = resultSet.getMetaData();

            //创建集合用于存放结果数据
            ArrayList<T> resultList = new ArrayList<>();

            //遍历数据,将每行数据封装成对象存入集合
            while (resultSet.next()) {
                T obj = clazz.newInstance();
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    if (underScoreToCamel) {
                        CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    BeanUtils.setProperty(obj, columnName, resultSet.getObject(i));
                }
                resultList.add(obj);
            }

            //释放资源
            statement.close();
            connection.close();

            //返回结果
            return resultList;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询数据失败！！！");
        }
    }
}
