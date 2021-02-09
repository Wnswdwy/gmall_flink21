package utils;

import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yycstart
 * @create 2021-02-09 17:41
 */
public class PhoenixUtil {

    public static Connection conn = null;

    public static void main(String[] args) {

        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch ( Exception e) {
            e.printStackTrace();
        }

        List<JSONObject> objectList = queryList("select * from DIM_USER_INFO", JSONObject.class);
        System.out.println(objectList);
    }

    public static  void queryInit() {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static <T> List<T> queryList(String sql, Class<T> clazz) {
        if(conn==null){
            queryInit();
        }
        List<T> resultList = new ArrayList();
        Statement stat = null;
        try {
            stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(sql);
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                T rowData = clazz.newInstance();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    BeanUtils.setProperty(rowData, md.getColumnName(i), rs.getObject(i));
                }
                resultList.add(rowData);
            }
            stat.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultList;
    }
}

