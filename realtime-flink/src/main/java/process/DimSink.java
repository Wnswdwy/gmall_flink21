package process;

import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Set;


/**
 * @author yycstart
 * @create 2021-02-09 17:14
 *      DimSink
 *      继承了RickSinkFunction,这个function的分两条时间线。
 * 一条是任务启动时执行open操作（图中紫线），我们可以把连接的初始化工作放在此处一次性执行。
 * 另一条是随着每条数据的到达反复执行invoke()（图中黑线）,在这里面我们要实现数据的保存，
 * 主要策略就是根据数据组合成sql提交给hbase。
 *      //8.将侧输出流数据写入HBase(Phoenix)
 */
public class DimSink extends RichSinkFunction<JSONObject> {
    //声明连接
    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }


    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        String tableName = value.getString("sink_table");
        JSONObject data = value.getJSONObject("data");

        if(data != null && data.size() > 0){
            try {
                String upsertSql = genUpsertSql(tableName.toUpperCase(), data);
                System.out.println(upsertSql);
                Statement statement = connection.createStatement();
                statement.executeQuery(upsertSql);
                connection.commit();
                statement.close();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("执行SQL失败");
            }
        }
    }

    private String genUpsertSql(String tableName,JSONObject data){
        Set<String> fields = data.keySet();
        String upsertSql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(" + StringUtils.join(fields, ",") + ")";
        String valuesSql = " values('" + StringUtils.join(data.values(), "','") + "')";
        return upsertSql + valuesSql;
    }
}
