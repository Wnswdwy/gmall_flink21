package process;

import bean.TableProcess;
import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.MySQLUtil;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * @author yycstart
 * @create 2021-02-09 15:44
 *
 *      自定义算子，实现将数据分流的业务
 */
public class TableProcessFunction extends ProcessFunction<JSONObject,JSONObject> {

    //侧输出流标签
    private OutputTag<JSONObject> outputTag;

    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    //用于内存中存储表配置对象【表名，表配置信息】
    private Map<String, TableProcess> tableProcessMap = null;

    //表示目前内存中已经存在的HBase表
    private Set<String> existsTables = new HashSet<>();

    private Connection connection = null;


    /**
     * 声明周期方法，初始化连接，开启定时任务
     * @param parameters
     * @throws Exception
     *      //生命周期方法,初始化连接,初始化配置表信息并开启定时任务,用于不断读取配置表信息
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181");

        //初始化配置表信息
        tableProcessMap = refreshMeta();

        //开启定时任务,用于不断读取配置表信息
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                tableProcessMap = refreshMeta();
            }
        }, 5000, 5000);

    }




    /**
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void processElement(JSONObject value, Context context, Collector<JSONObject> collector) throws Exception {
        String  table = value.getString("table");
        String  type = value.getString("type");
        JSONObject data = value.getJSONObject("data");

        if(data == null || data.size() <= 3){
            return;
        }

        if(type.equals("bootstrap-insert")){
            type = "insert";
            value.put("type",type);
        }

        if(tableProcessMap != null && tableProcessMap.size() > 0){
            String key = table + ":" + type;
            TableProcess tableProcess = tableProcessMap.get(key);

            if(tableProcess != null){
                value.put("sink_table",tableProcess.getSinkType());
                if(tableProcess.getSinkColumns() != null && tableProcess.getSinkColumns().length() > 0){
                    filterColumn(value.getJSONObject("data"),tableProcess.getSinkColumns());
                }
            }else {
                System.out.println("No This Key:" + key);
            }

            if(tableProcess != null && TableProcess.SINK_TYPE_HBASE.equalsIgnoreCase(tableProcess.getSinkType())){
                context.output(outputTag,value);
            }else if (tableProcess != null && TableProcess.SINK_TYPE_KAFKA.equalsIgnoreCase(tableProcess.getSinkType())){
                collector.collect(value);
            }
        }
    }


    /**
     * 校验字段，过滤掉多余的字段
     * @param data
     * @param sinkColumns
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] columns = StringUtils.split(sinkColumns, ",");
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        List<String> columnList = Arrays.asList(columns);
        entries.removeIf(next -> !columnList.contains(next.getKey()));
    }


    /**
     * //用于检查字段，配置表中添加了新数据,在Phoenix中创建新的表
     * @param tableName
     * @param fields
     * @param pk
     * @param ext
     */
    private void checkTable(String tableName,String fields,String pk,String ext){

        //主键不存在，给定默认值
        if(pk == null) pk = "id";

        //扩展字段不存在，给定默认值
        if(ext == null)   ext = "";

        //创建字符串拼接对象,用于拼接建表语句SQL
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");

        //将列做切分,并拼接至建表语句SQL中
        String[] fieldsArr = fields.split(",");
        for (int i = 0; i < fieldsArr.length; i++) {
            String field = fieldsArr[i];
            if (pk.equals(field)) {
                createSql.append(field).append(" varchar");
                createSql.append(" primary key ");
            } else {
                createSql.append("info.").append(field).append(" varchar");
            }
            if (i < fieldsArr.length - 1) {
                createSql.append(",");
            }
        }

        createSql.append(")");
        createSql.append(ext);

        try {
            //执行建表语句在Phoenix中创建表
            Statement statement = connection.createStatement();
            System.out.println(createSql);
            statement.execute(createSql.toString());
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表失败！！！");
        }
    }


    /**
     * //用于读取配置表信息存入内存的方法
     * @return
     */
    private Map<String, TableProcess> refreshMeta() {
        System.out.println("更新处理信息");

        //创建HashMap用于存放结果数据
        HashMap<String, TableProcess> processHashMap = new HashMap<>();

        //查询MySQL中的配置表数据
        List<TableProcess> tableProcessList = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);

        //遍历查询结果，将数据存入结果集合
        for (TableProcess tableProcess : tableProcessList) {
            //获取源表表名
            String sourceTable = tableProcess.getSourceTable();
            //获取数据操作类型
            String operateType = tableProcess.getOperateType();
            //获取结果表表名
            String sinkTable = tableProcess.getSinkTable();
            //拼接结果表表名
            String key = sourceTable + ":" + operateType;
            //将数据存入结果集合
            tableProcessMap.put(key,tableProcess);

            if ("insert".equals(operateType) && "hbase".equals(sinkTable)) {
                boolean noExist = existsTables.add(sourceTable);

                //如果表信息数据不存在内存,则在Phoenix中创建新的表
                if (noExist) {
                    checkTable(sinkTable, tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());
                }
            }
        }

        if (tableProcessMap.size() == 0) {
            throw new RuntimeException("缺少处理信息");
        }

        //返回最新的配置表数据信息
        return processHashMap;
    }



}
