package bean;

import lombok.Data;

/**
 * @author yycstart
 * @create 2021-02-09 15:37
 */
@Data
public class TableProcess {

    public static final String SINK_TYPE_HBASE = "HBASE";
    public static final String SINK_TYPE_KAFKA = "KAFKA";
    public static final String SINK_TYPE_CK = "CLICKHOUSE";

    String sourceTable;
    String operateType;
    String sinkType;
    String sinkTable;
    String sinkColumns;
    String sinkPk;
    String sinkExtend;
}

