package cn.edu.bit.linc.FieldConverter;

import java.util.ArrayList;

/**
 * Created by ihainan on 3/18/15.
 */
public class DependentTable {
    public String tableName;    // 表名，可为空
    public String alias;        // 表别名，可为空
    public ArrayList<String> fields;   // 该表依赖的其他表，以及依赖表中对应的字段
}
