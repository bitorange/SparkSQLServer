package cn.edu.bit.linc.FieldConverter;

import java.util.ArrayList;

/**
 * Created by ihainan on 3/18/15.
 */
public class TableInfo {
    public String tableName;
    public String aliasName;
    public ArrayList<String> fields = new ArrayList<String>();

    public void print(){
        System.out.println(tableName + " - " + aliasName);
        for(String field: fields){
            System.out.println(field + " ");
        }
        System.out.println();
    }
}
