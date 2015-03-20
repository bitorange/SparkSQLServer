package cn.edu.bit.linc.FieldConverter;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ihainan on 3/20/15.
 */
public class Rule {
    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getRex() {
        return rex;
    }

    public String getReplaceContent() {
        return replaceContent;
    }

    private String tableName;
    private String columnName;
    private String rex;
    private String replaceContent;

    public Rule(String tableName, String columnName, String rex, String replaceContent){
        this.tableName = tableName;
        this.columnName = columnName;
        this.rex = rex;
        this.replaceContent = replaceContent;
    }


    @Override
    public String toString() {
        return this.tableName + "\t" + this.columnName + "\t" + this.rex + "\t" + this.replaceContent;
    }

    /** Apply this rule to specified content **/
    public String applyRule(String tableName, String columnName, String originalContent){
        String newContent;
        if(checkAvaliable(tableName, columnName)) {
            Pattern pattern = Pattern.compile(this.rex);
            Matcher matcher = pattern.matcher(originalContent);
            newContent = matcher.replaceAll(this.replaceContent);
        }
        else {
            System.err.println("Err: 表或列名不符合");
            return null;
        }
        return newContent;
    }

    /** Check whether a rule is suitable for a specified table & column **/
    public Boolean checkAvaliable(String tableName, String columnName){
        return tableName.equals(this.tableName) && columnName.equals(this.columnName);
    }
}
