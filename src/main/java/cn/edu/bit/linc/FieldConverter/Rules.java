package cn.edu.bit.linc.FieldConverter;

import javax.xml.transform.Result;
import java.io.*;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;

/**
 * Created by ihainan on 3/20/15.
 */
public class Rules {
    private ArrayList<Rule> allRules = new ArrayList<Rule>();
    private static String ruleFilePath;

    static{
        ruleFilePath = ResourceBundle.getBundle("conf").getString("rulesFile");
    }

    public Rules(){
        this.readRules();
    }

    /** Read rules for local rule file **/
    public Boolean readRules(){
        allRules = new ArrayList<Rule>();

        // Open the file
        FileInputStream fstream = null;
        try {
            fstream = new FileInputStream(ruleFilePath);
        } catch (FileNotFoundException e) {
            System.err.println("Err: 规则文件 " + ruleFilePath + " 未找到");
            return false;
            // e.printStackTrace();
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        String strLine;

        // Read file line by line
        try {
            while ((strLine = br.readLine()) != null)   {
                String[] items = strLine.split("\t");
                Rule rule = new Rule(items[0], items[1], items[2], items[3]);
                allRules.add(rule);
            }
        } catch (IOException e) {
            System.err.println("Err: 读取规则文件 " + ruleFilePath + " 失败");
            return false;
        }

        // Close the input stream
        try {
            br.close();
        } catch (IOException e) {
            System.err.println("Err: 关闭规则文件 " + ruleFilePath + " 失败");
            return false;
            // e.printStackTrace();
        }
        return true;
    }

    /** Apply Rules **/
    public String applyRules(String tableName, String columnName, String originalContent){
        String newContent = originalContent;
        for(Rule rule: allRules){
            if(rule.checkAvaliable(tableName, columnName)){
                newContent = rule.applyRule(tableName, columnName, originalContent);
            }
        }
        return newContent;
    }

    /** Add new rule **/
    public Boolean addNewRule(Rule rule){
        readRules();
        if(!allRules.contains(rule)){
            allRules.add(rule);

            // 写入文件
            try {
                Writer output = new BufferedWriter(new FileWriter(this.ruleFilePath, true));
                output.append("\n" + rule.toString());
                output.close();
            } catch (IOException e) {
                System.err.println("Err: 打开文件失败");
                return false;
            }

        }

        return true;
    }

    /** find specified rule **/
    public Rule findRule(String tableName, String columnName){
        for(Rule rule: allRules){
            if(tableName.equals(rule.getTableName()) && columnName.equals(rule.getColumnName())){
                return rule;
            }
        }
        return null;
    }

    /** Get final result after applying rules **/
    public ArrayList<HashMap<String, Object>> applyAllTheRules(ResultSet resultSet){

        return null;
    }
}
