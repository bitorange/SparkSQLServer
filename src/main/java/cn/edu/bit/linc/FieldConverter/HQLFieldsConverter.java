package cn.edu.bit.linc.FieldConverter;

import cn.edu.bit.linc.ConnectJDBC;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by ihainan on 3/20/15.
 */
public class HQLFieldsConverter {
    private static HashMap<Integer, String> typeNames;  // A hashmap used to store all the possible types of AST Nodes.
    static {
        try {
            typeNames = getAllASNodeType();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /** Get all the possible types of AST nodes **/
    private static
    HashMap<Integer, String> getAllASNodeType() throws ClassNotFoundException, IllegalAccessException {
        HashMap<Integer, String> typeNames = new HashMap<Integer, String>();
        for (Field field:Class.forName("org.apache.hadoop.hive.ql.parse.HiveParser").getFields()){
            if(Modifier.isFinal(field.getModifiers()) && field.getType() == int.class){
                typeNames.put(field.getInt(null), field.getName());
            }
        }
        return typeNames;
    }

    private static final int MAX_DEPTH = 100;
    /** Print an AST tree **/
    private static void printASTree(ASTNode tree, int depth){
        if(depth > MAX_DEPTH)
            return;
        for(Node n: tree.getChildren()){
            ASTNode asn = (ASTNode) n;
            for(int i = 0; i <= depth - 1; ++i)
                System.out.print("\t");
            System.out.print(getASTNodeType(asn.getToken().getType()));
            // if(asn.toString().equals(getASTNodeType(asn.getToken().getType()))) {
            System.out.print("  " + asn.toString());
            // }
            System.out.println();
            if(asn.getChildCount() > 0) {
                printASTree(asn, depth + 1);
            }
        }
    }

    /** Get the type of an AST Node **/
    private static String getASTNodeType(int typeInt){
        if(typeNames.containsKey(typeInt)){
            return typeNames.get(typeInt);
        }
        else{
            return "OTHER";
        }
    }

    // Rules
    private Rules rules = new Rules();  // Read rules from local file

    // An array list used to store all the dependence of tables.
    private HashMap<String, ArrayList<DependentTable>> dependenceOfTables = new HashMap<String, ArrayList<DependentTable>>();
    private ArrayList<String> allFinalSelectFields;

    /**
     * Analyse QUERY clause
     * Register the DEPENDENCY table and return the TABLEINFO
     **/
    private TableInfo queryAnalyse(ASTNode node, Boolean isRoot){
        /* 类型检查 */
        if(node.getToken().getType() != HiveParser.TOK_QUERY) {
            System.err.println("Err: 解析节点不是 TOK_QUERY 类型");
            return null;
        }

        /* 获取表名和别名 */
        String tableName, alias;
        if(isRoot){
            tableName = "FINAL_TABLE";
            alias = "FINAL_TABLE";
        }
        else{
            // 子查询，只有别名
            tableName = null;
            ASTNode idNode = getChild((ASTNode)node.getParent(), "Identifier");

            alias = idNode.toString();
        }

        /* FROM CLAUSE */
        ASTNode fromNode = getChild(node, "TOK_FROM");
        ArrayList<TableInfo> fromTableInfos = new ArrayList<TableInfo>();  // 存储 FROM 从句下所有表的信息

        // 获取得到所有子表的信息表
        if(fromNode != null) {
            // TODO: 多种 Join 的支持
            // 单表（单子查询）情况
            if(getChild(fromNode, "TOK_JOIN") == null){
                if(getChild(fromNode, "TOK_TABREF") != null) {
                    TableInfo tableInfo = getTableInfo(getChild(fromNode, "TOK_TABREF"));
                    fromTableInfos.add(tableInfo);  // 表
                }
                else if(getChild(fromNode, "TOK_SUBQUERY") != null){
                    ASTNode queryChildNode = getChild(getChild(fromNode, "TOK_SUBQUERY"), "TOK_QUERY");
                    TableInfo tableInfo = queryAnalyse(queryChildNode, false);
                    fromTableInfos.add(tableInfo);
                }
            }
            else {
                // 多表情况，通过 TOK_JOIN 嵌套
                ASTNode joinNode = getChild(fromNode, "TOK_JOIN");
                while(joinNode != null){
                    // 第二个子节点，只可能是子查询或者表
                    ASTNode child1 = (ASTNode)joinNode.getChild(1);
                    // 表
                    if(child1.getToken().getType() == HiveParser.TOK_TABREF){
                        TableInfo tableInfo = getTableInfo(child1);
                        fromTableInfos.add(tableInfo);
                    }
                    // 子查询，递归
                    else if(child1.getToken().getType() == HiveParser.TOK_SUBQUERY){
                        TableInfo tableInfo = queryAnalyse(getChild(child1, "TOK_QUERY"), false);
                        fromTableInfos.add(tableInfo);
                    }
                    else{
                        System.err.println("Error: Join 节点下第二个子节点不为子查询或者表");
                    }

                    // 第一个子节点，可能是Join，子查询或者表
                    ASTNode child0 = (ASTNode)joinNode.getChild(0);
                    if(child0.getToken().getType() == HiveParser.TOK_JOIN){
                        joinNode = child0;
                    }
                    else{
                        if(child0.getToken().getType() == HiveParser.TOK_TABREF){
                            TableInfo tableInfo = getTableInfo(child0);
                            fromTableInfos.add(tableInfo);
                        }
                        else if(child0.getToken().getType() == HiveParser.TOK_SUBQUERY){
                            TableInfo tableInfo = queryAnalyse(getChild(child0, "TOK_QUERY"), false);
                            fromTableInfos.add(tableInfo);
                        }
                        else{
                            System.err.println("Error: Join 节点下第一个子节点不为 Join、子查询或者表");
                            return null;
                        }
                        joinNode = null;
                    }
                }
            }
        }
        else{
            System.err.println("Err: FROM 子节点不存在");
        }

        /* INSERT CLAUSE */
        HashMap<String, ArrayList<String>> selectTableAndFields = new HashMap<String, ArrayList<String>>();   // 所有 SELECT 的字段，Key 为表名（或别名）
        ArrayList<String> allSelectFields = new ArrayList<String>();  // 所有 SELECT 字段的汇总
        String insertTable = null;

        ASTNode insertNode = getChild(node, "TOK_INSERT");
        if(insertNode != null){
            // SELECT CLAUSE
            ASTNode selectNode = getChild(insertNode, "TOK_SELECT");
            if(selectNode == null){
                System.err.println("Err: SELECT 子节点不存在");
                return null;
            }

            // 获取得到新表的所有字段，暂时不考虑函数, 表达式以及 *
            for(Node prNode: selectNode.getChildren()){
                String tmpTableName = null, tmpField = null;
                ASTNode prChild = (ASTNode)((ASTNode) prNode).getChild(0);

                // * 形式
                if(prChild.getToken().getType() == HiveParser.TOK_ALLCOLREF){
                    // 表名
                    TableInfo onlyTableInfo = fromTableInfos.get(0);
                    if((onlyTableInfo.tableName != null)){
                        tmpTableName = onlyTableInfo.tableName;
                    }
                    else{
                        tmpTableName = onlyTableInfo.aliasName;
                    }
                    ArrayList<String> tmpFields = getFieldOfTable(tmpTableName);
                    allSelectFields = tmpFields;

                    for(String f: allSelectFields){
                        if (tmpTableName == null) {
                            System.err.println("Error: 在依赖表中未能搜索到对应字段 " + f);
                            return null;
                        } else if (selectTableAndFields.containsKey(tmpTableName)) {
                            ArrayList<String> tmpList = selectTableAndFields.get(tmpTableName);
                            tmpList.add(f);
                            selectTableAndFields.put(tmpTableName, tmpList);
                        } else {
                            ArrayList<String> tmpList = new ArrayList<String>();
                            tmpList.add(f);
                            selectTableAndFields.put(tmpTableName, tmpList);
                        }
                    }
                    break;
                }
                else {
                    // Table.Field 形式
                    if (prChild.getToken().getType() == HiveParser.DOT) {
                        // 表名（或者别名）
                        tmpTableName = getChild(getChild(prChild, "TOK_TABLE_OR_COL"), "Identifier").toString();

                        // 字段名
                        tmpField = getChild(prChild, "Identifier").toString();
                    }
                    // Field 形式
                    else {
                        // 字段名
                        tmpField = getChild(getChild(prChild, "TOK_TABLE_OR_COL"), "Identifier").toString();

                        // 从所有依赖表中找出含有该字段的表的表名
                        for (TableInfo t : fromTableInfos) {
                            if (t.fields.contains(tmpField)) {
                                tmpTableName = t.tableName;
                                break;
                            }
                        }
                    }

                    allSelectFields.add(tmpField);

                    if (tmpTableName == null) {
                        System.err.println("Error: 在依赖表中未能搜索到对应字段 " + tmpField);
                        return null;
                    } else if (selectTableAndFields.containsKey(tmpTableName)) {
                        ArrayList<String> tmpList = selectTableAndFields.get(tmpTableName);
                        tmpList.add(tmpField);
                        selectTableAndFields.put(tmpTableName, tmpList);
                    } else {
                        ArrayList<String> tmpList = new ArrayList<String>();
                        tmpList.add(tmpField);
                        selectTableAndFields.put(tmpTableName, tmpList);
                    }
                }
            }

            // INSERT CLAUSE
            ASTNode insertIntoNode = getChild(insertNode, "TOK_INSERT_INTO");
            if(insertIntoNode != null){
                ASTNode tabNode = getChild(insertIntoNode, "TOK_TAB");
                ASTNode tabNamenode = getChild(tabNode, "TOK_TABNAME");
                ASTNode identifierNode = getChild(tabNamenode, "Identifier");
                insertTable = identifierNode.toString();
            }

            ASTNode destinationNode = getChild(insertNode, "TOK_DESTINATION");
            if(destinationNode != null){
                ASTNode tabNode = getChild(destinationNode, "TOK_TAB");
                if(tabNode != null) {
                    ASTNode tabNamenode = getChild(tabNode, "TOK_TABNAME");
                    ASTNode identifierNode = getChild(tabNamenode, "Identifier");
                    insertTable = identifierNode.toString();
                }
            }

        }
        else{
            System.err.println("Error: INSERT 子节点不存在");
        }

        /* 构建信息表 */
        TableInfo currentTableInfo = new TableInfo();
        currentTableInfo.tableName = tableName;
        currentTableInfo.aliasName = alias;
        currentTableInfo.fields = allSelectFields;

        /* 根据 selectTableAndFields 和 fromTableInfos，登记到全局依赖表 */
        ArrayList<DependentTable> dependentTables = getDependentTables(selectTableAndFields, fromTableInfos);
        dependenceOfTables.put(tableName + ", " + alias, dependentTables);
        if(isRoot){
            allFinalSelectFields = allSelectFields;
        }

        /* 规则继承 */
        if(insertTable != null){
            ArrayList<String> fields = getFieldOfTable(insertTable);
            for(int i = 0; i < fields.size(); ++i){
                String selectField = allSelectFields.get(i);
                String insertField = fields.get(i);

                // TODO: 考虑多表情况
                // 检查 FinalTable - Filed 是否出现在规则中
                ArrayList<DependentTable> tables = getOriginalTables(selectField);
                DependentTable t = tables.get(0);
                Rule rule = rules.findRule(t.tableName, selectField);
                if(rule != null){
                    Rule newRule = new Rule(insertTable, insertField, rule.getRex(), rule.getReplaceContent());
                    rules.addNewRule(newRule);
                }
            }
        }

        return currentTableInfo;
    }

    /** Get the first specified child node by the ASTNODE TYPE name **/
    private static ASTNode getChild(ASTNode node, String filedName){
        if(node != null && node.getChildCount() > 0) {
            for (Node child : node.getChildren()) {
                ASTNode childNode = (ASTNode) child;
                if (getASTNodeType(childNode.getType()).equals(filedName))
                    return childNode;
            }
        }
        return null;
    }

    /** Get tableInfo **/
    private static TableInfo getTableInfo(ASTNode tableNode){
        TableInfo tableInfo = new TableInfo();

        // 获取表名
        ASTNode tabNameNode = getChild(tableNode, "TOK_TABNAME");
        tableInfo.tableName = getChild(tabNameNode, "Identifier").toString();

        // 获取别名
        if(getChild(tableNode, "Identifier") != null) {
            tableInfo.aliasName = getChild(tableNode, "Identifier").toString();
        }

        // 获取表中字段
        tableInfo.fields = getFieldOfTable(tableInfo.tableName);

        return tableInfo;
    }

    /** Get all the filed of a table from database **/
    private static ArrayList<String> getFieldOfTable(String tableName){
        ArrayList<String> fields = new ArrayList<String>();

        // Connect to Database
        ConnectJDBC connectJDBC = new ConnectJDBC();
        String command = "select * from " + tableName;
        ResultSet resultSet = connectJDBC.getAndExucuteSQL(command);
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            for(int i = 1; i <= metaData.getColumnCount(); ++i){
                String columnName = metaData.getColumnName(i);
                fields.add(columnName);
            }
        } catch (SQLException e) {
            System.err.println("Err: 获取 metaData 失败");
            return null;
        }

        return fields;
    }

    /** Get dependent tables **/
    private static ArrayList<DependentTable> getDependentTables(HashMap<String, ArrayList<String>> selectTableAndFields,  ArrayList<TableInfo> fromTableInfos){
        ArrayList<DependentTable> currentDepentTable = new ArrayList<DependentTable>();

        for(String tableName: selectTableAndFields.keySet()){
            TableInfo rightTable = null;
            // 查看表名是否为某个表的表名或者别名
            for(TableInfo info: fromTableInfos){
                if(tableName.equals(info.tableName) || tableName.equals(info.aliasName)){
                    rightTable = info;
                    break;
                }
            }
            if(rightTable == null) {
                System.err.println("Error: " + tableName + " doesn't exist in TableInfo list");
            }
            else{
                DependentTable dependentTable = new DependentTable();
                dependentTable.tableName = rightTable.tableName;
                dependentTable.alias = rightTable.aliasName;
                dependentTable.fields = selectTableAndFields.get(tableName);
                currentDepentTable.add(dependentTable);
            }
        }
        return currentDepentTable;
    }

    /** Get original tables where a filed from **/
    private ArrayList<DependentTable> getOriginalTables(String field){
        ArrayList<DependentTable> originalTables = new ArrayList<DependentTable>();

        // 目前仅考虑 Filed 不包含表达式、函数的情况，但留扩展接口
        String tableName = "FINAL_TABLE";
        String alias = "FINAL_TABLE";
        DependentTable table = null;
        // TODO: 考虑字段为表达式（或函数）情况
        while(dependenceOfTables.containsKey(tableName + ", " + alias)){
            // 如果 Field 为表达式，此处可以得到多张 table
            table = findTable(field, tableName, alias);
            if(table == null){
                break;
            }

            // TODO: 考虑字段别名
            field = field;
            tableName = table.tableName;
            alias = table.alias;
        }

        if(table == null){
            System.err.println("Err: 找不到依赖表");
            return null;
        }
        else{
            originalTables.add(table);
        }

        return originalTables;
    }

    /** find a table that a filed dependent on **/
    private DependentTable findTable(String field, String tableName, String alias){
        for(DependentTable table: dependenceOfTables.get(tableName + ", " + alias)){
            if(table.fields.contains(field)) {
                return table;
            }
        }
        return null;
    }

    /** Print dependenceOfTables **/
    private void printDOT(){
        System.out.println("\n------------------ The Result of dependenceOfTables ------------------");
        for(String key: dependenceOfTables.keySet()) {
            System.out.println("Table Name / Alias : " + key + " : ");
            for(DependentTable dt: dependenceOfTables.get(key)){
                System.out.print("From Table Name / Alias : " + dt.tableName + ", " + dt.alias + "\n" + "Select: ");
                for(String field: dt.fields){
                    System.out.print(field + " ");
                }
                System.out.println();
            }
        }
    }

    /** Parse Command **/
    public ArrayList<HashMap<String, String>> parseCommand(String command, ResultSet resultSet){
        ArrayList<HashMap<String, String>> finalResult = new ArrayList<HashMap<String, String>>();
        ParseDriver pd = new ParseDriver();
        try {
            /** Start analysing AS tree **/
            System.out.println("\n------------------ Parsing SQL Command ------------------");
            ASTNode tree = pd.parse(command);

            /** Print the AS tree **/
            System.out.println("\n------------------ AS Tree ------------------");
            printASTree(tree, 0);

            // ONLY FOR TOK_QUERY!
            if(getChild(tree, "TOK_QUERY") != null) {
                /** Analyse Query ASTNode **/
                queryAnalyse((ASTNode) tree.getChild(0), true);

                /** Print dependenceOfTables **/
                // printDOT();

                /** Get final result after converted **/
                if (resultSet != null) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    while (resultSet.next()) {
                        HashMap<String, String> resultOfOneRow = new HashMap<String, String>();
                        // Convert all the columns of one row
                        for (int i = 1; i <= allFinalSelectFields.size(); ++i) {
                            String value = resultSet.getString(i);
                            String field = allFinalSelectFields.get(i - 1);
                            if (value == null)
                                value = "";

                            // TODO: 多张表的情况的处理
                            ArrayList<DependentTable> tables = getOriginalTables(field);
                            DependentTable t = tables.get(0);

                            String newContent = rules.applyRules(t.tableName, field, value);
                            resultOfOneRow.put(metaData.getColumnName(i), newContent);
                        }
                        finalResult.add(resultOfOneRow);
                    }
                } else {
                    return finalResult;
                }
            }
            else{
                if (resultSet != null) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    while (resultSet.next()) {
                        HashMap<String, String> resultOfOneRow = new HashMap<String, String>();
                        for(int i = 1; i <= metaData.getColumnCount(); ++i){
                            resultOfOneRow.put(metaData.getColumnName(i), resultSet.getString(i));
                        }
                        finalResult.add(resultOfOneRow);
                    }
                }
                else{
                    return finalResult;
                }
            }

        } catch (ParseException e) {
            System.err.println("Err: parse SQL command error");
            return null;
        } catch (SQLException e) {
            System.err.println("Err: Result Set get next result error");
            return null;
        }
        return finalResult;
    }
}