package cn.edu.bit.linc;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import cn.edu.bit.linc.FieldConverter.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * this class is used to connect the jdbc and execute SQL command.
 * @author  xwc
 * @version v1
 * Created by admin on 2015/3/16.
 */
public class ConnectJDBC {
    private String msg;
    private int code;

    /**
     * the method include register driver,get connection and execute sql, then return resultset
     *@param sql
     *@return resultset the Resultset of execute sql command
     **/
    public ResultSet getAndExucuteSQL(String sql){
        Connection conn;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            // 注册获得连接 getConnection
            conn = JDBCUtils.getConnection();
            stmt = conn.createStatement();
        } catch (SQLException e) {
            code = 1;
            msg = "There are some problems with the driver.";
        }

        // 执行 SQL，获取结果集
        try {
            rs = stmt.executeQuery(sql);
        } catch (SQLException e) {
            // 只取错误信息的后面一段,把冒号分割的第一段省略
            msg = "";
            String msgArray[] = e.getMessage().split(":");
            for(int i = 1; i <= msgArray.length - 1; i++){
                msg += msgArray[i];
            }
            code = 1 ;
        }finally {
            JDBCUtils.releaseAll();
        }
        return rs;
    }


    /**
     * this method transform the resultset into json object ,and return a jsonArray.
     * @param rs  the resultset
     * @return JSONArray
     * */
    public  JSONObject transformToJsonArray(ResultSet rs) throws JSONException {
        // 建立 jsonArray 数组封装所有的 ResultSet 信息
        JSONArray array = new JSONArray();

        JSONObject wholeJsonObj = null;
        ResultSetMetaData metaData = null;
        if(rs !=null) {
            try {
                // 定义列数
                int columnCount = 0;

                // 获取列数
                metaData = rs.getMetaData();
                columnCount = metaData.getColumnCount();

                // 遍历每条数据
                while (rs.next()) {
                    // 创建 json 存放 ResultSet
                    JSONObject jsonObj = new JSONObject();

                    // 遍历每一列
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnLabel(i);
                        // 获得 columnName 对应的值
                        String value = rs.getString(columnName);
                        jsonObj.put(columnName, value);
                    }
                    array.put(jsonObj);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        // wholeJsonObj 用于存放最终返回数据
        wholeJsonObj = new  JSONObject();

        // code为 0 则正常输出，为 1 则异常输出
        if(code == 1) {
            wholeJsonObj.put("code", code).put("msg", msg);
        }
        else {
            msg = "success";
        }

        // 将 result, time, code, msg, size 数据封装进 wholeArray
        wholeJsonObj.put("result", array).put("time", "N/Aih").put("size", "N/A").put("code", code).put("msg", msg);
        return wholeJsonObj;
    }


    public JSONObject convertArrayListToJsonOjbect(ArrayList<HashMap<String, String>> resultList){
        JSONArray jsonArray = new JSONArray();
        for(HashMap<String, String> dataRow: resultList){
            JSONObject jsonObj = new JSONObject();
            for(String columnName: dataRow.keySet()){
                String value = (dataRow.get(columnName) == null ? "" : dataRow.get(columnName)).toString();
                try {
                    jsonObj.put(columnName, value);
                } catch (JSONException e) {
                    System.err.println("Err: 字段转换成 JSON 数据失败");
                    return null;
                }
            }
            jsonArray.put(jsonObj);
        }

        // Add field into JSON data
        JSONObject finalJsonObject = new JSONObject();
        if(code == 1) {
            try {
                finalJsonObject.put("code", code).put("msg", msg);
            } catch (JSONException e) {
                System.err.println("Err: 字段转换成 JSON 数据失败");
                return null;
            }
        }
        else {
            msg = "success";
        }
        try {
            finalJsonObject.put("result", jsonArray).put("time", "N/A").put("size", "N/A").put("code", code).put("msg", msg);
        } catch (JSONException e) {
            System.err.println("Err: 构造 JSONObject 失败");
            return null;
        }
        return finalJsonObject;
    }
}
