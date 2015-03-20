package cn.edu.bit.linc;

import org.apache.avro.data.Json;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.omg.CosNaming.NamingContextExtPackage.StringNameHelper;

import java.sql.*;

/**
 * this class is used to connect the jdbc and execute SQL command.
 * @author  xwc
 * @version v1
 * Created by admin on 2015/3/16.
 */
public class ConnectJDBC {

    static Connection conn=null;
    static  Statement stmt = null;
    static ResultSet rs=null;

    /**
     * the method include register driver,get connection and execute sql, then return resultset
     * @param sql
     *@return resultset the Resultset of execute sql command
     * */
    public static ResultSet getAndExucuteSQL(String sql){
        //注册获得连接getconnection

        try {
            conn = JDBCUtils.getConnection();
            stmt= conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //执行sql，获取结果集
        try {
            rs = stmt.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("some problem the executeQuery");
        }
        return rs;
    }


    /**
     * this method transform the resultset into json object ,and return a jsonArray.
     * @param rs  the resultset
     * @return JSONArray
     * */
    public static JSONObject transformToJsonObject(ResultSet rs) throws JSONException {

        //建立jsonArray数组封装所有的resultset信息
        JSONArray array = new JSONArray();
      /*  //wholeArray封装所有包含时间，大小，返回码，返回信息的Json
        JSONArray wholeArray = null;*/
        JSONObject wholeJsonObj = null;
        ResultSetMetaData metaData = null;

        //定义列数
        int columnCount = 0;
        //获取列数
        try {
            metaData = rs.getMetaData();
            columnCount = metaData.getColumnCount();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //遍历每条数据
        try {
            while (rs.next()) {
                //创建json存放resultset
                JSONObject jsonObj = new JSONObject();
                // 遍历每一列
                for (int i = 1; i <= columnCount; i++) {
                    String columnName =metaData.getColumnLabel(i);
                    //获得columnName对应的值
                    String value = rs.getString(columnName);
                    jsonObj.put(columnName, value);
                }


                array.put(jsonObj);
                //存放时间，返回代码，信息和吞吐大小
            }
            //wholeJsonObj封装所有result中的子json数据
            wholeJsonObj = new JSONObject();
            wholeJsonObj.put("result",array);
            //这里将time,code,msg,size数据封装进wholeArray
            wholeJsonObj.put("time","20s").put("size","2M").put("code",100010).put("msg","success");

        }catch(SQLException e){
            e.printStackTrace();
        }finally {
            JDBCUtils.releaseAll();
        }
        return wholeJsonObj;
    }
}
