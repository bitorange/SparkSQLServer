package service;

/**
 * Created by orange on 2015/3/17.
 */
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import cn.edu.bit.linc.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;

@Path("/service")
public class JerseyServer {

    /**
     * 接受传入的SQL语句，然后执行SQL语句，return结果
     * @param     sqlstring  传入的SQL语句
     * @return    返回SQL语句查询的结果
     */
    @GET
    @Path("/{sql}")
    public Response helloWorld(@PathParam("sql") String sqlstring) {


        String response=null;
        String  newSqlString = null;
        try {
            newSqlString = java.net.URLDecoder.decode(sqlstring,   "utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        // String  newSqlString = sqlstring.replace("+"," ");
        // System.out.println(newSqlString);
        ConnectJDBC conn = new ConnectJDBC();
        ResultSet rs=null;
        try {
            rs = conn.getAndExucuteSQL(newSqlString);
        }catch (Exception e){

            return Response.status(200).entity(e.getMessage().toString()).build();
        }


        JSONObject jsonObject;
        try {
            jsonObject = conn.transformToJsonArray(rs);
            response=jsonObject.toString();
        } catch (JSONException e) {
            e.printStackTrace();
        }finally{
            JDBCUtils.releaseAll();
        }

        //将结果转换成json数据结构返回
        return Response.status(200).entity(response).build();
    }


    /**
     * 校验用户名与密码，并返回结果
     * @param     password  即用户名
     * @param     name  即密码
     * @return    用户名与密码的验证结果
     */
    @GET
    @Path("/check")
    @Produces("application/json")
    public String check(@QueryParam("name") String name,
                        @DefaultValue("26") @QueryParam("password") String password) {
        System.out.println(name);
        //获取name和password，进行校验

        String result=AccountCheck.checkAccount(name,password);

        //返回结果
        if(result.equals("登入成功"))
            return "ok";
        else
            return "no";
    }
}
