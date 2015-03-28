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
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.ResultSet;

@Path("/service")
public class JerseyServer {

    RPCConnection rpcCon=null;

    /**
     * 接受传入的SQL语句，然后执行SQL语句，return结果
     * @param     sqlstring  传入的SQL语句
     * @return    返回SQL语句查询的结果
     */
    @GET
    @Path("/{sql}")
    public Response helloWorld(@PathParam("sql") String sqlstring) {
        try {
            sqlstring=URLDecoder.decode(sqlstring,"utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        System.out.println(sqlstring);

        String response=null;
        try {
            if (rpcCon == null) {
                rpcCon = new RPCConnection();
            }
            sqlstring="{\"service\":\"sqlExecute\",\"sql\":\""+sqlstring+"\"}";
            response = rpcCon.call(sqlstring);

        }catch (Exception e){
            System.out.println(e.getMessage());
            response="{\"code\":\"1\",\"msg\":\"服务器内部出错\"}";
        }
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
    public Response check(@QueryParam("name") String name,
                          @DefaultValue("26") @QueryParam("password") String password) {
        //获取name和password
        String checkString="{\"name\":\""+name+"\", \"password\":\""+password+"\", \"service\":\"check\"}";//密码校验
        String response = null;
        try {
            if (rpcCon == null) {
                rpcCon = new RPCConnection();
            }

            System.out.println(checkString);
            // 调用插入队列的函数
            response = rpcCon.call(checkString);
        }catch (Exception e){
            System.out.println(e.getMessage());
            response="{\"code\":\"1\",\"msg\":\"服务器内部出错\"}";
        }
        // result属于json格式
        return Response.status(200).entity(response).build();
    }
}
