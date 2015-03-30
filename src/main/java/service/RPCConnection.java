package service; /**
 * Created by orange on 2015/3/27.
 */

import com.rabbitmq.client.*;
//import dnl.utils.text.table.TextTable;
//import net.sf.json.JSONArray;
//import net.sf.json.JSONObject;

//import org.json.JSONArray;
//import org.json.JSONException;
//import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RPCConnection {

    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";
    private String replyQueueName;
    private QueueingConsumer consumer;
    private final String conHost="localhost";
    private final String conUsername="guest";
    private final String conPassword="guest";

    public RPCConnection() throws Exception {

        // 初始化连接
        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost(conHost);
        factory.setUsername(conUsername);
        factory.setPassword(conPassword);

        connection = factory.newConnection();
        channel = connection.createChannel();
        replyQueueName = channel.queueDeclare().getQueue();
        consumer = new QueueingConsumer(channel);
        channel.basicConsume(replyQueueName, true, consumer);
    }

    /**
     * 发送消息至requestQueueName队列中，并监听replyQueueName队列，取出replyQueueName队列中的消息
     * @param message 客户端向服务器端请求的消息
     * @return 从replyQueueName队列中得到的消息
     * @throws Exception
     */
    public String call(String message) throws Exception {
        String response = null;
        String corrId = UUID.randomUUID().toString();

        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        //向服务器端队列发送消息
        channel.basicPublish("", requestQueueName, props, message.getBytes());

        //客户端对RPC的超时处理
        while (true) {
            // 阻塞监听replyQueueName队列,3s之后无响应即超时
            QueueingConsumer.Delivery delivery = consumer.nextDelivery(3000);

            if(delivery==null){  //3秒之后无返回
                response="{\"msg\":\" Error: 服务器连接失败\"}";
                break;
            }
            else if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response = new String(delivery.getBody(),"UTF-8");
                break;
            }
        }
        return response;
    }

    /**
     * 关闭channel和connection连接
     * @throws Exception
     */
    public void close() throws Exception {
        channel.close();
        connection.close();
    }

    /**
     * 将获得的json数据进行解析，并显示在界面上
     * @param     json  json字符串
     */
    /*
    public void jsonparser(String json){
        String code,msg,time,size;
        JSONObject jsonObj;
        try {
            // 获取整个json字符串对象
            jsonObj = JSONObject.fromObject(json);

            code = jsonObj.getString("code");
            msg = jsonObj.getString("msg");
            time = jsonObj.getString("time");
            size = jsonObj.getString("size");
        }catch (Exception e)
        {
            System.out.println("Error: 从服务端获取数据出错");
            return;
        }

        if(Integer.valueOf(code) == 0) {
            // 获取字符串对象中的result数组
            JSONArray jsonArray = jsonObj.getJSONArray("result");
            if (null != jsonArray && jsonArray.size() > 0) {
                List<Map<String, Object>> mapListJson = (List) jsonArray;

                // 得到表头
                ArrayList<String> headerList = new ArrayList<String>();
                Map<String, Object> obj1 = mapListJson.get(0);

                // TODO: 需要优化
                for (Map.Entry<String, Object> entry : obj1.entrySet()) {
                    String strkey1 = entry.getKey();    // 表头
                    headerList.add(strkey1);
                }
                // 表头数组
                String[] header = new String[headerList.size()];
                header = headerList.toArray(header);


                // 得到表中数据内容存在二维数组中
                String[][] data = new String[mapListJson.size()][headerList.size()];
                int j;
                for (int i = 0; i < mapListJson.size(); i++) {
                    Map<String, Object> obj = mapListJson.get(i);
                    j = 0;
                    for (Map.Entry<String, Object> entry : obj.entrySet()) {
                        Object strval1 = entry.getValue();
                        data[i][j++] = strval1.toString();
                    }
                }

                // 生成TextTable
                TextTable tt = new TextTable(header, data);

                // Add the numbering on the left
                tt.setAddRowNumbering(true);

                // Print table
                tt.printTable();
                System.out.println();
                System.out.println("SQL Execution Time: " + time + "  HDFS R&W Size: " + size);
            }
        }
        else{
            if(msg.contains("SocketException")) {
                System.out.println("Error: 与服务器连接失效，请重新连接");
            }else {
                System.out.println("Error: " + msg);
            }
        }
    }*/
}
