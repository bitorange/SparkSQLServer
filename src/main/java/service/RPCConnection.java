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
    private final String conHost="10.4.21.103";
    private final String conUsername="wucheng";
    private final String conPassword="123456";

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
                response="{\"code\":\"1\",\"msg\":\" Error: RPC服务器出现异常\"}";
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
}
