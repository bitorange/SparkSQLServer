package service;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;

/**
 * 本类用于建立到 RPCServer 端的连接
 */
public class RPCConnection {

    private Connection connection;
    private Channel channel;
    private String replyQueueName;
    private QueueingConsumer consumer;

    // TODO: 从配置文件中读取
    private String requestQueueName = "rpc_queue";
    private final String conHost = "localhost";
    private final String conUsername = "guest";
    private final String conPassword = "guest";

    public RPCConnection() throws Exception {
        // 初始化到 RPC 队列服务器的连接
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
     *
     * @param message 客户端向服务器端请求的消息
     * @return 从replyQueueName队列中得到的消息
     * @throws IOException          发送消息失败
     * @throws InterruptedException
     */
    public String call(String message) throws IOException, InterruptedException {
        String response;
        String corrId = UUID.randomUUID().toString();

        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        // 向服务器端队列发送消息
        channel.basicPublish("", requestQueueName, props, message.getBytes());

        // 客户端对RPC的超时处理
        while (true) {
            // 阻塞监听replyQueueName队列,3s之后无响应即超时
            QueueingConsumer.Delivery delivery = consumer.nextDelivery(3000);

            if (delivery == null) {  //3秒之后无返回
                response = "{\"code\":\"1\",\"msg\":\" Error: RPC服务器出现异常\"}";
                break;
            } else if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response = new String(delivery.getBody(), "UTF-8");
                break;
            }
        }
        return response;
    }

    /**
     * 关闭channel和connection连接
     *
     * @throws Exception 关闭连接失败
     */
    public void close() throws Exception {
        channel.close();
        connection.close();
    }
}
