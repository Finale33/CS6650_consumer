import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RecvMT {

    private final static String QUEUE_NAME = "myQueue";
    private final static int THREAD_NUM = 200;
//    private static ConcurrentHashMap<String, List<String>> database = new ConcurrentHashMap<>();

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("18.237.150.254");
        factory.setUsername("radmin");
        factory.setPassword("radmin");
        final Connection connection = factory.newConnection();
        // the HashMap uses skierID as the key and list of records as the value
        ConcurrentHashMap<String, List<String>> database = new ConcurrentHashMap<>();

        Runnable runnable = () -> {
            try {
                final Channel channel = connection.createChannel();
                channel.queueDeclare(QUEUE_NAME, true, false, false, null);
                // max one message per receiver
                channel.basicQos(1);
//                System.out.println(" [*] Thread waiting for messages. To exit press CTRL+C");

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                    // process and save to the hashmap
                    String[] messages = message.split("}");
                    List<String> records = database.getOrDefault(messages[1], Collections.synchronizedList(new ArrayList<>()));
                    records.add(messages[0]);
                    database.put(messages[1], records);
                    System.out.println( "Callback thread ID = " + Thread.currentThread().getId() + " Received '" + message + "'");
                };
                channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });
            } catch (IOException ex) {
                Logger.getLogger(RecvMT.class.getName()).log(Level.SEVERE, null, ex);
            }
        };
        // start threads and block to receive messages
        Thread[] threadPool = new Thread[THREAD_NUM];
        for (int i = 0; i < THREAD_NUM; i++) {
            threadPool[i] = new Thread(runnable);
        }
        for (int i = 0; i < THREAD_NUM; i++) {
            threadPool[i].start();
        }
    }
}