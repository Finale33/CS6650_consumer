package hw3;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RecvMT3 {

    private final static String QUEUE_NAME = "myQueue";
    private final static int THREAD_NUM = 200;
    private static JedisPool pool;

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("18.237.150.254");
        factory.setUsername("radmin");
        factory.setPassword("radmin");
        final Connection connection = factory.newConnection();
        try {
            pool = new JedisPool("52.38.2.0", 6379);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Runnable runnable = () -> {
            try {
                final Channel channel = connection.createChannel();
                channel.queueDeclare(QUEUE_NAME, true, false, false, null);
                // max one message per receiver
                channel.basicQos(1);

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    // we save as 2 kinds of key-value pairs
                    // first one is {"${skierId}", "${number of days}"}
                    // second one is {"${skierId}/${dayId}", list of string saving vertical and lift info}
                    try (Jedis jedis = pool.getResource()) {
                        String[] messages = message.split("}");
                        String[] params = messages[1].split(",");
                        String skierId = params[0];
                        String day = params[1];
                        // if this dayId appears for the first time
                        if(!jedis.exists(skierId + "/" + day)) {
                            // if none, set to 1
                            if (jedis.exists(skierId)) {
                                String days = jedis.get(skierId);
                                int numOfDays = Integer.parseInt(days) + 1;
                                jedis.set(skierId, String.valueOf(numOfDays));
                            } else {
                                jedis.set(skierId, "1");
                            }
                        }
                        // push the string to the skier+day key
                        jedis.lpush(skierId + "/" + day, messages[0]);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println( "Callback thread ID = " + Thread.currentThread().getId() + " Received '" + message + "'");
                };
                channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });
            } catch (IOException ex) {
                Logger.getLogger(RecvMT3.class.getName()).log(Level.SEVERE, null, ex);
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
//        try (Jedis jedis = pool.getResource()) {
//            Set<String> keys = jedis.keys("*");
//            for (String key: keys) {
//                System.out.println(key);
//            }
//        }
    }
}