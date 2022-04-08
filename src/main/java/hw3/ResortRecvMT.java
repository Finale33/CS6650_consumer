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

public class ResortRecvMT {

    private final static String QUEUE_NAME = "myQueue";
    private final static int THREAD_NUM = 200;
    private static JedisPool pool;

    private static String getRange(String timeStr) {
        int time = Integer.parseInt(timeStr);
        return String.valueOf(time / 60);
    }

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("18.237.150.254");
        factory.setUsername("radmin");
        factory.setPassword("radmin");
        final Connection connection = factory.newConnection();
        try {
            pool = new JedisPool("54.189.104.208", 6379);
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
                    // we save as 3 kinds of key-value pairs
                    // first one is {"${dayId}/ resortId:${resortId}", set of skierId}
                    // second one is {"${dayId}/ liftId: ${liftId}", "${count}"}
                    // third one is {${dayId}/ range: ${hourRange}, "${liftRide counts}"}
                    try (Jedis jedis = pool.getResource()) {
                        String[] messages = message.split("}");
                        String[] params = messages[1].split(",");
                        String skierId = params[0];
                        String day = params[1];
                        String resortId = params[3];
                        String[] queries = messages[0].split(",");
                        String liftId = queries[1].split(":")[1];
                        String time = queries[0].split(":")[1];

                        // first pair
                        jedis.sadd(day + "/ resortId: " + resortId, skierId);

                        // second pair
                        if (jedis.exists(day + "/ liftId: " + liftId)) {
                            String count = jedis.get(day + "/ liftId: " + liftId);
                            int curCount = Integer.parseInt(count) + 1;
                            jedis.set(day + "/ liftId: " + liftId, String.valueOf(curCount));
                        } else {
                            jedis.set(day + "/ liftId: " + liftId, "1");
                        }

                        // third pair
                        String range = getRange(time);
                        if (jedis.exists(day + "/ range: " + range)) {
                            String count = jedis.get(day + "/ range: " + range);
                            int curCount = Integer.parseInt(count) + 1;
                            jedis.set(day + "/ range: " + range, String.valueOf(curCount));
                        } else {
                            jedis.set(day + "/ range: " + range, "1");
                        }

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