package hw3;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class flushAll {
    private static Jedis jedis1, jedis2;
    public static void main(String[] args) {
        try {
            jedis1 = new Jedis("52.38.2.0", 6379);
            jedis2 = new Jedis("54.189.104.208", 6379);
        } catch (Exception e) {
            e.printStackTrace();
        }
        jedis1.flushAll();
        jedis2.flushAll();
    }
}
