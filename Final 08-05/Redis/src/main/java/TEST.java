import java.util.Set;

import redis.clients.jedis.*;
public class TEST {

	public static void main(String[] args) {
		 // Create a Jedis connection pool
		  JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost", 6379);

		  // Get the pool and use the database
		  try (Jedis jedis = jedisPool.getResource()) {

		  jedis.set("mykey", "Hello from Jedis");
		  String value = jedis.get("mykey");
		  System.out.println( value );

		  jedis.zadd("vehicles", 0, "car");
		  jedis.zadd("vehicles", 0, "bike");
		  Set<String> vehicles = jedis.zrange("vehicles", 0, -1);
		  System.out.println( vehicles );

		  }

		  // close the connection pool
		  jedisPool.close();

	}

}
