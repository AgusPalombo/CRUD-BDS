import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;


public class practicaRedisDocu {
    public static void main(String[] args) {
        JedisPool pool = new JedisPool("localhost", 6379);

        Jedis jedis = pool.getResource();
        
        insertarDatos(jedis);
        leerDatos(jedis);
        updateDatos(jedis);
        deleteDatos(jedis);
        
        jedis.close();
    }
    
    
    private static void deleteDatos(Jedis jedis) {
    	System.out.println("Persona:1 eliminada: ");    	
		jedis.del("persona:1");
		System.out.println(jedis.hgetAll("persona:1"));
        System.out.println(jedis.hgetAll("persona:2"));
        System.out.println(jedis.hgetAll("persona:3"));   
    }


	private static void updateDatos(Jedis jedis) {
    	jedis.hset("persona:1", "apellido", "Perez");
    	
    	System.out.println("Nuervos datos: ");
    	System.out.println(jedis.hgetAll("persona:1"));
        System.out.println(jedis.hgetAll("persona:2"));
        System.out.println(jedis.hgetAll("persona:3"));    	
	}


	private static void leerDatos(Jedis jedis) {
		//leer aquel cuyo nombre se agustin
		System.out.println("Agustin: ");
    	System.out.println(jedis.hgetAll("persona:2"));
    	
    	
	}


	public static void insertarDatos(Jedis jedis) {
    	
		System.out.println("Datos insertados: ");
        
        jedis.hset("persona:1", "nombre", "John");
        jedis.hset("persona:1", "apellido", "Doe");
        jedis.hset("persona:1","edad", "29");
        
        jedis.hset("persona:2", "nombre", "Agustin");
        jedis.hset("persona:2", "apellido", "Palombo");
        jedis.hset("persona:2","edad", "29");
		
        jedis.hset("persona:3", "nombre", "Mario");
        jedis.hset("persona:3", "apellido", "Pergolini");
        jedis.hset("persona:3","edad", "54");
		
        
	
        System.out.println(jedis.hgetAll("persona:1"));
        System.out.println(jedis.hgetAll("persona:2"));
        System.out.println(jedis.hgetAll("persona:3"));
    }
    
    
}

