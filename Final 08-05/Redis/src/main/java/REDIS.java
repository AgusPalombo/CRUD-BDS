import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class REDIS {
	
	private static Jedis jedis;
	
	
	//CARGA DE CLIENTES
	public static void datosClientes() {
		cargarClientes("1", "Agustin Palombo", "A");
		cargarClientes("2", "Karina Brunacci", "B");
		cargarClientes("3", "Ricardo Palombo", "C");
	}
			
	public static void cargarClientes(String cuit, String nombre, String tipo) {
		 jedis.hset("cliente:" + cuit, "nombre", nombre);
		 jedis.hset("cliente:" + cuit, "tipo", tipo);
	}
	
	
	public static  void datosPedidos() {
		cargarPedido(1, "1", "Producto 1", "Producto 2");
        cargarPedido(2, "2", "Producto 2",  "Producto 3" );
        cargarPedido(3, "3", "Producto 1",  "Producto 3" );
        cargarPedido(4, "2", "Producto 1",  "Producto 3");
        cargarPedido(5, "1", "Producto 1",  "Producto 3");
	}
	
	
	private static void cargarPedido(int pedido, String cliente, String producto1, String producto2) {
		jedis.hset("pedido"+ pedido, "cliente", cliente);
		jedis.hset("pedido"+ pedido, "producto 1", producto1);
		jedis.hset("pedido"+ pedido, "producto 2", producto2);
	}

	
	private static void obtenerPedidos(String nroCliente) {
		 // Obtener todas las claves de pedidos que contienen el número de cliente
        Set<String> keysPedidos = jedis.keys("pedido:*:cliente:" + nroCliente);

        // Recorrer las claves de pedidos y mostrar sus valores
        for (String keyPedido : keysPedidos) {
            // Obtener el mapa de datos del pedido
            Map<String, String> datosPedido = jedis.hgetAll(keyPedido);

            // Mostrar los datos del pedido
            System.out.println("Pedido: " + keyPedido);
            for (Map.Entry<String, String> entry : datosPedido.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }
            System.out.println();
        
	    }
	}
	
	
	public static void main(String[] args) {
		
		jedis = new Jedis("localhost", 6379);
		
		datosClientes();
		datosPedidos();
		
		obtenerPedidos("1");
		
		jedis.close();
	}

}
