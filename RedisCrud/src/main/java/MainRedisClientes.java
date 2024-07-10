import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;

public class MainRedisClientes {

	public static void main(String[] args) {
        // Conectar a Redis
		
		        Jedis jedis = new Jedis("localhost", 6379);

		        // Crear Clientes
//		        Map<String, String> cliente1 = new HashMap<>();
//		        cliente1.put("nombre", "Juan Perez");
//		        cliente1.put("tipo", "A");
//		        jedis.hmset("cliente:20-12345678-1", cliente1);
//
//		        Map<String, String> cliente2 = new HashMap<>();
//		        cliente2.put("nombre", "Maria Gomez");
//		        cliente2.put("tipo", "B");
//		        jedis.hmset("cliente:20-23456789-2", cliente2);
//
//		        Map<String, String> cliente3 = new HashMap<>();
//		        cliente3.put("nombre", "Luis Rodriguez");
//		        cliente3.put("tipo", "C");
//		        jedis.hmset("cliente:20-34567890-3", cliente3);
//
//		        
//		        // Crear Pedidos para Cliente 1
//		        for (int i = 1; i <= 5; i++) {
//		            Map<String, String> pedido = new HashMap<>();
//		            pedido.put("producto1", String.valueOf((int) (Math.random() * 10 + 1)));
//		            pedido.put("producto2", String.valueOf((int) (Math.random() * 10 + 1)));
//		            pedido.put("producto3", String.valueOf((int) (Math.random() * 10 + 1)));
//		            jedis.hmset("pedido:20-12345678-1:" + i, pedido);
//		        }
//
//		        // Crear Pedidos para Cliente 2
//		        for (int i = 1; i <= 5; i++) {
//		            Map<String, String> pedido = new HashMap<>();
//		            pedido.put("producto4", String.valueOf((int) (Math.random() * 10 + 1)));
//		            pedido.put("producto5", String.valueOf((int) (Math.random() * 10 + 1)));
//		            pedido.put("producto6", String.valueOf((int) (Math.random() * 10 + 1)));
//		            jedis.hmset("pedido:20-23456789-2:" + i, pedido);
//		        }
//
//		        // Crear Pedidos para Cliente 3
//		        for (int i = 1; i <= 5; i++) {
//		            Map<String, String> pedido = new HashMap<>();
//		            pedido.put("producto7", String.valueOf((int) (Math.random() * 10 + 1)));
//		            pedido.put("producto8", String.valueOf((int) (Math.random() * 10 + 1)));
//		            pedido.put("producto9", String.valueOf((int) (Math.random() * 10 + 1)));
//		            jedis.hmset("pedido:20-34567890-3:" + i, pedido);
//		        }

		        // Consulta para obtener los pedidos de un cliente determinado
		        String clienteId = "20-12345678-1";
		        System.out.println("Pedidos del cliente " + clienteId + ":");
		        for (String key : jedis.keys("pedido:" + clienteId + ":*")) {
		            System.out.println("Pedido ID: " + key);
		            System.out.println(jedis.hgetAll(key));
		        }

		        // Consulta para obtener la cantidad de unidades de todos los productos de un pedido
		        String pedidoId = "pedido:20-12345678-1:1";
		        System.out.println("Productos del pedido " + pedidoId + ":");
		        Map<String, String> productos = jedis.hgetAll(pedidoId);
		        productos.forEach((producto, cantidad) -> {
		            System.out.println("Producto: " + producto + ", Cantidad: " + cantidad);
		        });
		        

		        System.out.println("Clientes:");
		        for (String key : jedis.keys("cliente:*")) {
		            System.out.println("Cliente ID: " + key);
		            System.out.println(jedis.hgetAll(key));
		        }
		        
		        // Cerrar la conexión
		        jedis.close();
		    }        
	}


