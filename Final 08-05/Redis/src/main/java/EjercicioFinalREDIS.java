import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;


public class EjercicioFinalREDIS {

	public static void main(String[] args) {
		 JedisPool pool = new JedisPool("localhost", 6379);
		 
		 try (Jedis jedis = pool.getResource()) {
		 
		 //CARGAR CLIENTES
		 
		 //CLIENTE 1
		 Map<String, String> cliente1 = new HashMap<>();
		 cliente1.put("nombre", "Agustin Palombo");
		 cliente1.put("tipo", "A");
		 jedis.hmset("cliente:1",cliente1);
		 
		 
		 //CLIENTE 2
		 Map<String, String> cliente2 = new HashMap<>();
		 cliente2.put("nombre", "Karina Brunacci");
		 cliente2.put("tipo", "b");
		 jedis.hmset("cliente:2",cliente1);
		 
		 //CLIENTE 3
		 Map<String, String> cliente3 = new HashMap<>();
		 cliente3.put("nombre", "Ricardo Palombo");
		 cliente3.put("tipo", "C");
		 jedis.hmset("cliente:3",cliente3);
		 
		 
		 //CARGAR PEDIDOS
		 
		 //PEDIDO 1
		 
		 Map<String, String> productosClienteA = new HashMap<>();
         productosClienteA.put("Producto 1", "5");
         productosClienteA.put("Producto 2", "10");
         productosClienteA.put("Producto 3", "15");
         jedis.hmset("pedido:1"  + ":cliente:1", productosClienteA);
         
         Map<String, String> productosClienteA2 = new HashMap<>();
         productosClienteA2.put("Producto 1", "5");
         productosClienteA2.put("Producto 2", "10");
         productosClienteA2.put("Producto 3", "15");
         jedis.hmset("pedido:1B"  + ":cliente:1", productosClienteA2);
         
         //PEDIDO 2
		 
		 Map<String, String> productosClienteB = new HashMap<>();
         productosClienteB.put("Producto 1", "6");
         productosClienteB.put("Producto 2", "11");
         productosClienteB.put("Producto 3", "16");
         jedis.hmset("pedido:2"  + ":cliente:2", productosClienteB);
         
         //PEDIDO 3
		 
		 Map<String, String> productosClienteC = new HashMap<>();
         productosClienteC.put("Producto 1", "16");
         productosClienteC.put("Producto 2", "21");
         productosClienteC.put("Producto 3", "26");
         jedis.hmset("pedido:3"  + ":cliente:3", productosClienteC);

         
         //GET ALL PEDIDOS DE CLIENTE 1
         Scanner scannerCliente = new Scanner(System.in);
         System.out.print("Ingrese el número de cliente: ");
         String clienteBuscar = scannerCliente.nextLine();
         
         
         Set<String> cliente = jedis.keys("pedido:*:cliente:"+clienteBuscar);
         for (String key : cliente) {
             System.out.println(key + ": " + jedis.hgetAll(key));
         }
         
         
         Scanner scannerPedido = new Scanner(System.in);
         System.out.print("Ingrese el número de pedido: ");
         String pedido = scannerPedido.nextLine();
         
         
         Set<String> pedidoCliente = jedis.keys("pedido:*:cliente:"+pedido);
         for (String  key : pedidoCliente) {
             System.out.println(key + ": " + jedis.hgetAll(key));
         }
		 }

	}
}
