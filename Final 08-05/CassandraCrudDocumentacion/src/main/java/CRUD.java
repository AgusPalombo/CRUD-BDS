import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspaceStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;


import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;



import java.net.InetSocketAddress;



public class CRUD {

	public static void main(String[] args) {
		

		
		 CqlSession session = CqlSession.builder()
				    .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
				    .withLocalDatacenter("datacenter1")
				    .build();
	
		
		 session.execute("CREATE KEYSPACE IF NOT EXISTS personas WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
	     session.execute("USE personas");
	     
	     
	     session.execute( createTable("personas", "persona")
	    		 	.ifNotExists()
	    		    .withPartitionKey("id", DataTypes.INT)
	    		    .withColumn("id", DataTypes.INT)
	     			.withColumn("first_name", DataTypes.TEXT)
	     			.withColumn("last_name", DataTypes.TEXT).build()
	     );	
	     
		//PERSONA 1
	    session.execute( insertInto("persona")
	    		.value("id", literal(1))
	    		.value("first_name", literal("John"))
	    		.value("last_name", literal("Doe"))
	    		.build()
		);
		
		//PERSONA 2
	    session.execute(
			insertInto("persona")
		    .value("id", literal(2))
		    .value("first_name", literal("Maria"))
		    .value("last_name", literal("Perez"))
		    .build()
	    );
		
		//PERSONA 3
	    session.execute(
	    	insertInto("persona")
		    .value("id", literal(3))
		    .value("first_name", literal("Agustin"))
		    .value("last_name", literal("Palombo"))
		    .build()
	    );
		
		
		//SELECCIONAR TODOS
	   ResultSet result =  session.execute(
			 selectFrom("persona")
		    .column("first_name")
		    .column("last_name")
		    .build()
		 );
	   
	   for (Row row : result) {
		   String nombre = row.getString("first_name");
		   String apellido = row.getString("last_name");
		   System.out.println(nombre + " " + apellido);
		   
	   }
	   
	   System.out.println();
		
		//SELECCIONAR JOHN DOE
	   ResultSet result2 = session.execute(selectFrom("persona").all().whereColumn("id").isEqualTo(literal(1)).build());
	   System.out.println("TODOS LOS QUE ESTAN EN LA TABLA PERSONA");
	   
	   for (Row row : result2) {
		   int id = row.getInt("id");
		   String nombre = row.getString("first_name");
		   String apellido = row.getString("last_name");
		   System.out.println("Persona con ID "+ id +" = "+ nombre + " " + apellido);
		   
	   }
		
	   System.out.println();
	   
	   //Actualizar maria a Gonzalez
	   session.execute(
		   update("persona")
	       .setColumn("last_name", literal("Gonzalez"))
	       .whereColumn("id").isEqualTo(literal(2))
	       .build()
       );
	   
	 //SELECCIONAR TODOS
	   ResultSet result3 =  session.execute(
			 selectFrom("persona")
		    .column("first_name")
		    .column("last_name")
		    .build()
		 );
	   
	   System.out.println("TODOS LOS QUE ESTAN EN LA TABLA PERSONA");
	   
	   for (Row row : result3) {
		   String nombre = row.getString("first_name");
		   String apellido = row.getString("last_name");
		   System.out.println(nombre + " " + apellido);
	   }
	   
	   System.out.println();
	   
	 //DELETE MARIA
	   
	   session.execute(
	      deleteFrom("persona")
	      .whereColumn("id")
	      .isEqualTo(literal(2))
	      .build()
	   );
	   
	   
	 //SELECCIONAR TODOS
	   ResultSet result4 =  session.execute(
			 selectFrom("persona")
		    .column("first_name")
		    .column("last_name")
		    .build()
		 );
	   
	   System.out.println("TODOS LOS QUE ESTAN EN LA TABLA PERSONA");
	   
	   for (Row row : result4) {
		   String nombre = row.getString("first_name");
		   String apellido = row.getString("last_name");
		   System.out.println(nombre + " " + apellido);
		}  
	   System.out.println();
	}

	
}
