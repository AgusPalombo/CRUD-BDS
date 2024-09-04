import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspaceStart;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;
public class CRUDtest2 {

	
	
	private static void leerTodos(CqlSession session) {
		ResultSet result = session.execute("SELECT * FROM personas");
		
		System.out.println("todos los registros: ");
		for(Row row:result) {
			System.out.println(row.getString("id"));
			System.out.println(row.getString("first_name"));
			System.out.println(row.getString("last_name"));
			System.out.println();
		}
		
		
	}
	
	
	public static void main(String[] args) {
		
		
		CqlSession session = CqlSession.builder()
			    .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
			    .withLocalDatacenter("datacenter1")
			    .build();
		
		session.execute("CREATE KEYSPACE IF NOT EXISTS gentes WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
	    session.execute("USE gente");
	
				
		//CREAR TABLA PERSONAS
		
		session.execute( createTable("gentes", "personas").ifNotExists()
			    .withPartitionKey("id", DataTypes.INT)
			    .withColumn("id", DataTypes.INT)
     			.withColumn("first_name", DataTypes.TEXT)
     			.withColumn("last_name", DataTypes.TEXT)
			    .withColumn("cyclist_name", DataTypes.TEXT)
			    .build()
			   );
		
		
		//CARGAR DATOS
		
		//PERSONA 1
		
		session.execute( insertInto("personas")
	    		.value("id", literal(1))
	    		.value("first_name", literal("John"))
	    		.value("last_name", literal("Doe"))
	    		.build()
	    );
	    		
		
		
		//PERSONA 2
		session.execute( insertInto("personas")
			    .value("id", literal(2))
			    .value("first_name", literal("Agustin"))
			    .value("last_name", literal("Palombo"))
			    .build()
			);
		
		//PERSONA 3
		session.execute( insertInto("personas")
			    .value("id", literal(3))
			    .value("first_name", literal("Maria"))
			    .value("last_name", literal("Perez"))
			    .build()
			);
		
		
		//READ ALL
		leerTodos(session);
		
		
		//READ ID 2
		ResultSet result = session.execute(
				selectFrom("personas")
				.column("firs_name")
				.column("last_name")
				.whereColumn("id").isEqualTo(literal(2))
				.build()
		);		
		
		System.out.println("PERSONA CON EL ID 2: ");
		
		for (Row row:result) {
			System.out.println(row.getString("id"));
			System.out.println(row.getString("first_name"));
			System.out.println(row.getString("last_name"));
			System.out.println();
		}
		
		
		
	}

}
