import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.QueryConfig;

public class docuNeo4j1 {
	
	
	public static void cargarDatos(Driver driver) {
		var result1 = driver.executableQuery("CREATE (:Person {name: $name, lastName: $lastName, age: $age})")
			    .withParameters(Map.of(
			        "name", "Alice",
			        "lastName", "Smith",
			        "age", 30
			    ))
			    .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
			    .execute();
		
		var summary = result1.summary();
		System.out.printf("Created %d records in %d ms.%n",
		    summary.counters().nodesCreated(),
		    summary.resultAvailableAfter(TimeUnit.MILLISECONDS));
		
		var result2 = driver.executableQuery("CREATE (:Person {name: $name, lastName: $lastName, age: $age})")
			    .withParameters(Map.of(
			        "name", "Agustin",
			        "lastName", "Palombo",
			        "age", 25
			    ))
			    .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
			    .execute();
		
		var summary2 = result2.summary();
		System.out.printf("Created %d records in %d ms.%n",
		    summary2.counters().nodesCreated(),
		    summary2.resultAvailableAfter(TimeUnit.MILLISECONDS));
		
		var result3 = driver.executableQuery("CREATE (:Person {name: $name, lastName: $lastName, age: $age})")
			    .withParameters(Map.of(
			        "name", "John",
			        "lastName", "Perez",
			        "age", 36
			    ))
			    .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
			    .execute();
		
		var summary3 = result3.summary();
		System.out.printf("Created %d records in %d ms.%n",
		    summary3.counters().nodesCreated(),
		    summary3.resultAvailableAfter(TimeUnit.MILLISECONDS));
		
	}
	
	
	public static void readAll(Driver driver) {
		System.out.println("Leemos todos los nodos: ");
		var result = driver.executableQuery("MATCH (p:Person) RETURN p.name AS name, p.lastName AS lastName, p.age AS age")
			    .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
			    .execute();

			// Loop through results and do something with them
			var records = result.records();
			records.forEach(r -> {
			    System.out.println(r);  // or r.get("name").asString()
			});

			// Summary information
			var summary = result.summary();
			System.out.printf("The query %s returned %d records in %d ms.%n",
			    summary.query(), records.size(),
			    summary.resultAvailableAfter(TimeUnit.MILLISECONDS));
	}
	
	public static void readAgustin (Driver driver) {
		System.out.println("Leemos el nodo igual a Agustin: ");
		var result = driver.executableQuery("MATCH (p:Person) WHERE p.name = 'Agustin' RETURN p.name AS name, p.lastName AS lastName, p.age AS age")
			    .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
			    .execute();

			// Loop through results and do something with them
			var records = result.records();
			records.forEach(r -> {
			    System.out.println(r);  // or r.get("name").asString()
			});

			// Summary information
			var summary = result.summary();
			System.out.printf("The query %s returned %d records in %d ms.%n",
			    summary.query(), records.size(),
			    summary.resultAvailableAfter(TimeUnit.MILLISECONDS));
		
	}
	
	public static void updateDatos(Driver driver) {
		System.out.println("Datos actualizados");
		var result = driver.executableQuery("MATCH (p:Person) WHERE p.name = 'Alice' SET p.age = 42 ")
			    .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
			    .execute();
		
			var summary = result.summary();
			System.out.println("Query updated the database?");
			System.out.println(summary.counters().containsUpdates());
	}
	
	
	private static void deleteAlice(Driver driver) {
		var result = driver.executableQuery("MATCH (p:Person)WHERE p.name = 'Alice' DELETE p")
			    .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
			    .execute();
			var summary = result.summary();
			System.out.println("Query updated the database?");
			System.out.println(summary.counters().containsUpdates());
		
	}
	
   public static void main(String... args) {

	        // URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
	        final String dbUri = "bolt://localhost:7687";
	        final String dbUser = "neo4j";
	        final String dbPassword = "12345678";

	        try (var driver = GraphDatabase.driver(dbUri, AuthTokens.basic(dbUser, dbPassword))) {
	            driver.verifyConnectivity();
	            
	            //cargarDatos(driver);
	            readAll(driver);
	            readAgustin(driver);
	            updateDatos(driver);
	            readAll(driver);
	            deleteAlice(driver);
	            readAll(driver);
	           
	        }
	        
	        
	        
	    }


	}
