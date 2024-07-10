import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.RoutingControl;

public class MainNeoGente {

	public static void main(String[] args) {
		final String dbUri = "bolt://localhost:7687";
        final String dbUser = "neo4j";
        final String dbPassword = "peliculas";
        
        try (var driver = GraphDatabase.driver(dbUri, AuthTokens.basic(dbUser, dbPassword))) {
            driver.verifyConnectivity();
            
            @SuppressWarnings("rawtypes")
			List<Map> people = List.of(
                    Map.of("name", "Alice", "age", 42, "friends", List.of("Bob", "Peter", "Anna")),
                    Map.of("name", "Bob", "age", 19),
                    Map.of("name", "Peter", "age", 50),
                    Map.of("name", "Anna", "age", 30)
                );
            
               //Create some nodes
                people.forEach(person -> {
                    var result = driver.executableQuery("CREATE (p:Person {name: $person.name, age: $person.age})")
                        .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
                        .withParameters(Map.of("person", person))
                        .execute();
                });
                
                people.forEach(person -> {
                    if(person.containsKey("friends")) {
                        var result = driver.executableQuery("""
                            MATCH (p:Person {name: $person.name})
                            UNWIND $person.friends AS friend_name
                            MATCH (friend:Person {name: friend_name})
                            CREATE (p)-[:KNOWS]->(friend)
                             """)
                            .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
                            .withParameters(Map.of("person", person))
                            .execute();
                    }
                });
                
             // Retrieve Alice's friends who are under 40
                var result = driver.executableQuery("""
                    MATCH (p:Person {name: $name})-[:KNOWS]-(friend:Person)
                    WHERE friend.age < $age
                    RETURN friend
                     """)
                    .withConfig(QueryConfig.builder()
                        .withDatabase("neo4j")
                        .withRouting(RoutingControl.READ)
                        .build())
                    .withParameters(Map.of("name", "Alice", "age", 40))
                    .execute();

                // Loop through results and do something with them
                result.records().forEach(r -> {
                    System.out.println(r.toString());
                });

                // Summary information
                System.out.printf("The query %s returned %d records in %d ms.%n",
                    result.summary().query(), result.records().size(),
                    result.summary().resultAvailableAfter(TimeUnit.MILLISECONDS));
                
                var result2 = driver.executableQuery("""
                	    MATCH (p:Person {name: $name})
                	    DETACH DELETE p
                	    """)
                	    .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
                	    .withParameters(Map.of("name", "Bob"))
                	    .execute();
                	var summary = result2.summary();
                	System.out.println("Query updated the database?");
                	System.out.println(summary.counters().containsUpdates());
                	
               
                	var resultDelete = driver.executableQuery("MATCH (p:Person) DETACH DELETE p")
                            .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
                            .execute();
        

        }
	}
}
