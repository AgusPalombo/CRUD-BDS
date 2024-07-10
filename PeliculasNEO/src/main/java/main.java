import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.QueryConfig;

public class main {

	public static void main(String[] args) {
		 // URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
        final String dbUri = "bolt://localhost:7687";
        final String dbUser = "neo4j";
        final String dbPassword = "peliculas";

        try (Driver driver = GraphDatabase.driver(dbUri, AuthTokens.basic(dbUser, dbPassword))) {
            driver.verifyConnectivity();
            System.out.println("Connection estabilished.");
            
            var result = driver.executableQuery("CREATE (:Person {name: $name})")
            	    .withParameters(Map.of("name", "Alice"))
            	    .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
            	    .execute();
            	var summary = result.summary();
            	System.out.printf("Created %d records in %d ms.%n",
            	    summary.counters().nodesCreated(),
            	    summary.resultAvailableAfter(TimeUnit.MILLISECONDS));
        }
    
        

	}

}
