import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.Record;
import org.neo4j.driver.RoutingControl;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionContext;
import org.neo4j.driver.exceptions.NoSuchRecordException;



public class Neo4JTEST {
	
			

	public static void main(String[] args) {
		final String dbUri = "bolt://localhost:7687";
        final String dbUser = "neo4j";
        final String dbPassword = "12345678";
        
        try (var driver = GraphDatabase.driver(dbUri, AuthTokens.basic(dbUser, dbPassword))) {
            driver.verifyConnectivity();
            
            Map<String, Object> parameterPulp = new HashMap<>();
            parameterPulp.put("title","Pulp Fiction");
            parameterPulp.put("writer", "Quentin Tarantino");
            parameterPulp.put("year", "1994");
            parameterPulp.put("actors", Arrays.asList("Brad Pitt", "Uma Thurman"));
            
            
			
			var resultPulp = driver.executableQuery("CREATE (m:Movie {title: $title, writer: $writer, year: $year, actors: $actors})")
                    .withParameters(parameterPulp)
                    .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
                    .execute();

            	var summaryPulp = resultPulp.summary();
            	System.out.printf("Created %d records in %d ms.%n",
            	    summaryPulp.counters().nodesCreated(),
            	    summaryPulp.resultAvailableAfter(TimeUnit.MILLISECONDS));
            	
            
            //FIGHT CLUB
            	
            Map<String, Object> parameterFight = new HashMap<>();
            parameterFight.put("title","Fight Club");
            parameterFight.put("writer", "Chuck Palahniuk");
            parameterFight.put("year", "1999");
            parameterFight.put("actors", Arrays.asList("Brad Pitt", "Edward Norton"));
            
           var resultFight = driver.executableQuery("CREATE (m:Movie {title: $title, writer: $writer, year: $year, actors: $actors})")
            .withParameters(parameterFight)
            .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
            .execute();
           
           var summaryFight = resultFight.summary();
           	System.out.printf("Created %d records in %d ms.%n",
           		summaryFight.counters().nodesCreated(),
           		summaryFight.resultAvailableAfter(TimeUnit.MILLISECONDS));
           
           
           	
           	//LEER TODOS
           	var readAll = driver.executableQuery("MATCH (m:Movie) RETURN m.title AS title")
           		    .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
           		    .execute();
           	
           	var records = readAll.records();
           	records.forEach(r -> {
           	    System.out.println(r);  // or r.get("name").asString()
           	});
           	
           	
           //ACTUALIZAR año como ej
           	var resultActualizar = driver.executableQuery("MATCH(m:Movie {title: $title}) SET m.year = $year")
           		    .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
           		    .withParameters(Map.of("title", "Pulp Fiction", "year","2024"))
           		    .execute();
           		var summaryActualizar = resultActualizar.summary();
           		System.out.println("Query updated the database?");
           		System.out.println(summaryActualizar.counters().containsUpdates());
           	
          //LEER TODOS
           	var readAll2 = driver.executableQuery("MATCH (m:Movie) RETURN m.year AS year")
           		    .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
           		    .execute();
           	
           	var records2 = readAll2.records();
           	records2.forEach(r -> {
           	    System.out.println(r);  // or r.get("name").asString()
           	});
           	
           	//ELIMINAR TODOS
           	var resultDelete = driver.executableQuery("MATCH (m:Movie) DETACH DELETE m")
                    .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
                    .execute();

            // Obtener el resumen de la ejecución
            var summaryDelete = resultDelete.summary();

            // Verificar si la consulta actualizó la base de datos
            boolean updates = summaryDelete.counters().containsUpdates();
            System.out.println("Query updated the database? " + updates);
           
           //CERRAR CONEXION	
           driver.close();	
            
        }
        
       
		
		
		
	}

}
