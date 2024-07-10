import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.Record;
import org.neo4j.driver.RoutingControl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class peliculas {

    public static void main(String... args) {
        final String dbUri = "bolt://localhost:7687"; // Reemplaza con tu URI
        final String dbUser = "neo4j"; // Reemplaza con tu usuario
        final String dbPassword = "peliculas"; 

        try (var driver = GraphDatabase.driver(dbUri, AuthTokens.basic(dbUser, dbPassword))) {

            // Datos de la película
            
            List<Map<String, Object>> movies = List.of(
                    Map.of("title", "Fight Club", "writer", "Chuck Palahniuk", "year", 1999, "actors", List.of("Brad Pitt", "Edward Norton")),
                    Map.of("title", "Pulp Fiction", "writer", "Quentin Tarantino", "year", 1994, "actors", List.of("John Travolta", "Uma Thurman")),
                    Map.of("title", "Inglorious Basterds", "writer", "Quentin Tarantino", "year", 2009, "actors", List.of("Brad Pitt", "Diane Kruger", "Eli Roth")),
                    Map.of("title", "The Hobbit: An Unexpected Journey", "writer", "J.R.R. Tolkien", "year", 2012, "franchise", "The Hobbit"),
                    Map.of("title", "The Hobbit: The Desolation of Smaug", "writer", "J.R.R. Tolkien", "year", 2013, "franchise", "The Hobbit"),
                    Map.of("title", "The Hobbit: The Battle of the Five Armies", "writer", "J.R.R. Tolkien", "year", 2014, "franchise", "The Hobbit", "synopsis", "Bilbo and Company are forced to engage in a war against an array of combatants and keep the Lonely Mountain from falling into the hands of a rising darkness."),
                    Map.of("title", "Pee Wee Herman's Big Adventure"),
                    Map.of("title", "Avatar")
                );
           
            movies.forEach(movie -> {
                var result = driver.executableQuery("CREATE (m:Movie {title: $movie.title, writer: $movie.writer, year: $movie.year, actors: $movie.actors, franchise: $movie.franchise, synopsis: $movie.synopsis})")
                    .withConfig(QueryConfig.builder().withDatabase("peliculas").build())
                    .withParameters(Map.of("movie", movie))
                    .execute();
            });
            try {
                // Insertar la película
                driver.executableQuery("CREATE (m:Movie {title: $movie.title, writer: $movie.writer, year: $movie.year, actors: $movie.actors, franchise: $movie.franchise, synopsis: $movie.synopsis})")
                    .withConfig(QueryConfig.builder().withDatabase("peliculas").build())
                    .withParameters(Map.of("movie", movies))
                    .execute();
                System.out.println("Movie 'The Hobbit: An Unexpected Journey' added successfully.");
                
            
            //Actualizar Sinopsis The hobbit:An unexpected journey
            
            var actualizarHobbit = driver.executableQuery("""
            	    MATCH (m:Movie {title: $title})
            	    SET m.synopsis = $synopsis
            	    """)
            	    .withConfig(QueryConfig.builder().withDatabase("peliculas").build())
            	    .withParameters(Map.of("title", "The Hobbit: An Unexpected Journey", "synopsis", "\"A reluctant hobbit, Bilbo Baggins, sets out to the Lonely Mountain with a spirited \r\n"
            	    		+ "group of dwarves to reclaim their mountain home - and the gold within it - from \r\n"
            	    		+ "the dragon Smaug."))
            	    .execute();
            	var summary = actualizarHobbit.summary();
            	System.out.println("Query updated the database?");
            	System.out.println(summary.counters().containsUpdates());
            	
            	//Actualizar Sinopsis The hobbit: The Desolation of Smaug
                
                var actualizarHobbit2 = driver.executableQuery("""
                	    MATCH (m:Movie {title: $title})
                	    SET m.synopsis = $synopsis
                	    """)
                	    .withConfig(QueryConfig.builder().withDatabase("peliculas").build())
                	    .withParameters(Map.of("title", "The Hobbit: The Desolation of Smaug", "synopsis", "The dwarves, along with Bilbo Baggins and Gandalf the Grey, continue their \r\n"
                	    		+ "quest to reclaim Erebor, their homeland, from Smaug. Bilbo Baggins is in \r\n"
                	    		+ "possession of a mysterious and magical ring.\""))
                	    .execute();
                	var summary2 = actualizarHobbit2.summary();
                	System.out.println("Query updated the database?");
                	System.out.println(summary2.counters().containsUpdates());
                	
                
                	 // Agregar un actor a la película "Pulp Fiction"
                    
                    driver.executableQuery("MATCH (m:Movie {title: $title}) SET m.actors = coalesce(m.actors, []) + $actor")
                        .withConfig(QueryConfig.builder().withDatabase("peliculas").build())
                        .withParameters(Map.of("title", "Pulp Fiction", "actor", "Samuel L. Jackson"))
                        .execute();
                    System.out.println("Actor 'Samuel L. Jackson' added to 'Pulp Fiction' successfully.");

            
            
             // Leer todas las películas
                var result = driver.executableQuery("MATCH (m:Movie) RETURN m.title AS title, m.writer AS writer, m.year AS year, m.actors AS actors, m.franchise AS franchise, m.synopsis AS synopsis")
                    .withConfig(QueryConfig.builder()
                        .withDatabase("peliculas")
                        .withRouting(RoutingControl.READ)
                        .build())
                    .execute();

                System.out.println("All movies:");
                result.records().forEach(r -> {
                	 System.out.println("Title: " + r.get("title").asString());
                     System.out.println("Writer: " + (r.get("writer").isNull() ? "" : r.get("writer").asString()));
                     System.out.println("Year: " + (r.get("year").isNull() ? "" : r.get("year").asInt()));
                     System.out.println("Actors: " + (r.get("actors").isNull() ? "" : String.join(", ", r.get("actors").asList(v -> v.asString()))));
                     System.out.println("Franchise: " + (r.get("franchise").isNull() ? "" : r.get("franchise").asString()));
                     System.out.println("Synopsis: " + (r.get("synopsis").isNull() ? "" : r.get("synopsis").asString()));
                    System.out.println("-------------------------");
                });
              
                //DELETE ALL MOVIES
	                driver.executableQuery("MATCH (m:Movie) DELETE m")
	                .withConfig(QueryConfig.builder().withDatabase("peliculas").build())
	                .execute();
	                System.out.println("All movies deleted successfully.");
	                
               // Summary information
                System.out.printf("The query %s returned %d records in %d ms.%n",
                    result.summary().query(), result.records().size(),
                    result.summary().resultAvailableAfter(TimeUnit.MILLISECONDS));
                
                
                // Actualizar la sinopsis de la película
                String newSynopsis = "A reluctant hobbit, Bilbo Baggins, sets out to the Lonely Mountain with a spirited group of dwarves to reclaim their mountain home - and the gold within it - from the dragon Smaug. Bilbo Baggins is in possession of a mysterious and magical ring.";
                driver.executableQuery("MATCH (m:Movie {title: $title}) SET m.synopsis = $synopsis")
                    .withConfig(QueryConfig.builder().withDatabase("peliculas").build())
                    .withParameters(Map.of("title", "The Hobbit: An Unexpected Journey", "synopsis", newSynopsis))
                    .execute();
                System.out.println("Movie 'The Hobbit: An Unexpected Journey' updated successfully.");
                
                
                
            // Leer todas las películas
                var result2 = driver.executableQuery("MATCH (m:Movie) RETURN m.title AS title, m.writer AS writer, m.year AS year, m.actors AS actors, m.franchise AS franchise, m.synopsis AS synopsis")
                    .withConfig(QueryConfig.builder()
                        .withDatabase("peliculas")
                        .withRouting(RoutingControl.READ)
                       .build())
                    .execute();

                System.out.println("All movies:");
                result2.records().forEach(r -> {
                    System.out.println("Title: " + r.get("title").asString());
                    System.out.println("Writer: " + r.get("writer").asString());
                    System.out.println("Year: " + r.get("year").asInt());
                    System.out.println("Actors: " + String.join(", ", r.get("actors").asList(v -> v.asString())));
                    System.out.println("Franchise: " + r.get("franchise").asString());
                    System.out.println("Synopsis: " + r.get("synopsis").asString());
                    System.out.println("-------------------------");
                });

                // Summary information
                System.out.printf("The query %s returned %d records in %d ms.%n",
                    result.summary().query(), result.records().size(),
                    result.summary().resultAvailableAfter(TimeUnit.MILLISECONDS));

                // Eliminar la película
                driver.executableQuery("MATCH (m:Movie {title: $title}) DELETE m")
                    .withConfig(QueryConfig.builder().withDatabase("peliculas").build())
                    .withParameters(Map.of("title", "The Hobbit: An Unexpected Journey"))
                    .execute();
                System.out.println("Movie 'The Hobbit: An Unexpected Journey' deleted successfully.");

                  // Leer todas las películas
                var result3 = driver.executableQuery("MATCH (m:Movie) RETURN m.title AS title, m.writer AS writer, m.year AS year, m.actors AS actors, m.franchise AS franchise, m.synopsis AS synopsis")
                    .withConfig(QueryConfig.builder()
                        .withDatabase("peliculas")
                        .withRouting(RoutingControl.READ)
                        .build())
                    .execute();

                System.out.println("All movies:");
                result3.records().forEach(r -> {
                    System.out.println("Title: " + r.get("title").asString());
                    System.out.println("Writer: " + r.get("writer").asString());
                    System.out.println("Year: " + r.get("year").asInt());
                    System.out.println("Actors: " + String.join(", ", r.get("actors").asList(v -> v.asString())));
                    System.out.println("Franchise: " + r.get("franchise").asString());
                    System.out.println("Synopsis: " + r.get("synopsis").asString());
                    System.out.println("-------------------------");
                });

                // Summary information
                System.out.printf("The query %s returned %d records in %d ms.%n",
                    result.summary().query(), result.records().size(),
                    result.summary().resultAvailableAfter(TimeUnit.MILLISECONDS));

            } catch (Exception e) {
                System.out.println(e.getMessage());
                System.exit(1);
            }
        }
    }

