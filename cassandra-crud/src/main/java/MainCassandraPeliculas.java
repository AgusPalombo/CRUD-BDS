import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

import java.util.Arrays;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.Literal;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;


public class MainCassandraPeliculas {
	

	public static void main(String[] args) {
		try (CqlSession session = CqlSession.builder().build()) {
			  CreateKeyspace createKs = createKeyspace("FinalBD2").ifNotExists().withSimpleStrategy(1);
			  session.execute(createKs.build());
			  
			// Iniciar la construcción de la tabla "movies"
			  
	            session.execute( createTable("FinalBD2", "movies")
	            	.ifNotExists()
	                .withPartitionKey("title", DataTypes.TEXT)
	                .withColumn("title", DataTypes.TEXT)
	                .withColumn("writer", DataTypes.TEXT)
	                .withColumn("year", DataTypes.INT)
	                .withColumn("actors", DataTypes.listOf(DataTypes.TEXT))
	                .withColumn("franchise", DataTypes.TEXT)
	                .withColumn("synopsis", DataTypes.TEXT)
	                .build()
	               );


			 
		
	            //fight club insert
	            session.execute(
	            insertInto("FinalBD2","movies")
                .value("title", literal("Fight Club"))
                .value("writer", literal("Chuck Palahniuk"))
                .value("year", literal(1999))
                .value("actors", literal(Arrays.asList("Brad Pitt", "Edward Norton")))
                .build()
                );
			  
//	            //PULP FICTION
	            session.execute(
	            insertInto("FinalBD2", "movies")
                .value("title", literal("Pulp Fiction"))
                .value("writer", literal("Quentin Tarantino"))
                .value("year", literal(1994))
                .value("actors", literal(Arrays.asList("John Travolta", "Uma Thurman")))
                .build()
                );
          
//	            //THE Hobbit
	            session.execute(insertInto("FinalBD2", "movies")
                    .value("title", literal("The Hobbit: An Unexpected Journey"))
                    .value("writer", literal("J.R.R. Tolkein"))
                    .value("year", literal(2012))
                    .value("franchise", literal("The Hobbit"))
                    .build()
                );
	            
	            //Pee Wee
	            
	            session.execute(insertInto("FinalBD2", "movies")
	                    .value("title", literal("Pee Wee"))
	                    .value("writer", literal("ASD"))
	                    .value("year", literal(2021))
	                    .build()
	                );
	            
	             
	            //LEER TODOS
	            ResultSet result = session.execute(selectFrom("FinalBD2","movies").all().build());
	            for(Row row : result) {
	            	 System.out.println("Title: " + row.getString("title"));
	                 System.out.println("Writer: " + row.getString("writer"));
	                 System.out.println("Year: " + row.getInt("year"));
	                 System.out.println("Actors: " + row.getList("actors", String.class));
	                 System.out.println("Franchise: " + row.getString("franchise"));
	                 System.out.println("Synopsis: " + row.getString("synopsis"));
	                 System.out.println("-------------------------");
	            }
	            
	            
	            //UPDATE EL HOBBIT
	            session.execute(update("FinalBD2", "movies")
	            	    .setColumn("synopsis", literal("A reluctant hobbit, Bilbo Baggins, sets out to the Lonely Mountain with a spirited group of dwarves to reclaim their mountain home - and the gold within it - from the dragon Smaug."))
	            	    .whereColumn("title").isEqualTo(literal("The Hobbit: An Unexpected Journey")).build());       
	            
	            
	            //UPDATE BRAD PITT
	            session.execute(update("FinalBD2", "movies")
	            		.appendListElement("actors", literal("Samuel L. Jackson"))
	            		.whereColumn("title").isEqualTo(literal("Pulp Fiction"))
	            		.build()
	            );
	            
	            
	            //DELETE PEE WEE
	            session.execute(deleteFrom("FinalBD2","movies")
	            		.whereColumn("title")
	            		.isEqualTo(literal("Pee Wee"))
	            		.build()
	            	);
	            
	          //LEER TODOS
	            ResultSet result2 = session.execute(selectFrom("FinalBD2","movies").all().build());
	            for(Row row : result2) {
	            	 System.out.println("Title: " + row.getString("title"));
	                 System.out.println("Writer: " + row.getString("writer"));
	                 System.out.println("Year: " + row.getInt("year"));
	                 System.out.println("Actors: " + row.getList("actors", String.class));
	                 System.out.println("Franchise: " + row.getString("franchise"));
	                 System.out.println("Synopsis: " + row.getString("synopsis"));
	                 System.out.println("-------------------------");
	            }
	            
	          
	         
			  
		}

	}

}
