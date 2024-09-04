import com.mongodb.*;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;

import static com.mongodb.client.model.Filters.eq;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

public class MongoTestDOCU {


	
	public static void main(String[] args) {
		String uri = "mongodb://localhost:27017";
		
		MongoClient mongoClient = MongoClients.create(uri);
		MongoDatabase database = mongoClient.getDatabase("movies");
		MongoCollection<Document> collection = database.getCollection("moviesEjercicio");
            
                           
            
            //Cargar documentos
		
            List<Document> movieList = Arrays.asList(
         
            		new Document()
            		.append("title", "Fight Club")
            		.append("writer", "Chuck Palahniuk")
            		.append("year", 1999)
            		.append("actors", Arrays.asList("Brad Pitt", "Edward Norton"))
            		,
            		
            	    new Document()
            		.append("title", "Pulp Fiction")
            		.append("writer", "Quentin Tarantino")
            		.append("year", 1994)
            		.append("actors", Arrays.asList("John Travolta", "Uma Thurman"))
            		,
            		
            		new Document()
                    .append("title", "Inglorious Basterds")
                    .append("writer", "Quentin Tarantino")
                    .append("year", 2009)
                    .append("actors", Arrays.asList("Brad Pitt", "Diane Kruger", "Eli Roth")),
                    
                    new Document()
                    .append("title", "The Hobbit: An Unexpected Journey")
                    .append("writer", "J.R.R. Tolkein")
                    .append("year", 2012)
                    .append("franchise", "The Hobbit"),
                    
                    new Document()
                    .append("title", "The Hobbit: The Desolation of Smaug")
                    .append("writer", "J.R.R. Tolkein")
                    .append("year", 2013)
                    .append("franchise", "The Hobbit"),
                    
                    new Document()
                    .append("title", "The Hobbit: The Battle of the Five Armies")
                    .append("writer", "J.R.R. Tolkein")
                    .append("year", 2012)
                    .append("franchise", "The Hobbit")
                    .append("synopsis", "Bilbo and Company are forced to engage in a war against an array of combatants and keep the Lonely Mountain from falling into the hands of a rising darkness."),
                    
                    new Document()
                    .append("title", "Pee Wee Herman's Big Adventure"),
                    
                    new Document()
                    .append("title", "Avatar")
            		           		
            		);
            
            //INSERTAR TODOS LOS DOCUMENTOS
            InsertManyResult insert = collection.insertMany(movieList);
            
            System.out.println("Inserted document ids: " + insert.getInsertedIds());
            
            
            //UPDATE PELICULAS
            
            // Update The hobbit: an unexpected Journey
	            try {
	                
	            	Bson query = Filters.eq("title", "The Hobbit: An Unexpected Journey");
	            	Bson updates = Updates.set("synopsis", "A reluctant hobbit, Bilbo Baggins, sets out to the Lonely Mountain with a spirited group of dwarves to reclaim their mountain home - and the gold within it - from the dragon Smaug.");
	            	UpdateResult result = collection.updateOne(query, updates);
	            	
	           // Prints the number of updated documents and the upserted document ID, if an upsert was performed
	            	if(result.getModifiedCount()>0) {
	            		System.out.println("Modified document count: " + result.getModifiedCount());
	            	}else {
	                    System.out.println("Upserted id: " + result.getUpsertedId());
	            	}
	            // Prints a message if any exceptions occur during the operation
	            } catch (MongoException me) {
	                System.err.println("Unable to update due to an error: " + me);
	            }
	            
	         // Update The hobbit: DESOLACION DE SMAUG
	            try {
	                
	            	Bson query = Filters.eq("title", "The Hobbit: The Desolation of Smaug");
	            	Bson updates = Updates.set("synopsis", "The dwarves, along with Bilbo Baggins and Gandalf the Grey, continue their quest to reclaim Erebor, their homeland, from Smaug. Bilbo Baggins is in possession of a mysterious and magical ring.");
	            	UpdateResult result = collection.updateOne(query, updates);
	            	
	           // Prints the number of updated documents and the upserted document ID, if an upsert was performed
	            	if(result.getModifiedCount()>0) {
	            		System.out.println("Modified document count: " + result.getModifiedCount());
	            	}else {
	                    System.out.println("Upserted id: " + result.getUpsertedId());
	            	}
	            // Prints a message if any exceptions occur during the operation
	            } catch (MongoException me) {
	                System.err.println("Unable to update due to an error: " + me);
	            }
            		
            
	         // Update Pulp Fiction
	            try {
	                
	            	Bson query = Filters.eq("title", "Pulp Fiction");
	            	Bson updates = Updates.addToSet("actors", "Samuel L. Jackson");
	            	UpdateResult result = collection.updateOne(query, updates);
	            	
	           // Prints the number of updated documents and the upserted document ID, if an upsert was performed
	            	if(result.getModifiedCount()>0) {
	            		System.out.println("Modified document count: " + result.getModifiedCount());
	            	}else {
	                    System.out.println("Upserted id: " + result.getUpsertedId());
	            	}
	            // Prints a message if any exceptions occur during the operation
	            } catch (MongoException me) {
	                System.err.println("Unable to update due to an error: " + me);
	            }
	            
	            // Update Avatar
	            try {
	                
	            	Bson query = Filters.eq("title", "Avatar");
	            	Bson updates = Updates.addToSet("actors", Arrays.asList("Sam Worthington", "Zoe Saldaña", "Stephen Lang" , "Sigourney Weaver", "Michelle Rodríguez" ));
	              	UpdateResult result = collection.updateOne(query, updates);
	            	
	           // Prints the number of updated documents and the upserted document ID, if an upsert was performed
	            	if(result.getModifiedCount()>0) {
	            		System.out.println("Modified document count: " + result.getModifiedCount());
	            	}else {
	                    System.out.println("Upserted id: " + result.getUpsertedId());
	            	}
	            // Prints a message if any exceptions occur during the operation
	            } catch (MongoException me) {
	                System.err.println("Unable to update due to an error: " + me);
	            }
	            
	            // Update Pee Wee
	            try {
	                
	            	Bson query = Filters.eq("title", "Pee Wee Herman´s Big Adventure");
	            	Bson updates = Updates.addToSet("writer", Arrays.asList("Tom McCarthy", "Alex Ross Perry", "Allison Schroeder"));
	              	UpdateResult result = collection.updateOne(query, updates);
	            	
	           // Prints the number of updated documents and the upserted document ID, if an upsert was performed
	            	if(result.getModifiedCount()>0) {
	            		System.out.println("Modified document count: " + result.getModifiedCount());
	            	}else {
	                    System.out.println("Upserted id: " + result.getUpsertedId());
	            	}
	            // Prints a message if any exceptions occur during the operation
	            } catch (MongoException me) {
	                System.err.println("Unable to update due to an error: " + me);
	            }
	            
	            
	            //BUSQUEDA DE DOCUMENTOS 1. Todos - 2. Quentin Tarantino - 3. 90s
	            
	            try {
		            // Buscar todos los documentos
		            System.out.println("Todos los documentos:");
		            MongoCursor<Document> cursor = collection.find().iterator();
		            while (cursor.hasNext()) {
		                System.out.println(cursor.next().toJson());
		            }
		            cursor.close();
	
		            // Buscar documentos de la época de los 90s
		            System.out.println("\nDocumentos de la época de los 90s:");
		            Bson filtro90s = Filters.and(
		                Filters.gte("year", 1990), // Mayor o igual a 1990
		                Filters.lt("year", 2000)   // Menor a 2000
		            );
		            cursor = collection.find(filtro90s).iterator();
		            while (cursor.hasNext()) {
		                System.out.println(cursor.next().toJson());
		            }
		            cursor.close();
	
		            // Buscar documentos con "writer" igual a "Quentin Tarantino"
		            System.out.println("\nDocumentos escritos por Quentin Tarantino:");
		            Bson filtroTarantino = Filters.eq("writer", "Quentin Tarantino");
		            cursor = collection.find(filtroTarantino).iterator();
		            while (cursor.hasNext()) {
		                System.out.println(cursor.next().toJson());
		            }
		            cursor.close();
	
		        } catch (Exception e) {
		            System.err.println("Error al conectar con MongoDB: " + e);
		        } 
	            
			  
		
		
			//ELIMINAR DOCUMENTOS
			
				//Eliminar Pee Wee
				Bson peeWee = eq("title", "Pee Wee Herman's Big Adventure"); 
				
				try {
	                DeleteResult result = collection.deleteOne(peeWee);
	                System.out.println("Deleted document count: " + result.getDeletedCount());
	          
	            } catch (MongoException me) {
	                System.err.println("Unable to delete due to an error: " + me);
	            }
				
				//Eliminar Contiene Brad Pitt
				Bson bradPitt = Filters.regex("actors", "Brad Pitt"); 
				
				try {
	                DeleteResult result = collection.deleteOne(bradPitt);
	                System.out.println("Deleted document count: " + result.getDeletedCount());
	          
	            } catch (MongoException me) {
	                System.err.println("Unable to delete due to an error: " + me);
	            }
		
			
        }
    }
	
	





