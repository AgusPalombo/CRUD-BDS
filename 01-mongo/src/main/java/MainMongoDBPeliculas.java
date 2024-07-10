import java.util.Arrays;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.result.DeleteResult;

import static com.mongodb.client.model.Filters.*;

public class MainMongoDBPeliculas {

	
	public static void insertar() {
		
		String uri = "mongodb://127.0.0.1:27017";
		
		try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("FinalBD2");
            MongoCollection<Document> collection = database.getCollection("movies");

            List<Document> movieList = Arrays.asList(
            		new Document("title", "Fight Club").append("writer", "Chuck Palahniuk").append("year", 1999).append("actors", Arrays.asList("Brad Pitt", "Edward Norton")),
                    new Document("title", "Pulp Fiction").append("writer", "Quentin Tarantino").append("year", 1994).append("actors", Arrays.asList("John Travolta", "Uma Thurman")),
                    new Document("title", "Inglorious Basterds").append("writer", "Quentin Tarantino").append("year", 2009).append("actors", Arrays.asList("Brad Pitt", "Diane Kruger", "Eli Roth")),
                    new Document("title", "The Hobbit: An Unexpected Journey").append("writer", "J.R.R. Tolkien").append("year", 2012).append("franchise", "The Hobbit"),
                    new Document("title", "The Hobbit: The Desolation of Smaug").append("writer", "J.R.R. Tolkien").append("year", 2013).append("franchise", "The Hobbit"),
                    new Document("title", "The Hobbit: The Battle of the Five Armies").append("writer", "J.R.R. Tolkien").append("year", 2014).append("franchise", "The Hobbit").append("synopsis", "Bilbo and Company are forced to engage in a war against an array of combatants and keep the Lonely Mountain from falling into the hands of a rising darkness."),
                    new Document("title", "Pee Wee Herman's Big Adventure"),
                    new Document("title", "Avatar")
                  );
            try {
                
                InsertManyResult result = collection.insertMany(movieList);

                System.out.println("Inserted document ids: " + result.getInsertedIds());
            
            } catch (MongoException me) {
                System.err.println("Unable to insert due to an error: " + me);
            }
            leerTodos();
        }
	}
	
	public static void leerTodos() {
		String uri = "mongodb://127.0.0.1:27017";
		
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("FinalBD2");
            MongoCollection<Document> collection = database.getCollection("movies");

            Bson projectionFields = Projections.fields(
                    Projections.include("title", "writer", "year", "actors", "synopsis"),
                    Projections.excludeId());
            
            MongoCursor<Document> cursor = collection.find()
                    .projection(projectionFields)
                    .sort(Sorts.descending("title")).iterator();
            
            try {
                while(cursor.hasNext()) {
                    System.out.println(cursor.next().toJson());
                }
                
            } finally {
                cursor.close();
            }
            
        }
		
	}
	
	
	public static void updateSynopsis() {
		 String uri = "mongodb://127.0.0.1:27017";
		 
	        try (MongoClient mongoClient = MongoClients.create(uri)) {
	            MongoDatabase database = mongoClient.getDatabase("FinalBD2");
	            MongoCollection<Document> collection = database.getCollection("movies");
	            
	            //"The Hobbit: An Unexpected Journey" update
	            
	            Document query = new Document().append("title",  "The Hobbit: An Unexpected Journey");

	            Bson updates = Updates.combine(Updates.addToSet("synopsis", "A reluctant hobbit, Bilbo Baggins, sets out to the Lonely Mountain with a spirited \r\n"
	            		+ "group of dwarves to reclaim their mountain home - and the gold within it - from \r\n"
	            		+ "the dragon Smaug."));
	                    
	            //si no existe el doc que buscamos lo crea
	            UpdateOptions options = new UpdateOptions().upsert(false);
	            
	            try {
	                UpdateResult result = collection.updateOne(query, updates, options);

	                System.out.println("Modified document count: " + result.getModifiedCount());
	                System.out.println("Upserted id: " + result.getUpsertedId());
	            
	            } catch (MongoException me) {
	                System.err.println("Unable to update due to an error: " + me);
	            }
	            
	          //"The Hobbit: The Desolation of Smaug" update
	            Document query2 = new Document().append("title",  "The Hobbit: The Desolation of Smaug");

	            Bson updates2 = Updates.combine(Updates.addToSet("synopsis", "The dwarves, along with Bilbo Baggins and Gandalf the Grey, continue their quest to reclaim Erebor, their homeland, from Smaug. Bilbo Baggins is in possession of a mysterious and magical ring."));
	                    
	            //si no existe el doc que buscamos lo crea
	            UpdateOptions options2 = new UpdateOptions().upsert(false);
	            
	            try {
	                UpdateResult result2 = collection.updateOne(query2, updates2, options2);

	                System.out.println("Modified document count: " + result2.getModifiedCount());
	                System.out.println("Upserted id: " + result2.getUpsertedId());
	            
	            } catch (MongoException me) {
	                System.err.println("Unable to update due to an error: " + me);
	            }
	               
	        }
	}
	
	
	private static void updateActors() {
		 String uri = "mongodb://127.0.0.1:27017";
		 try (MongoClient mongoClient = MongoClients.create(uri)) {
			 
	            MongoDatabase database = mongoClient.getDatabase("FinalBD2");
	            MongoCollection<Document> collection = database.getCollection("movies");
	
	            
	            //UPDATE ACTORS PULP FICTION
	            Document queryPulp = new Document().append("title", "Pulp Fiction");

	            Bson updates = Updates.combine(
	                    Updates.addToSet("actors", "Samuel L. Jackson"));

	            try {
	                UpdateResult result = collection.updateMany(queryPulp, updates);

	                System.out.println("Modified document count: " + result.getModifiedCount());

	            } catch (MongoException me) {
	                System.err.println("Unable to update due to an error: " + me);
	            }
	            
	            //UPDATE ACTORS AVATAR
	            Document queryAvatar = new Document().append("title", "Avatar");

	            Bson updatesAvatar = Updates.combine(
	                    Updates.addToSet("actors", Arrays.asList("Sam Worthington", "Zoe Saldaña", "Stephen Lang" , "Sigourney Weaver","Michelle Rodríguez")));

	            try {
	                UpdateResult result = collection.updateMany(queryAvatar, updatesAvatar);

	                System.out.println("Modified document count: " + result.getModifiedCount());

	            } catch (MongoException me) {
	                System.err.println("Unable to update due to an error: " + me);
	            }
	            
	            //UPDATE ACTORS PEE WEE
	            Document queryPee = new Document().append("title", "Pee Wee Herman's Big Adventure");

	            Bson updatesPee = Updates.combine(
	                    Updates.addToSet("actors", Arrays.asList("Tom McCarthy", "Alex Ross Perry", "Allison Schroeder")));

	            try {
	                UpdateResult result = collection.updateMany(queryPee, updatesPee);

	                System.out.println("Modified document count: " + result.getModifiedCount());

	            } catch (MongoException me) {
	                System.err.println("Unable to update due to an error: " + me);
	            }
	            
	            
	        }
		
	}
	
	public static void leerTarantino() {
		
		String uri = "mongodb://127.0.0.1:27017";
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("FinalBD2");
            MongoCollection<Document> collection = database.getCollection("movies");

            Bson projectionFields = Projections.fields(
                    Projections.include("title", "year", "actors"),
                    Projections.excludeId());

            MongoCursor<Document> cursor = collection.find(eq("writer", "Quentin Tarantino"))
                    .projection(projectionFields)
                    .sort(Sorts.descending("title")).iterator();

            try {
                while(cursor.hasNext()) {
                    System.out.println(cursor.next().toJson());
                }
            } finally {
                cursor.close();
            }
        }
		
	}
	
	public static void leerBrad() {
		String uri = "mongodb://127.0.0.1:27017";
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("FinalBD2");
            MongoCollection<Document> collection = database.getCollection("movies");

            Bson projectionFields = Projections.fields(
                    Projections.include("title"),
                    Projections.excludeId());

            MongoCursor<Document> cursor = collection.find(eq("actors", "Brad Pitt"))
                    .projection(projectionFields)
                    .sort(Sorts.descending("title")).iterator();

            try {
                while(cursor.hasNext()) {
                    System.out.println(cursor.next().toJson());
                }
            } finally {
                cursor.close();
            }
        }
	}
	
	
	public static void leer90s() {
		String uri = "mongodb://127.0.0.1:27017";
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("FinalBD2");
            MongoCollection<Document> collection = database.getCollection("movies");

            Bson projectionFields = Projections.fields(
                    Projections.include("title", "year"),
                    Projections.excludeId());

            MongoCursor<Document> cursor = collection.find(Filters.and(Filters.gte("year", 1990), Filters.lte("year", 2000)))
                    .projection(projectionFields)
                    .sort(Sorts.descending("title")).iterator();

            try {
                while(cursor.hasNext()) {
                    System.out.println(cursor.next().toJson());
                }
            } finally {
                cursor.close();
            }
        }
	}
	
	public static void leer2000() {
		String uri = "mongodb://127.0.0.1:27017";
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("FinalBD2");
            MongoCollection<Document> collection = database.getCollection("movies");

            Bson projectionFields = Projections.fields(
                    Projections.include("title", "year"),
                    Projections.excludeId());

            MongoCursor<Document> cursor = collection.find(Filters.and(Filters.gte("year", 2000), Filters.lte("year", 2010)))
                    .projection(projectionFields)
                    .sort(Sorts.descending("title")).iterator();

            try {
                while(cursor.hasNext()) {
                    System.out.println(cursor.next().toJson());
                }
            } finally {
                cursor.close();
            }
        }
	}
	
	public static void DeletePeliculas() {
		
        	String uri = "mongodb://127.0.0.1:27017";
	        try (MongoClient mongoClient = MongoClients.create(uri)) {
	            MongoDatabase database = mongoClient.getDatabase("FinalBD2");
	            MongoCollection<Document> collection = database.getCollection("movies");
	            
	            //ELIMINAR PEE WEE
	            Bson query = eq("title", "Pee Wee Herman's Big Adventure");
	            try {
	
	                DeleteResult result = collection.deleteOne(query);
	                System.out.println("Deleted document count: " + result.getDeletedCount());

	            } catch (MongoException me) {
	                System.err.println("Unable to delete due to an error: " + me);
	            }
	            
	            //ELIMINAR Brad Pit
	            Bson queryBrad = eq("actors", "Brad Pitt");
	            try {
	
	                DeleteResult result2 = collection.deleteOne(queryBrad);
	                System.out.println("Deleted document count: " + result2.getDeletedCount());

	            } catch (MongoException me) {
	                System.err.println("Unable to delete due to an error: " + me);
	            }
	            
	            leerTodos();
	        }
	    }
	
	public static void deleteAll() {
		String uri = "mongodb://127.0.0.1:27017";
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("FinalBD2");
            MongoCollection<Document> collection = database.getCollection("movies");

            // Borrar todos los documentos
            collection.deleteMany(new Document());
            System.out.println("All documents deleted from the collection");
            leerTodos();
        	}
        }
	

	public static void main(String[] args) {
		//A
		System.out.println("Todas las peliculas:");
		insertar();
		System.out.println(" ");
		//B
		System.out.println("Update Actores:");
		updateActors();
		System.out.println(" ");
		
		System.out.println("Update Synopsis:");
		updateSynopsis();
		System.out.println(" ");
		
		//C
		System.out.println("Todas las peliculas:");
		leerTodos();
		System.out.println(" ");
		
		System.out.println("Peliculas escritar por Quentin Tarantino:");
		leerTarantino();
		System.out.println(" ");
		
		System.out.println("Peliculas donde actua Brad Pitt:");
		leerBrad();
		System.out.println(" ");
		
		System.out.println("Peliculas de los 90:");
		leer90s();
		System.out.println(" ");
		
		System.out.println("Peliculas de los 2000:");
		leer2000();
		System.out.println(" ");
		
		System.out.println("Eliminar Peliculas:");
		DeletePeliculas();
		System.out.println(" ");
		
		System.out.println("Eliminar todas las Peliculas:");
		deleteAll();

    }

	

	

	


	
}


