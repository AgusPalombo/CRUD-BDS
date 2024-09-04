import static com.mongodb.client.model.Filters.eq;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;


import java.util.Arrays;
import java.util.List;
import org.bson.Document;


public class TestMongo2 {
	
	
	
	
	public static void main(String[] args) {
		
		 		String uri = "mongodb://127.0.0.1:27017";
		 		MongoClient mongoClient = MongoClients.create(uri);
	            MongoDatabase database = mongoClient.getDatabase("personas");
	            MongoCollection<Document> collection = database.getCollection("gente");
	            
	            
	            
	            //AGREGAR DATOS
	        	//List<Document> humanos = Arrays.asList(
	            //    	new Document().append("ID", 1).append("nombre", "Agustin").append("apellido", "Palombo"),
	            //    	new Document().append("ID", 2).append("nombre", "Mario").append("apellido", "Pergolini"),
	            //    	new Document().append("ID", 3).append("nombre", "Juan").append("apellido", "Doe")
	            // );
	        	
	        	//InsertManyResult result = collection.insertMany(humanos);
	        	//System.out.println("Inserted document ids: " + result.getInsertedIds());
	            
	        	
				//LEER TODOS LOS DATOS
	        	
	        	System.out.println("Todos los documentos:");
	            MongoCursor<Document> cursor = collection.find().iterator();
	            while (cursor.hasNext()) {
	                System.out.println(cursor.next().toJson());
	            }
	            cursor.close();
	            
	            //LEER PERSONA CON ID 1
	            Bson projectionFields = Projections.fields(
	                    Projections.include("ID", "nombre"),
	                    Projections.excludeId());
	            // Retrieves the first matching document, applying a projection and a descending sort to the results
	            Document doc = collection.find(eq("ID", 1))
	                    .projection(projectionFields)
	                    .first();

	            
	            if (doc == null) {
	                System.out.println("No results found.");
	            } else {
	                System.out.println(doc.toJson());
	            }
	        	
	            
	            
	            //UPDATE DE DOCUMENTOS
	            Bson updates = Updates.set("apellido", "Perez");
	            Document query = new Document().append("ID",  3);
	            
	            UpdateOptions options = new UpdateOptions().upsert(true);
	            
	            UpdateResult result = collection.updateOne(query, updates, options);
	            
	            System.out.println("Modified document count: " + result.getModifiedCount());
                System.out.println("Upserted id: " + result.getUpsertedId());         
	            
                
                System.out.println("Todos los documentos:");
	            MongoCursor<Document> cursor2 = collection.find().iterator();
	            while (cursor2.hasNext()) {
	                System.out.println(cursor2.next().toJson());
	            }
	            cursor.close();
	            
	            
	            //DELETE DOCUMENTOS
	            Bson queryDelete = eq("apellido", "Doe");
	            DeleteResult resultDelete = collection.deleteOne(queryDelete);
	            
	            System.out.println("Deleted document count: " + resultDelete.getDeletedCount());
	            
	            System.out.println("Todos los documentos:");
	            MongoCursor<Document> cursorDelete = collection.find().iterator();
	            while (cursorDelete.hasNext()) {
	                System.out.println(cursorDelete.next().toJson());
	            }
	            cursor.close();
	            
	            mongoClient.close();
	        
	    }

	

}
