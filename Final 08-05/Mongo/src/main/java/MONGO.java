import java.util.Arrays;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;

public class MONGO {

	private static final String DATABASE_NAME = "test";
	private static final String COLLECTION_NAME = "peliculas";
	private final MongoClient mongoClient;
	private final MongoDatabase database;

	
	public MONGO(String connectionString) { //Conexion BD
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(DATABASE_NAME);
    }
	
	
	public void insertarDatos() {
		MongoCollection<Document> peliculasCollection = database.getCollection(COLLECTION_NAME);
		Document fightClub = new Document("title", "Fight Club")
				.append("writer", "Chuck Palahniuk")
				.append("year",1999)
				.append("actors", Arrays.asList("Brad Pitt","Edward Norton"));
		
		Document pulpFiction = new Document("title", "Pulp Fiction")
				.append("writer", "Quentin Tarantino")
				.append("year", 1994)
				.append("actors", Arrays.asList("John Travolta", "Uma Thurman"));
		
		Document ingloriousBasterds = new Document("title", "Inglorious Basterds")
				.append("writer", "Quentin Tarantino")
				.append("year", 2009)
				.append("actors", Arrays.asList("Brad Pitt", "Diane Kruger", "Eli Roth"));
		
		Document theHobbit = new Document("title", "The Hobbit: An Unexpected Journey")
				.append("writer", "J.R.R Tolkien")
				.append("year", 2012);
				
		Document theHobbit2 = new Document("title", "The Hobbit: The Desolation of Smaug")
				.append("writer", "J.R.R Tolkien")
				.append("year", 2013);
		
		Document pee = new Document("title", "Pee Wee Herman´s Big Adventure");
		
		peliculasCollection.insertMany(Arrays.asList(fightClub, pulpFiction, ingloriousBasterds, theHobbit, theHobbit2, pee));
	}
	
	public void updateDocumentos() {
		MongoCollection<Document> peliculasCollection = database.getCollection(COLLECTION_NAME);
		
		peliculasCollection.updateOne(Filters.eq("title", "The Hobbit: An Unexpected Journey"), Updates.set("synopsis", "\"A reluctant hobbit, Bilbo Baggins, sets out to the Lonely Mountain with a spirited group of dwarves to reclaim their mountain home - and the gold within it - from the dragon Smaug."));
		peliculasCollection.updateOne(Filters.eq("title", "The Hobbit: The Desolation of Smaug"), Updates.set("synopsis", "The dwarves, along with Bilbo Baggins and Gandalf the Grey, continue their quest to reclaim Erebor, their homeland, from Smaug. Bilbo Baggins is in possession of a mysterious and magical ring."));
		peliculasCollection.updateOne(Filters.eq("title","Pulp Fiction"),Updates.addToSet("actors", "Samuel L. Jackson"));
		peliculasCollection.updateOne(Filters.eq("title","Pee Wee Herman´s Big Adventure"), Updates.set("actors","Tom McCarthy, Alex Ross Perry, Allison Schroeder"));
	}
	
	
	public void readAllDocs() {
		MongoCollection<Document> peliculas = database.getCollection(COLLECTION_NAME);
		//obtener todos los documentos
		System.out.println("Todos los documentos:");
		FindIterable<Document> collections = peliculas.find();
		//obtener todos los documentos show
		for(Document collection:collections) {
			System.out.println(collection.toJson());
		}
	}
	
	public void readDocuments() {
		MongoCollection<Document> peliculas = database.getCollection(COLLECTION_NAME);
			
		
		//QUENTIN TARANTINO
		System.out.println("Documentos con writer igual a 'Quentin Tarantino':");
		FindIterable<Document> quentin = peliculas.find(Filters.eq("writer","Quentin Tarantino"));
		for(Document document:quentin) {
			System.out.println(document.toJson());
		}
		
		//BRAD PITT
		System.out.println("Documentos con actors que incluyan a 'Brad Pitt':");
		FindIterable<Document> brad = peliculas.find(Filters.in("actors","Brad Pitt"));
		for(Document document:brad) {
			System.out.println(document.toJson());
		}
						
		//90´S
		System.out.println("Películas de los 90s:");
		FindIterable<Document> noventas = peliculas.find(Filters.and(Filters.gte("year",1990),Filters.lt("year", 2000)));
		for(Document document:noventas) {
			System.out.println(document.toJson());
		}
		
		//2000 Y 2010
		System.out.println("Películas estrenadas entre el año 2000 y 2010:");
		FindIterable<Document> dosmilDiez = peliculas.find(Filters.and(Filters.gte("year",2000),Filters.lt("year", 2011)));
		for(Document document:dosmilDiez) {
			System.out.println(document.toJson());
		}
	}
	
	
	public void deleteDocuments() {
		MongoCollection<Document> peliculas = database.getCollection(COLLECTION_NAME);
		//PEE WEE
		DeleteResult resultPee = peliculas.deleteOne(Filters.eq("title","Pee Wee Herman´s Big Adventure"));
		System.out.println("Se elimino Pee Wee Herman´s Big Adventure" + resultPee.getDeletedCount());
		
		//BRAD PITT
		DeleteResult  resultBrad = peliculas.deleteOne(Filters.in("actors","Brad Pitt"));
		System.out.println("Se eliminaron las peliculas que contienen a Brad Pitt de actor"+ resultBrad.getDeletedCount());
		
		//THE HOBBIT
		DeleteResult  resultTheHobbit = peliculas.deleteOne(Filters.in("title","The Hobbit"));
		System.out.println("Se eliminaron las peliculas que contienen The Hobbit"+ resultTheHobbit.getDeletedCount());
	
	}
	
	
	
	public static void main(String[] args) {
        MONGO mainMongoDB = new MONGO("mongodb://localhost:27017");
        //mainMongoDB.insertarDatos();
        mainMongoDB.updateDocumentos();
        mainMongoDB.readAllDocs();
        mainMongoDB.readDocuments();
        mainMongoDB.deleteDocuments();
        mainMongoDB.readAllDocs();
        
        mainMongoDB.mongoClient.close();
	}
	
	
	
}
