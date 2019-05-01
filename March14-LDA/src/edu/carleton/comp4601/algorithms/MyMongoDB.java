package edu.carleton.comp4601.algorithms;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
//import org.jgrapht.graph.DefaultEdge;
//import org.jgrapht.graph.DirectedMultigraph;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;

public class MyMongoDB {
	static String ID = "Identity";
	static String URL = "URL";
	static String LINKS = "Links";
	static String IMAGEs = "Images";
	static String TEXT = "Text";
	static String IMGMETA = "Image Metadata";
	static String PDFMETA = "PDF Metadata";
	static String DOCMETA = "Doc Metadata";
	static String TAGS = "Tags";
	static String SCORE = "Score";
	static String NAME = "Name";
	static String DATE = "Date";
	
	
	private static MyMongoDB instance;
	public static void setInstance(MyMongoDB instance){
		MyMongoDB.instance = instance;
	}
	public static MyMongoDB getInstance(){
		if (instance == null)
			instance = new MyMongoDB();
		return instance;
	}
	
	private MongoClient mongoClient;
	private MongoDatabase db;
	private MongoCollection<Document> coll;
	private MongoCollection<Document> collTopics;
	private MongoCollection<Document> collUsers;
	private MongoCollection<Document> collTerms;
	private MongoCursor<Document> cur;
	
	public MyMongoDB() {
		mongoClient = new MongoClient("localhost", 27017);
		db = mongoClient.getDatabase("a2");
		coll = db.getCollection("movies");
		collTopics = db.getCollection("topics"); 
		collUsers = db.getCollection("users");
		collTerms = db.getCollection("terms");
	}
	
	public MongoCollection<Document> getCollection(){
		return coll;
	}
	
	// make upsert version so with update or insert instead of arbitrarily inserting
	public void addFile(int id, String title, String topic) {
		Document doc = new Document(ID, id).append(NAME, title);
		Bson filter = null;
		Bson query = null;
		filter = Filters.eq("Identity",id);

	    coll.updateOne(filter, new Document("$set", doc),new UpdateOptions().upsert( true ));

	}
	
	public void updateFile(int id, String topic) {
		Document doc = new Document(ID, id).append("Topic", topic);
		Bson filter = null;
		Bson query = null;
		filter = Filters.eq("Identity",id);

	    coll.updateOne(filter, new Document("$set", doc),new UpdateOptions().upsert( true ));
	}
	
	public void updateMovieRating(String name, double rating) {
		Document doc = new Document("Name", name).append("Avg_Rating", rating);
		Bson filter = null;
		Bson query = null;
		filter = Filters.eq("Name",name);

	    coll.updateOne(filter, new Document("$set", doc),new UpdateOptions().upsert( true ));
	}
	

	
	public long getTopicCount() {
		long count = collTopics.count();
		
		return count;
	}
	
	
	
	public String getMovieTopic(String name) {
		BasicDBObject query = new BasicDBObject();

		query.put("Name", name);
		FindIterable<Document> t =  coll.find(query);
		String term = "";
		for (Document doc : t) {
			term = (String) doc.get("Topic");
		}
		System.out.println("Check inside mongo " + term + " name " + name);
		return term;

	}
	
	public FindIterable<Document> getTopFiveRatedMovies(String topic) {
		BasicDBObject query = new BasicDBObject();

		query.put("Topic", topic);
		FindIterable<Document> t =  coll.find(query).sort(new BasicDBObject("Avg_Rating", -1)).limit(5);

		return t;
	}
	
	@SuppressWarnings("unchecked")
	public ArrayList<String> getTopFiveForTopics(String topic) {
		BasicDBObject query = new BasicDBObject();

		query.put("Name", topic);
		FindIterable<Document> t =  collTopics.find(query);
		ArrayList<String> term = new ArrayList<String>();
		for (Document doc : t) {
			term = (ArrayList<String>) doc.get("TopFive");
		}
		return term;
	}
	
	public void addTerm(int id, String title) {
        
        Document doc = new Document(ID, id).append(NAME, title);
		Bson filter = null;
		Bson query = null;
		filter = Filters.eq("Identity",id);

	    collTerms.updateOne(filter, new Document("$set", doc),new UpdateOptions().upsert( true ));

	}

	
	public String getTerm(int id) {
		
		BasicDBObject query = new BasicDBObject();

		query.put("Identity", id);
		FindIterable<Document> t =  collTerms.find(query);
		String term = "";
		for (Document doc : t) {
			term = (String) doc.get("Name");
		}
		return term;

	}
	
	@SuppressWarnings("unchecked")
	public ArrayList<String> getCommunityUsers(String name) {
		
		BasicDBObject query = new BasicDBObject();

		query.put("Name", name);
		FindIterable<Document> t =  collTopics.find(query);
		ArrayList<String> term = new ArrayList<String>();
		for (Document doc : t) {
			term = (ArrayList<String>) doc.get("users");
		}
		return term;

	}
	
	public void addTopic(int id, String topic) {
		Document doc = new Document(ID, id).append(NAME, topic);

		Bson filter = null;
		Bson query = null;
		filter = Filters.eq("Identity",id);

		collTopics.updateOne(filter, new Document("$set", doc),new UpdateOptions().upsert( true ));
	}
	
	public String getTopic(int id) {
		
		BasicDBObject query = new BasicDBObject();

		query.put("Identity", id);
		FindIterable<Document> t =  collTopics.find(query);
		String term = "";
		for (Document doc : t) {
			term = (String) doc.get("Name");
		}
		return term;

	}
	
	public void updateTopicCommunityUsers(String name, ArrayList<String> users) {
		Document doc = new Document("Name", name).append("users", users);
		Bson filter = null;
		Bson query = null;
		filter = Filters.eq("Name",name);

		collTopics.updateOne(filter, new Document("$set", doc),new UpdateOptions().upsert( true ));
	}
	
	public void updateTopicTopFive(String name, ArrayList<String> movies) {
		Document doc = new Document("Name", name).append("TopFive", movies);
		Bson filter = null;
		Bson query = null;
		filter = Filters.eq("Name",name);

		collTopics.updateOne(filter, new Document("$set", doc),new UpdateOptions().upsert( true ));
	}
	
	public FindIterable<Document> getTopic(String name) {
		
		BasicDBObject query = new BasicDBObject();

		query.put("Name", name);
		FindIterable<Document> t =  collTopics.find(query);
//		String term = "";
//		for (Document doc : t) {
//			term = (String) doc.get("Name");
//		}
		return t;

	}
	
	public void updateUserTopic (String name, String topic, double rating) {
		Document doc = new Document("_id", name).append(topic, rating);
		Bson filter = null;
		Bson query = null;
		filter = Filters.eq("_id",name);

		collUsers.updateOne(filter, new Document("$set", doc),new UpdateOptions().upsert( true ));
	}
	
	public void updateUserCommunity (String name, String topic) {
		Document doc = new Document("_id", name).append("Community", topic);
		Bson filter = null;
		Bson query = null;
		filter = Filters.eq("_id",name);

		collUsers.updateOne(filter, new Document("$set", doc),new UpdateOptions().upsert( true ));
	}
	
	public long getUserCount() {
		long count = collUsers.count();
		
		return count;
	}
	
	public String getUserCommunity(String user) {
		BasicDBObject query = new BasicDBObject();

		query.put("_id", user);
		FindIterable<Document> t =  collUsers.find(query);
		String term = "";
		for (Document doc : t) {
			term = (String) doc.get("Community");
		}
		return term;
	}
	
	public double getUserTopicScore(String name, String topic) {
		BasicDBObject query = new BasicDBObject();

		query.put("_id", name);
		FindIterable<Document> t =  collUsers.find(query);
		double term = 0;
		for (Document doc : t) {
			term = (double) doc.get(topic);
		}
		return term;
		
		
	}
	
	public FindIterable<Document> getAllUsers() {

		FindIterable<Document> t =  collUsers.find();
		return t;
		
		
	}
	
	public FindIterable<Document> getAllTopics() {
		FindIterable<Document> t =  collTopics.find();
		return t;
	}

	
	public void insert(int id, String url) {
		Document doc = new Document(ID, id).append(URL, url);
        coll.insertOne(doc);
	}
	
	public void delete(Integer id){
		coll.deleteOne(new Document(ID, id));
	}
	
	public MongoCursor<Document> getCursor(){
		 cur = coll.find().iterator();
		 return cur;
	}

	
}
