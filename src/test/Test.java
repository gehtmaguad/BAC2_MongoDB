package test;

import java.math.BigInteger;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

public class Test {

	public static void main(String args[]) {
		// Server Details
		ArrayList<ServerAddress> addrs = new ArrayList<ServerAddress>();
		try {
			addrs.add(new ServerAddress("192.168.122.194", 27017));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// To connect to mongodb server
		MongoClient mongoClient = new MongoClient(addrs);
		// Set Write Concern
		mongoClient.setWriteConcern(WriteConcern.UNACKNOWLEDGED);
		
		// Now connect to your databases
		DB embedded = mongoClient.getDB("embedded");
		DB referenced = mongoClient.getDB("referenced");
		
		//executeInsertReferenced(referenced);
		executeInsertEmbedded(embedded);
	}

	public static String randomText(Integer bits) {

		SecureRandom random = new SecureRandom();
		String randomString = new BigInteger(bits, random).toString(32);
		return randomString;

	}

	public static int randomNumber(Integer min, Integer max) {

		Random rand = new Random();
		int randomNum = rand.nextInt((max - min) + 1) + min;
		return randomNum;
	}

	public static void executeInsertEmbedded(DB db) {
		try {

			// Helper Variable
			int count;
			int numberOfUsers = 1000;
			int numberOfBlogs = 1000;
			int numberOfComments = 1000;
			int numberOfLikes = 1000;

			// Get Collection
			DBCollection blogCollection = db.getCollection("Blog");
			DBCollection userCollection = db.getCollection("User");
			DBCollection commentCollection = db.getCollection("Comment");
			DBCollection likesCollection = db.getCollection("Likes");

			// Insert User
			for (count = 0; count < numberOfUsers; count++) {
				Map<String, Object> user = new HashMap<String, Object>();
				user.put("id", count + 1);
				user.put("vorname", randomText(100));
				user.put("nachname", randomText(150));
				user.put("email", randomText(120) + "@" + randomText(20) + "."
						+ randomText(10));
				userCollection.insert(new BasicDBObject(user));
			}

			// Insert Blog
			for (count = 0; count < numberOfBlogs; count++) {
				Map<String, Object> blog = new HashMap<String, Object>();
				Map<String, String> user = new HashMap<String, String>();
				blog.put("id", count + 1);
				blog.put("blogpost", randomText(5000));
				// get User Object
				user = selectUserById(db, randomNumber(1, numberOfUsers));
				blog.put("user_id", user.get("id"));
				blog.put("vorname", user.get("vorname"));
				blog.put("nachname", user.get("nachname"));
				blog.put("email", user.get("email"));
				blogCollection.insert(new BasicDBObject(blog));
			}

			// Insert Comment
			for (count = 0; count < numberOfComments; count++) {
				Map<String, Object> comment = new HashMap<String, Object>();
				Map<String, String> blog = new HashMap<String, String>();
				Map<String, String> user = new HashMap<String, String>();
				comment.put("id", count + 1);
				comment.put("text", randomText(2000));
				// get Blog Object
				blog = selectBlogById(db, randomNumber(1, numberOfBlogs));
				comment.put("blog_id", blog.get("id"));
				comment.put("blogpost", blog.get("blogpost"));
				// get User Object
				user = selectUserById(db, randomNumber(1, numberOfUsers));
				comment.put("user_id", user.get("id"));
				comment.put("vorname", user.get("vorname"));
				comment.put("nachname", user.get("nachname"));
				comment.put("email", user.get("email"));
				commentCollection.insert(new BasicDBObject(comment));
			}

			// Insert Likes
			for (count = 0; count < numberOfLikes; count++) {
				Map<String, Object> likes = new HashMap<String, Object>();
				Map<String, String> blog = new HashMap<String, String>();
				Map<String, String> comment = new HashMap<String, String>();
				Map<String, String> user = new HashMap<String, String>();
				likes.put("id", count + 1);
				// get User Object
				user = selectUserById(db, randomNumber(1, numberOfUsers));
				likes.put("user_id", user.get("id"));
				likes.put("vorname", user.get("vorname"));
				likes.put("nachname", user.get("nachname"));
				likes.put("email", user.get("email"));
				// get Comment Object
				comment = selectCommentById(db, randomNumber(1, numberOfComments));
				likes.put("comment_id", comment.get("id"));
				likes.put("text", comment.get("text"));
				// get Blog Object
				blog = selectBlogById(db, randomNumber(1, numberOfBlogs));
				likes.put("blog_id", blog.get("id"));
				likes.put("blogpost", blog.get("blogpost"));
				likesCollection.insert(new BasicDBObject(likes));
			}

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	public static void executeInsertReferenced(DB db) {

		try {
			// Helper Variable
			int count;
			int numberOfUsers = 1000;
			int numberOfBlogs = 1000;
			int numberOfComments = 1000;
			int numberOfLikes = 1000;

			// Get Collection
			DBCollection blogCollection = db.getCollection("Blog");
			DBCollection userCollection = db.getCollection("User");
			DBCollection commentCollection = db.getCollection("Comment");
			DBCollection likesCollection = db.getCollection("Likes");

			// Insert User
			for (count = 0; count < numberOfUsers; count++) {
				Map<String, Object> user = new HashMap<String, Object>();
				user.put("id", count + 1);
				user.put("vorname", randomText(100));
				user.put("nachname", randomText(150));
				user.put("email", randomText(120) + "@" + randomText(20) + "."
						+ randomText(10));
				userCollection.insert(new BasicDBObject(user));
			}

			// Insert Blog
			for (count = 0; count < numberOfBlogs; count++) {
				Map<String, Object> blog = new HashMap<String, Object>();
				blog.put("id", count + 1);
				blog.put("blogpost", randomText(5000));
				blog.put("user_id", randomNumber(1, numberOfUsers));
				blogCollection.insert(new BasicDBObject(blog));
			}

			// Insert Comment
			for (count = 0; count < numberOfComments; count++) {
				Map<String, Object> comment = new HashMap<String, Object>();
				comment.put("id", count + 1);
				comment.put("text", randomText(2000));
				comment.put("user_id", randomNumber(1, numberOfUsers));
				comment.put("blog_id", randomNumber(1, numberOfBlogs));
				commentCollection.insert(new BasicDBObject(comment));
			}

			// Insert Likes
			for (count = 0; count < numberOfLikes; count++) {
				Map<String, Object> likes = new HashMap<String, Object>();
				likes.put("id", count + 1);
				likes.put("comment_id", randomNumber(1, numberOfComments));
				likes.put("user_id", randomNumber(1, numberOfUsers));
				likesCollection.insert(new BasicDBObject(likes));
			}

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	public static Map<String, String> selectUserById(DB db, int id) {
		
		Map<String, String> resultSet = null;

		try {
			// Get Collection
			DBCollection userCollection = db.getCollection("User");

			// Make Query
			BasicDBObject userQuery = new BasicDBObject("id", id);
			DBCursor userCursor = userCollection.find(userQuery);
			try {
				while (userCursor.hasNext()) {
					BasicDBObject user = (BasicDBObject) userCursor.next();
					
					// Parse Result Set
					resultSet = new HashMap<String, String>();
					resultSet.put("id", user.getString("id") );
					resultSet.put("vorname", user.getString("vorname") );
					resultSet.put("nachname", user.getString("nachname") );
					resultSet.put("email", user.getString("email") );
				}
			} finally {
				userCursor.close();
			}
			
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}

		return resultSet;
	}

	public static Map<String, String> selectBlogById(DB db, int id) {
		
		Map<String, String> resultSet = null;

		try {
			// Get Collection
			DBCollection blogCollection = db.getCollection("Blog");

			// Make Query
			BasicDBObject blogQuery = new BasicDBObject("id", id);
			DBCursor blogCursor = blogCollection.find(blogQuery);
			try {
				while (blogCursor.hasNext()) {
					BasicDBObject blog = (BasicDBObject) blogCursor.next();
					
					// Parse Result Set
					resultSet = new HashMap<String, String>();
					resultSet.put("id", blog.getString("id") );
					resultSet.put("blogpost", blog.getString("blogpost") );
					resultSet.put("user_id", blog.getString("user_id") );
					resultSet.put("vorname", blog.getString("vorname") );
					resultSet.put("nachname", blog.getString("nachname") );
					resultSet.put("email", blog.getString("email") );
				}
			} finally {
				blogCursor.close();
			}
			
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}

		return resultSet;
	}

	public static Map<String, String> selectCommentById(DB db, int id) {
		
		Map<String, String> resultSet = null;

		try {
			// Get Collection
			DBCollection blogCollection = db.getCollection("Comment");

			// Make Query
			BasicDBObject blogQuery = new BasicDBObject("id", id);
			DBCursor blogCursor = blogCollection.find(blogQuery);
			try {
				while (blogCursor.hasNext()) {
					BasicDBObject blog = (BasicDBObject) blogCursor.next();
					
					// Parse Result Set
					resultSet = new HashMap<String, String>();
					resultSet.put("id", blog.getString("id") );
					resultSet.put("text", blog.getString("text") );
					resultSet.put("user_id", blog.getString("user_id") );
					resultSet.put("vorname", blog.getString("vorname") );
					resultSet.put("nachname", blog.getString("nachname") );
					resultSet.put("email", blog.getString("email") );
					resultSet.put("blog_id", blog.getString("blog_id") );
					resultSet.put("blogpost", blog.getString("blogpost") );
				}
			} finally {
				blogCursor.close();
			}
			
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}

		return resultSet;
	}
	
	public static void select() {
		try {

			ArrayList<ServerAddress> addrs = new ArrayList<ServerAddress>();
			addrs.add(new ServerAddress("192.168.122.194", 27017));

			// To connect to mongodb server
			MongoClient mongoClient = new MongoClient(addrs);

			// Now connect to your databases
			DB db = mongoClient.getDB("referenced");
			System.out.println("Connect to database successfully");

			// Get Collection
			DBCollection blogCollection = db.getCollection("Blog");
			DBCollection userCollection = db.getCollection("User");

			DBCursor blogCursor = blogCollection.find();
			try {
				while (blogCursor.hasNext()) {
					BasicDBObject blog = (BasicDBObject) blogCursor.next();
					System.out.println(blog);

					int user_id = ((Number) blog.get("user_id")).intValue();
					BasicDBObject userQuery = new BasicDBObject("user_id",
							user_id);
					DBCursor userCursor = userCollection.find(userQuery);

					try {
						while (userCursor.hasNext()) {
							BasicDBObject user = (BasicDBObject) userCursor
									.next();
							System.out.println("    " + user);
						}
					} finally {
						userCursor.close();
					}

				}
			} finally {
				blogCursor.close();
			}

			// Setting Write Concern
			mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);

			// // Create JSON Object
			// BasicDBObject doc = new BasicDBObject("name", "MongoDB")
			// .append("type", "database")
			// .append("count", 1)
			// .append("info",
			// new BasicDBObject("x", 203).append("y", 102));
			//
			// // Insert JSON OBject to DB
			// coll.insert(doc);

			// long startTime = System.nanoTime();
			// // Insert multiple simple JSON OBjects
			// for (int i = 0; i < 10000; i++) {
			// coll.insert(new BasicDBObject("i", i));
			// }
			// long estimatedTime = System.nanoTime() - startTime;
			// double seconds = (double) estimatedTime / 1000000000.0;
			// System.out.println(seconds);

			// Count Objects and print count
			// System.out.println(coll.getCount());

			// Get All the Documents (with a DB Cursor)
			// DBCursor cursor = coll.find();
			// try {
			// while(cursor.hasNext()) {
			// System.out.println(cursor.next());
			// }
			// } finally {
			// cursor.close();
			// }

			// // Drop Collection
			// coll.drop();

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}
}
