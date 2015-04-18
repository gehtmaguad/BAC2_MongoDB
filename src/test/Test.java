package test;

import java.math.BigInteger;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

public class Test {

	public static int numberOfUsers = 90000;
	public static int numberOfBlogs = 90000;
	public static int numberOfComments = 90000;
	public static int numberOfLikes = 90000;

	public static void main(String args[]) {

		// Connection Details
		MongoClient mongoClient = connectToDB("192.168.122.194");
		DB embedded = mongoClient.getDB("referenced");
		// DB referenced = mongoClient.getDB("referenced");

		// executeInsertReferenced(referenced);

		// // Tag Top Blogger
		// long startTime = System.nanoTime();
		// HashMap<String, String> result = tagTopBloggerReferenced(referenced);
		// long estimatedTime = System.nanoTime() - startTime;
		// double seconds = (double) estimatedTime / 1000000000.0;
		// System.out.println(result);
		// System.out.println("Duration: " + seconds);

		// Loop through Referenced Documents
		// long startTime = System.nanoTime();
		// ArrayList<HashMap<String, String>> result =
		// selectBlogWithAssociatesReferenced(referenced);
		// long estimatedTime = System.nanoTime() - startTime;
		// double seconds = (double) estimatedTime / 1000000000.0;
		// System.out.println(result);
		// System.out.println("Duration: " + seconds);

		// // Get one merged Document from Referenced Documents
		// long startTime = System.nanoTime();
		// ArrayList<HashMap<String, String>> result =
		// selectBlogWithAssociatesReferencedSingle(referenced, 25);
		// long estimatedTime = System.nanoTime() - startTime;
		// double seconds = (double) estimatedTime / 1000000000.0;
		// System.out.println(result);
		// System.out.println("Duration: " + seconds);

		// executeInsertEmbeddedWithNewUser(embedded);

		// // Tag Top Blogger
		// long startTime = System.nanoTime();
		// HashMap<String, String> result = tagTopBloggerEmbedded(embedded);
		// long estimatedTime = System.nanoTime() - startTime;
		// double seconds = (double) estimatedTime / 1000000000.0;
		// System.out.println(result);
		// System.out.println("Duration: " + seconds);

		// Loop through Embedded Documents
		// long startTime = System.nanoTime();
		// ArrayList<HashMap<String, String>> result =
		// selectBlogWithAssociatesEmbedded(embedded);
		// long estimatedTime = System.nanoTime() - startTime;
		// double seconds = (double) estimatedTime / 1000000000.0;
		// System.out.println(result);
		// System.out.println("Duration: " + seconds);

		// // Get one Document from Embedded Documents
		// long startTime = System.nanoTime();
		// ArrayList<HashMap<String, String>> result =
		// selectBlogWithAssociatesEmbeddedSingle(embedded, 222);
		// long estimatedTime = System.nanoTime() - startTime;
		// double seconds = (double) estimatedTime / 1000000000.0;
		// System.out.println(result);
		// System.out.println("Duration: " + seconds);

		// executeInsertUserWithItem(embedded);

		// // Move Item with Transaction
		// long startTime = System.nanoTime();
		// HashMap<String, String> result =
		// moveItemBetweenUserEmbeddedTransaction(embedded);
		// long estimatedTime = System.nanoTime() - startTime;
		// double seconds = (double) estimatedTime / 1000000000.0;
		// System.out.println(result);
		// System.out.println("Duration: " + seconds);

		// // Move Item without Transaction
		// long startTime = System.nanoTime();
		// HashMap<String, String> result =
		// moveItemBetweenUserEmbedded(embedded);
		// long estimatedTime = System.nanoTime() - startTime;
		// double seconds = (double) estimatedTime / 1000000000.0;
		// System.out.println(result);
		// System.out.println("Duration: " + seconds);

	}

	public static MongoClient connectToDB(String ip) {

		// Server Details
		ArrayList<ServerAddress> addrs = new ArrayList<ServerAddress>();
		try {
			addrs.add(new ServerAddress(ip, 27017));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		// To connect to mongodb server
		MongoClient mongoClient = new MongoClient(addrs);
		// Set Write Concern
		mongoClient.setWriteConcern(WriteConcern.UNACKNOWLEDGED);

		return mongoClient;
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

	public static void executeInsertEmbeddedWithNewUser(DB db) {
		try {

			// Helper Variable
			int count;

			// Get Collection
			DBCollection blogCollection = db.getCollection("Blog");
			DBCollection userCollection = db.getCollection("User");

			// // Insert User
			// for (count = 0; count < numberOfUsers; count++) {
			// Map<String, Object> user = new HashMap<String, Object>();
			// user.put("id", count + 1);
			// user.put("vorname", randomText(100));
			// user.put("nachname", randomText(150));
			// user.put("email", randomText(120) + "@" + randomText(20) + "."
			// + randomText(10));
			// userCollection.insert(new BasicDBObject(user));
			// }

			// Insert Comment
			int b;
			int c;
			int l;
			for (b = 1; b <= numberOfBlogs; b++) {

				Map<String, String> user = new HashMap<String, String>();

				BasicDBObject blog = new BasicDBObject();
				blog.put("id", b);
				blog.put("blogpost", randomText(5000));
				blog.put("user_id", b);
				blog.put("vorname", randomText(100));
				blog.put("nachname", randomText(150));
				blog.put("email", randomText(120) + "@" + randomText(20) + "."
						+ randomText(10));

				for (c = 1; c <= 3; c++) {

					BasicDBObject comment = new BasicDBObject();
					comment.put("id", c);
					comment.put("comment", randomText(2000));
					comment.put("user_id", c);
					comment.put("vorname", randomText(100));
					comment.put("nachname", randomText(150));
					comment.put("email", randomText(120) + "@" + randomText(20)
							+ "." + randomText(10));

					ArrayList<BasicDBObject> myList = new ArrayList<BasicDBObject>();
					for (l = 1; l <= 3; l++) {

						BasicDBObject like = new BasicDBObject();
						like.put("user_id", l);
						like.put("vorname", randomText(100));
						like.put("nachname", randomText(150));
						like.put("email", randomText(120) + "@"
								+ randomText(20) + "." + randomText(10));
						myList.add(like);
					}
					comment.put("Likes", myList);

					BasicDBObject update = new BasicDBObject();
					update.put("$push", new BasicDBObject("Comment", comment));
					blogCollection.update(blog, update, true, true);
				}

			}

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	public static void executeInsertEmbeddedWithExistingUser(DB db) {

		try {

			// Helper Variable
			int count;

			// Get Collection
			DBCollection blogCollection = db.getCollection("Blog");
			DBCollection userCollection = db.getCollection("User");

			// // Insert User
			// for (count = 0; count < numberOfUsers; count++) {
			// Map<String, Object> user = new HashMap<String, Object>();
			// user.put("id", count + 1);
			// user.put("vorname", randomText(100));
			// user.put("nachname", randomText(150));
			// user.put("email", randomText(120) + "@" + randomText(20) + "."
			// + randomText(10));
			// userCollection.insert(new BasicDBObject(user));
			// }

			// Insert Comment
			int b;
			int c;
			int l;
			for (b = 1; b <= numberOfBlogs; b++) {

				Map<String, String> user = new HashMap<String, String>();
				user = selectUserById(db, randomNumber(1, numberOfUsers));

				BasicDBObject blog = new BasicDBObject();
				blog.put("id", b);
				blog.put("blogpost", randomText(5000));
				blog.put("user_id", user.get("id"));
				blog.put("vorname", user.get("vorname"));
				blog.put("nachname", user.get("nachname"));
				blog.put("email", user.get("email"));

				for (c = 1; c <= 3; c++) {

					BasicDBObject comment = new BasicDBObject();
					user = selectUserById(db, randomNumber(1, numberOfUsers));
					comment.put("id", c);
					comment.put("comment", randomText(2000));
					comment.put("user_id", user.get("id"));
					comment.put("vorname", user.get("vorname"));
					comment.put("nachname", user.get("nachname"));
					comment.put("email", user.get("email"));

					ArrayList<BasicDBObject> myList = new ArrayList<BasicDBObject>();
					for (l = 1; l <= 3; l++) {

						BasicDBObject like = new BasicDBObject();
						user = selectUserById(db,
								randomNumber(1, numberOfUsers));
						like.put("user_id", user.get("id"));
						like.put("vorname", user.get("vorname"));
						like.put("nachname", user.get("nachname"));
						like.put("email", user.get("email"));
						myList.add(like);
					}
					comment.put("Likes", myList);

					BasicDBObject update = new BasicDBObject();
					update.put("$push", new BasicDBObject("Comment", comment));
					blogCollection.update(blog, update, true, true);
				}

			}

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	public static void executeInsertUserWithItem(DB db) {

		try {

			// Helper Variable
			int count;

			// Get Collection
			DBCollection userCollection = db.getCollection("User");

			// Insert User
			List<String> items = new ArrayList<String>();
			items.add("laser");
			items.add("sword");
			for (count = 0; count < numberOfUsers; count++) {
				Map<String, Object> user = new HashMap<String, Object>();
				user.put("id", count + 1);
				user.put("vorname", randomText(100));
				user.put("nachname", randomText(150));
				user.put("email", randomText(120) + "@" + randomText(20) + "."
						+ randomText(10));
				user.put("item", items);
				userCollection.insert(new BasicDBObject(user));
			}

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	public static void executeInsertReferenced(DB db) {

		try {
			// Helper Variable
			int count;

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

	// Helper Method
	private static Map<String, String> selectUserById(DB db, int id) {

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
					resultSet.put("id", user.getString("id"));
					resultSet.put("vorname", user.getString("vorname"));
					resultSet.put("nachname", user.getString("nachname"));
					resultSet.put("email", user.getString("email"));
				}
			} finally {
				userCursor.close();
			}

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}

		return resultSet;
	}

	// Helper Method
	private static Map<String, String> selectBlogById(DB db, int id) {

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
					resultSet.put("id", blog.getString("id"));
					resultSet.put("blogpost", blog.getString("blogpost"));
					resultSet.put("user_id", blog.getString("user_id"));
					resultSet.put("vorname", blog.getString("vorname"));
					resultSet.put("nachname", blog.getString("nachname"));
					resultSet.put("email", blog.getString("email"));
				}
			} finally {
				blogCursor.close();
			}

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}

		return resultSet;
	}

	// Helper Method
	private static Map<String, String> selectCommentById(DB db, int id) {

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
					resultSet.put("id", blog.getString("id"));
					resultSet.put("text", blog.getString("text"));
					resultSet.put("user_id", blog.getString("user_id"));
					resultSet.put("vorname", blog.getString("vorname"));
					resultSet.put("nachname", blog.getString("nachname"));
					resultSet.put("email", blog.getString("email"));
					resultSet.put("blog_id", blog.getString("blog_id"));
					resultSet.put("blogpost", blog.getString("blogpost"));
				}
			} finally {
				blogCursor.close();
			}

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}

		return resultSet;
	}

	public static ArrayList<HashMap<String, String>> selectBlogWithAssociatesEmbedded(
			DB db) {

		ArrayList<HashMap<String, String>> result = new ArrayList<HashMap<String, String>>();

		try {
			// Get Collection
			DBCollection blogCollection = db.getCollection("Blog");

			// Make Query
			int i;
			for (i = 1; i <= numberOfBlogs; i++) {

				BasicDBObject blogQuery = new BasicDBObject("id", i);
				DBCursor blogCursor = blogCollection.find(blogQuery);
				try {

					// Loop through Result and build Result Set
					HashMap<String, String> resultSet = new HashMap<String, String>();
					while (blogCursor.hasNext()) {
						BasicDBObject blog = (BasicDBObject) blogCursor.next();
						resultSet.put("id", blog.getString("id"));
					}

					result.add(resultSet);

				} finally {
					blogCursor.close();
				}
			}
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}

		return result;
	}

	public static ArrayList<HashMap<String, String>> selectBlogWithAssociatesEmbeddedSingle(
			DB db, Integer id) {

		ArrayList<HashMap<String, String>> result = new ArrayList<HashMap<String, String>>();

		try {
			// Get Collection
			DBCollection blogCollection = db.getCollection("Blog");

			BasicDBObject blogQuery = new BasicDBObject("id", id);
			DBCursor blogCursor = blogCollection.find(blogQuery);
			try {

				// Loop through Result and build Result Set
				HashMap<String, String> resultSet = new HashMap<String, String>();
				while (blogCursor.hasNext()) {
					BasicDBObject blog = (BasicDBObject) blogCursor.next();
					resultSet.put("id", blog.getString("id"));
				}

				result.add(resultSet);

			} finally {
				blogCursor.close();
			}

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}

		return result;
	}

	public static ArrayList<HashMap<String, String>> selectBlogWithAssociatesReferenced(
			DB db) {

		ArrayList<HashMap<String, String>> result = new ArrayList<HashMap<String, String>>();

		try {
			// Get Collection
			DBCollection blogCollection = db.getCollection("Blog");
			DBCollection commentCollection = db.getCollection("Comment");
			DBCollection userCollection = db.getCollection("User");
			DBCollection likesCollection = db.getCollection("Likes");

			// Make Query
			int i;
			for (i = 1; i <= numberOfBlogs; i++) {

				// Get Blog
				BasicDBObject blogQuery = new BasicDBObject("id", i);
				DBCursor blogCursor = blogCollection.find(blogQuery);
				try {

					// Loop through Result and build Result Set
					HashMap<String, String> resultSet = new HashMap<String, String>();
					while (blogCursor.hasNext()) {
						BasicDBObject blog = (BasicDBObject) blogCursor.next();
						// System.out.println(" Blog " + blog);

						// Get Author from Blog
						BasicDBObject userBlogQery = new BasicDBObject("id",
								blog.getInt("user_id"));
						DBCursor userBlogCursor = userCollection
								.find(userBlogQery);

						while (userBlogCursor.hasNext()) {
							BasicDBObject userBlog = (BasicDBObject) userBlogCursor
									.next();
							// System.out.println(" BlogUser    " + userBlog);
						}

						// Get Comments
						BasicDBObject commentQuery = new BasicDBObject(
								"blog_id", i);
						DBCursor commentCursor = commentCollection
								.find(commentQuery);

						while (commentCursor.hasNext()) {
							BasicDBObject comment = (BasicDBObject) commentCursor
									.next();
							// System.out.println("    Comment   " + comment);

							// Get Author from Comment
							BasicDBObject userCommentQery = new BasicDBObject(
									"id", comment.getInt("user_id"));
							DBCursor userCommentCursor = userCollection
									.find(userCommentQery);

							while (userCommentCursor.hasNext()) {
								BasicDBObject user = (BasicDBObject) userCommentCursor
										.next();
								// System.out.println("   CommentUser    " +
								// user);
							}

							// Get Likes from Comment
							BasicDBObject likesQery = new BasicDBObject(
									"comment_id", comment.getInt("id"));
							DBCursor likesCursor = likesCollection
									.find(likesQery);

							while (likesCursor.hasNext()) {
								BasicDBObject like = (BasicDBObject) likesCursor
										.next();
								// System.out.println("         LIKE    " +
								// like);

								// Get Author from Like
								BasicDBObject userLikeQuery = new BasicDBObject(
										"id", like.getInt("user_id"));
								DBCursor userLikeCursor = userCollection
										.find(userLikeQuery);

								while (userLikeCursor.hasNext()) {
									BasicDBObject user = (BasicDBObject) userLikeCursor
											.next();
									// System.out.println("        LikeUser    "
									// + user);
								}
							}

						}
						resultSet.put("id", blog.getString("id"));

					}

					result.add(resultSet);

				} finally {
					blogCursor.close();
				}
			}

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}

		return result;
	}

	public static ArrayList<HashMap<String, String>> selectBlogWithAssociatesReferencedSingle(
			DB db, Integer id) {

		ArrayList<HashMap<String, String>> result = new ArrayList<HashMap<String, String>>();

		try {
			// Get Collection
			DBCollection blogCollection = db.getCollection("Blog");
			DBCollection commentCollection = db.getCollection("Comment");
			DBCollection userCollection = db.getCollection("User");
			DBCollection likesCollection = db.getCollection("Likes");

			// Get Blog
			BasicDBObject blogQuery = new BasicDBObject("id", id);
			DBCursor blogCursor = blogCollection.find(blogQuery);
			try {

				// Loop through Result and build Result Set
				HashMap<String, String> resultSet = new HashMap<String, String>();
				while (blogCursor.hasNext()) {
					BasicDBObject blog = (BasicDBObject) blogCursor.next();
					// System.out.println(" Blog " + blog);

					// Get Author from Blog
					BasicDBObject userBlogQery = new BasicDBObject("id",
							blog.getInt("user_id"));
					DBCursor userBlogCursor = userCollection.find(userBlogQery);

					while (userBlogCursor.hasNext()) {
						BasicDBObject userBlog = (BasicDBObject) userBlogCursor
								.next();
						// System.out.println(" BlogUser    " + userBlog);
					}

					// Get Comments
					BasicDBObject commentQuery = new BasicDBObject("blog_id",
							id);
					DBCursor commentCursor = commentCollection
							.find(commentQuery);

					while (commentCursor.hasNext()) {
						BasicDBObject comment = (BasicDBObject) commentCursor
								.next();
						// System.out.println("    Comment   " + comment);

						// Get Author from Comment
						BasicDBObject userCommentQery = new BasicDBObject("id",
								comment.getInt("user_id"));
						DBCursor userCommentCursor = userCollection
								.find(userCommentQery);

						while (userCommentCursor.hasNext()) {
							BasicDBObject user = (BasicDBObject) userCommentCursor
									.next();
							// System.out.println("   CommentUser    " +
							// user);
						}

						// Get Likes from Comment
						BasicDBObject likesQery = new BasicDBObject(
								"comment_id", comment.getInt("id"));
						DBCursor likesCursor = likesCollection.find(likesQery);

						while (likesCursor.hasNext()) {
							BasicDBObject like = (BasicDBObject) likesCursor
									.next();
							// System.out.println("         LIKE    " +
							// like);

							// Get Author from Like
							BasicDBObject userLikeQuery = new BasicDBObject(
									"id", like.getInt("user_id"));
							DBCursor userLikeCursor = userCollection
									.find(userLikeQuery);

							while (userLikeCursor.hasNext()) {
								BasicDBObject user = (BasicDBObject) userLikeCursor
										.next();
								// System.out.println("        LikeUser    "
								// + user);
							}
						}

					}
					resultSet.put("id", blog.getString("id"));

				}

				result.add(resultSet);

			} finally {
				blogCursor.close();
			}

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}

		return result;
	}

	public static HashMap<String, String> tagTopBloggerEmbedded(DB db) {

		// Result Helper
		HashMap<String, String> result = new HashMap<String, String>();

		// Get Collection
		DBCollection blogCollection = db.getCollection("Blog");

		// Create Group Field Object
		DBObject groupFields = new BasicDBObject("_id", "$user_id");
		groupFields.put("sum", new BasicDBObject("$sum", 1));
		DBObject group = new BasicDBObject("$group", groupFields);

		// Create Sort Field Object
		DBObject sortFields = new BasicDBObject("sum", -1);
		sortFields.put("_id", 1);
		DBObject sort = new BasicDBObject("$sort", sortFields);

		// Create Limit Field Object
		DBObject limit = new BasicDBObject("$limit", 1);

		// Create Project Field Object
		DBObject projectFields = new BasicDBObject("_id", 1);
		DBObject project = new BasicDBObject("$project", projectFields);

		// Execute Aggregation
		List<DBObject> pipeline = Arrays.asList(group, sort, limit, project);
		AggregationOutput output = blogCollection.aggregate(pipeline);

		// Loop through Result From Aggregation
		for (DBObject dbobject : output.results()) {
			result.put("id", dbobject.get("_id").toString());

			// Update User in Blog Dokument
			blogCollection.updateMulti(
					new BasicDBObject("user_id", dbobject.get("_id")),
					new BasicDBObject("$set",
							new BasicDBObject("topBlogger", 1)));

			// Update User in Subdokument Comment
			blogCollection.updateMulti(new BasicDBObject("Comment.user_id",
					dbobject.get("_id")), new BasicDBObject("$set",
					new BasicDBObject("Comment.$.topBlogger", 1)));

			// Update User in Subdokument Likes
			DBCursor userCursor = blogCollection.find(new BasicDBObject(
					"Comment.Likes.user_id", dbobject.get("_id")));
			try {
				while (userCursor.hasNext()) {
					BasicDBObject user = (BasicDBObject) userCursor.next();

					// TODO: APPLICATION CODE TO GET BLOG AND COMMENT ID

					blogCollection.updateMulti(new BasicDBObject(
							"Comment.Likes.user_id", dbobject.get("_id")),
							new BasicDBObject("$set", new BasicDBObject(
									"Comment.0.Likes.$.topBlogger", 1)));
				}
			} finally {
				userCursor.close();
			}

		}

		return result;

	}

	public static HashMap<String, String> tagTopBloggerReferenced(DB db) {

		// Result Helper
		HashMap<String, String> result = new HashMap<String, String>();

		// Get Collection
		DBCollection blogCollection = db.getCollection("Blog");
		DBCollection userCollection = db.getCollection("User");

		// Create Group Field Object
		DBObject groupFields = new BasicDBObject("_id", "$user_id");
		groupFields.put("sum", new BasicDBObject("$sum", 1));
		DBObject group = new BasicDBObject("$group", groupFields);

		// Create Sort Field Object
		DBObject sortFields = new BasicDBObject("sum", -1);
		sortFields.put("_id", 1);
		DBObject sort = new BasicDBObject("$sort", sortFields);

		// Create Limit Field Object
		DBObject limit = new BasicDBObject("$limit", 1);

		// Create Project Field Object
		DBObject projectFields = new BasicDBObject("_id", 1);
		DBObject project = new BasicDBObject("$project", projectFields);

		// Execute Aggregation
		List<DBObject> pipeline = Arrays.asList(group, sort, limit, project);
		AggregationOutput output = blogCollection.aggregate(pipeline);

		// Loop through Result From Aggregation
		for (DBObject dbobject : output.results()) {
			result.put("id", dbobject.get("_id").toString());

			// Update User in Blog Dokument
			userCollection.update(new BasicDBObject("id", dbobject.get("_id")),
					new BasicDBObject("$set",
							new BasicDBObject("topBlogger", 1)));

		}

		return result;

	}

	public static HashMap<String, String> moveItemBetweenUserEmbeddedTransaction(
			DB db) {

		// Helper
		HashMap<String, String> result = new HashMap<String, String>();
		Integer id = null;
		Integer from = null;
		Integer to = null;
		String item = null;

		// Get Collection
		DBCollection userCollection = db.getCollection("User");
		DBCollection transactionCollection = db.getCollection("Transaction");

		// Create Transaction Object and set state to INIT
		Map<String, Object> transaction = new HashMap<String, Object>();
		transaction.put("id", 1);
		transaction.put("from", 1);
		transaction.put("to", 2);
		transaction.put("item", "sword");
		transaction.put("state", "init");
		transactionCollection.insert(new BasicDBObject(transaction));

		BasicDBObject transactionQuery = new BasicDBObject("id", 1);
		DBCursor transactionCursor = transactionCollection
				.find(transactionQuery);
		try {
			while (transactionCursor.hasNext()) {
				BasicDBObject transactionObject = (BasicDBObject) transactionCursor
						.next();

				// Parse Result Set
				id = transactionObject.getInt("id");
				from = transactionObject.getInt("from");
				to = transactionObject.getInt("to");
				item = transactionObject.getString("item");

			}
		} finally {
			transactionCursor.close();
		}

		// Set State to PENDING
		transactionQuery = new BasicDBObject("id", id);
		DBObject transactionUpdate = new BasicDBObject();
		transactionUpdate.put("$set", new BasicDBObject("state", "pending"));
		transactionCollection.update(transactionQuery, transactionUpdate);

		// Update First Document and Set Transaction to 1
		BasicDBObject userQuery = new BasicDBObject("id", from);
		DBObject userUpdate = new BasicDBObject();
		userUpdate.put("$pull", new BasicDBObject("item", item));
		userUpdate.put("$set", new BasicDBObject("transaction", 1));
		userCollection.update(userQuery, userUpdate);

		// Update Second Document and Set Transaction to 1
		userQuery = new BasicDBObject("id", to);
		userUpdate = new BasicDBObject();
		userUpdate.put("$push", new BasicDBObject("item", item));
		userUpdate.put("$set", new BasicDBObject("transaction", 1));
		userCollection.update(userQuery, userUpdate);

		// Set State to COMMITTED
		transactionQuery = new BasicDBObject("id", id);
		transactionUpdate = new BasicDBObject();
		transactionUpdate.put("$set", new BasicDBObject("state", "committed"));
		transactionCollection.update(transactionQuery, transactionUpdate);

		// Update First Document and Set Transaction to 0
		userQuery = new BasicDBObject("id", from);
		userUpdate = new BasicDBObject();
		userUpdate.put("$set", new BasicDBObject("transaction", 0));
		userCollection.update(userQuery, userUpdate);

		// Update Second Document and Set Transaction to 0
		userQuery = new BasicDBObject("id", to);
		userUpdate = new BasicDBObject();
		userUpdate.put("$set", new BasicDBObject("transaction", 0));
		userCollection.update(userQuery, userUpdate);

		// Retrieve Transaction Object and set State to DONE
		transactionQuery = new BasicDBObject("id", id);
		transactionUpdate = new BasicDBObject();
		transactionUpdate.put("$set", new BasicDBObject("state", "done"));
		transactionCollection.update(transactionQuery, transactionUpdate);

		// Delete Transaction Object
		transactionQuery = new BasicDBObject("id", id);
		transactionCollection.remove(transactionQuery);

		return result;

	}

	public static HashMap<String, String> moveItemBetweenUserEmbedded(DB db) {

		// Helper
		HashMap<String, String> result = new HashMap<String, String>();
		Integer from = 1;
		Integer to = 2;
		String item = "laser";

		// Get Collection
		DBCollection userCollection = db.getCollection("User");
		DBCollection transactionCollection = db.getCollection("Transaction");

		// Update First Document
		BasicDBObject userQuery = new BasicDBObject("id", from);
		DBObject userUpdate = new BasicDBObject();
		userUpdate.put("$pull", new BasicDBObject("item", item));
		userCollection.update(userQuery, userUpdate);

		// Update Second Document
		userQuery = new BasicDBObject("id", to);
		userUpdate = new BasicDBObject();
		userUpdate.put("$push", new BasicDBObject("item", item));
		userCollection.update(userQuery, userUpdate);

		return result;

	}

}
