package project;

import java.io.File;
import java.rmi.NoSuchObjectException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;


/**
 * 
 * create graph database and add node into graph database
 * 
 * @author Yan
 *
 */
public class GraphDatabase {

	private String[] hashtagList = CreateDatabase.hashtagList;

	private String[] locations = CreateDatabase.locations;

	// Declare database path
	public static final String DB_PATH = "MovieDatabaseM1Nov30UntilDec3";
	private GraphDatabaseService graphDb;
	// Declare labels
	private ArrayList<Label> hashtagLabel = new ArrayList<Label>();
	private ArrayList<Label> locationLabel = new ArrayList<Label>();
	// Declare indexes
	private ArrayList<IndexDefinition> tweetidIndex = new ArrayList<IndexDefinition>();
	private ArrayList<IndexDefinition> useridIndex = new ArrayList<IndexDefinition>();

	private ArrayList<IndexDefinition> tweetnumIndex = new ArrayList<IndexDefinition>();
	private ArrayList<IndexDefinition> usernumIndex = new ArrayList<IndexDefinition>();
	// Declare node/edge numbers
	private long userNodeNum = 0;
	private long tweetNodeNum = 0;
	private long edges = 0;

	// a twitter reweet from one other twitter, a user tweet a twitter
	enum RelationshipTypes implements RelationshipType {
		retweetFrom, hasTweeted, RUN_USER, HOUR_TWEET, 
		USER_HOUR, NEXT_LEVEL, NEXT_MONTH, NEXT_WEEK, 
		NEXT_DAY, NEXT_HOUR, HAS_RUN, HAS_TIMELINE
	}

	// initialize graph database, create index
	public void initGraphDataBase() {
		// start database
		deleteFileOrDirectory(new File(DB_PATH));
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		
//		graphDb = new GraphDatabaseFactory()
//			     .newEmbeddedDatabaseBuilder( DB_PATH )
//			     .loadPropertiesFromFile( "MovieDatabase\neo4j.properties" )
//			     .newGraphDatabase();

		// create index and label by hashtag and state
		try (Transaction tx = graphDb.beginTx()) {
			// schema for graph database
			Schema schema = graphDb.schema();

			// create label by hashtag
			for (int i = 0; i < hashtagList.length; i++) {
				Label label = DynamicLabel.label(hashtagList[i]);
				hashtagLabel.add(label);
			}

			// //create label by locations
			// for(int i = 0; i < locations.length; i++){
			// Label label = DynamicLabel.label( locations[i] );
			// locationLabel.add(label);
			// }

			// create index by hashtag for TweetId and UserId: easy to find
			// isContainUser and isContainTweet
			// create index by hashtag for TweetNum and NodeNum:easy to find
			// relations in tweets
			for (int i = 0; i < hashtagLabel.size(); i++) {
				Label label = hashtagLabel.get(i);

				IndexDefinition index_tweetId = schema.indexFor(label).on("TweetId").create();
				IndexDefinition index_userId = schema.indexFor(label).on("UserId").create();

				IndexDefinition index_tweetnum = schema.indexFor(label).on("TweetNum").create();
				IndexDefinition index_usernum = schema.indexFor(label).on("UserNum").create();

				tweetidIndex.add(index_tweetId);
				useridIndex.add(index_userId);

				tweetnumIndex.add(index_tweetnum);
				usernumIndex.add(index_usernum);

			}
			tx.success();
		}

		// wait index online
		try (Transaction tx = graphDb.beginTx()) {
			Schema schema = graphDb.schema();

			for (int i = 0; i < tweetidIndex.size(); i++) {
				schema.awaitIndexOnline(tweetidIndex.get(i), 10, TimeUnit.SECONDS);
				schema.awaitIndexOnline(useridIndex.get(i), 10, TimeUnit.SECONDS);
				schema.awaitIndexOnline(tweetnumIndex.get(i), 10, TimeUnit.SECONDS);
				schema.awaitIndexOnline(usernumIndex.get(i), 10, TimeUnit.SECONDS);
			}
			tx.success();
		}

		// create timeline

		try (Transaction tx = graphDb.beginTx()) {
			createTimeline();
			tx.success();
		}

	}

	/**
	 * create user and tweet nodes for a hashtag
	 * 
	 * @param tweet
	 * @return
	 * @throws NoSuchObjectException
	 */
	public int createUserAndTweetNode(Tweet tweet) throws NoSuchObjectException {
		// for user
		String UserId = tweet.getUserID();
		String UserName = tweet.getUserName();
		// for tweet
		Date Time = tweet.getTime(); // can not set property as Date, transfer
										// it later
		String TweetId = tweet.getTweetId();
		String Text = tweet.getText();
		// String RetweetFromID = tweet.getRetweetFromID();
		// String location = tweet.getState();

		ArrayList<String> Hashtag = tweet.getHashtag();
		boolean isvalid = tweet.getisValid();

		if (!isvalid || isContainTwitter(TweetId) != null)
			return -1;

		// find the existed node and add new hashtag and tweet
		Node ExistUserNode;
		if ((ExistUserNode = isContainUser(UserId)) != null) {
			try (Transaction tx = graphDb.beginTx()) {
				// add new label to exist user node
				for (int i = 0; i < Hashtag.size(); i++) {
					Label label = DynamicLabel.label(Hashtag.get(i));
					if (!ExistUserNode.hasLabel(label))
						ExistUserNode.addLabel(label);
				}

				// add new twitter to user
				Node tweetNode = graphDb.createNode();

				// set property to tweet node
				tweetNode.setProperty("TweetNum", tweetNodeNum); // good for me
																	// to
																	// control
																	// tweet
				tweetNode.setProperty("TweetId", TweetId);
				tweetNode.setProperty("Time", Time.getTime()); // transfer time
																// to m seconds
				tweetNode.setProperty("Text", Text);
				// tweetNode.setProperty("RetweetFromID", RetweetFromID);
				// tweetNode.setProperty("Location", location);

				// create relationship to exist user node
				ExistUserNode.createRelationshipTo(tweetNode, RelationshipTypes.hasTweeted);
				edges += 2;
				tweetNodeNum++;

				tx.success();

				return 0;
			}
		}

		// create user node and tweet node with property and label
		try (Transaction tx = graphDb.beginTx()) {

			// create node
			Node userNode = graphDb.createNode();
			Node tweetNode = graphDb.createNode();

			// add hashtag of label to two nodes
			for (int i = 0; i < Hashtag.size(); i++) {
				Label label = DynamicLabel.label(Hashtag.get(i));
				userNode.addLabel(label);
				tweetNode.addLabel(label);
				edges += 2;
			}

			// add state of label to two nodes
			// {
			// Label label = DynamicLabel.label(State);
			// userNode.addLabel(label);
			// tweetNode.addLabel(label);
			// }

			// set property to user node
			userNode.setProperty("UserNum", userNodeNum); // good for me to
															// control user
			userNode.setProperty("UserId", UserId);
			userNode.setProperty("UserName", UserName);
			// userNode.setProperty("Location", location);

			// set property to tweet node
			tweetNode.setProperty("TweetNum", tweetNodeNum); // good for me to
																// control tweet
			tweetNode.setProperty("TweetId", TweetId);
			tweetNode.setProperty("Time", Time.getTime()); // transfer time to m
															// seconds
			tweetNode.setProperty("Text", Text);
			// tweetNode.setProperty("RetweetFromID", RetweetFromID);
			// tweetNode.setProperty("State", location);

			// connect the user and tweet
			userNode.createRelationshipTo(tweetNode, RelationshipTypes.hasTweeted);
			edges += 2;

			connectUserTweet(userNode, tweetNode, Time);
			// increase node number by 1
			userNodeNum++;
			tweetNodeNum++;

			tx.success();

			return 1;
		}

	}

	/**
	 * The second graph model for graph database
	 * 
	 * @param tweet
	 * @return
	 */
	public int createUserAndTweetNode_2(Tweet tweet) {
		// for user
		String UserId = tweet.getUserID();
		String UserName = tweet.getUserName();
		// for tweet
		Date Time = tweet.getTime(); // can not set property as Date, transfer
										// it later
		String TweetId = tweet.getTweetId();
		String Text = tweet.getText();
		// String RetweetFromID = tweet.getRetweetFromID();
		// String location = tweet.getState();

		ArrayList<String> Hashtag = tweet.getHashtag();
		boolean isvalid = tweet.getisValid();

		if (!isvalid || isContainTwitter(TweetId) != null)
			return -1;

		try (Transaction tx = graphDb.beginTx()) {
			Node tweetNode = graphDb.createNode();

			// add hashtag of label to two nodes
			for (int i = 0; i < Hashtag.size(); i++) {
				Label label = DynamicLabel.label(Hashtag.get(i));
				tweetNode.addLabel(label);
				edges += 2;
			}

			// add state of label to two nodes
			// {
			// Label label = DynamicLabel.label(location);
			//
			// tweetNode.addLabel(label);
			// }

			// set property to tweet node
			tweetNode.setProperty("UserId", UserId);
			tweetNode.setProperty("UserName", UserName);
			tweetNode.setProperty("TweetNum", tweetNodeNum); // good for me to
																// control tweet
			tweetNode.setProperty("TweetId", TweetId);
			tweetNode.setProperty("Time", Time.getTime()); // transfer time to m
															// seconds
			tweetNode.setProperty("Text", Text);
			// tweetNode.setProperty("RetweetFromID", RetweetFromID);
			// tweetNode.setProperty("State", location);

			// increase node number by 1
			tweetNodeNum++;

			tx.success();
			return 0;
		}

	}

	// start from tweetnum = 0
	// find node with tweetnum and tweetid, create relationship among twitters
	public void creatRelatioShipInTweets() {
		try (Transaction tx = graphDb.beginTx()) {
			for (long TweetNumTofind = 0; TweetNumTofind < tweetNodeNum; TweetNumTofind++) {
				for (int i = 0; i < hashtagLabel.size(); i++) { // start from
																// tweetNUm = 0

					String tweetnum_str = String.valueOf(TweetNumTofind);
					// System.out.print("node num =" + tweetnum_str + "\n");

					ResourceIterator<Node> tweetsOri = graphDb
							.findNodesByLabelAndProperty(hashtagLabel.get(i), "TweetNum", tweetnum_str).iterator(); 
					// find all nodes with this property, even on such label
					if (tweetsOri.hasNext()) {
						// find node has tweetnum
						Node tweetNodeOri = tweetsOri.next();
						// get retweetfromid from string to long
						long retweetfromid = Long.parseLong(tweetNodeOri.getProperty("RetweetFromID").toString());
						// System.out.print("find tweet
						// from="+retweetfromid+"\n");
						if (retweetfromid != 0) { // find node has retweetfromid
													// as tweet id
							for (int j = 0; j < hashtagLabel.size(); j++) {
								ResourceIterator<Node> tweetsFrom = graphDb
										.findNodesByLabelAndProperty(hashtagLabel.get(j), "TweetId", retweetfromid)
										.iterator(); 
								// find all nodes with this property, even on such label
								if (tweetsFrom.hasNext()) {
									Node tweetNodeFrom = tweetsFrom.next();
									tweetNodeOri.createRelationshipTo(tweetNodeFrom, RelationshipTypes.retweetFrom);
									edges++;
									// System.out.print("find tweet
									// id="+tweetNodeOri.getProperty("TweetId")+"\n");
									break;
								}
							}
						}
						break;
					}

				}
			}
			tx.success();
		}
	}

	// check whether contain same user node
	private Node isContainUser(String userId) {
		try (Transaction tx = graphDb.beginTx()) {
			for (int i = 0; i < hashtagLabel.size(); i++) { 
				// find user by userId
				ResourceIterator<Node> users = graphDb
						.findNodesByLabelAndProperty(hashtagLabel.get(i), "UserId", userId).iterator(); 
				// find all nodes with this property, even on such label
				if (users.hasNext()) {
					System.out.print("user = " + userId + "\n");
					return users.next();
				}

			}
			tx.success();
		}

		return null;
	}

	// check whether contain same twitter
	private Node isContainTwitter(String twitterId) {
		try (Transaction tx = graphDb.beginTx()) {
			for (int i = 0; i < hashtagLabel.size(); i++) { // find user by
															// userId
				ResourceIterator<Node> twitters = graphDb
						.findNodesByLabelAndProperty(hashtagLabel.get(i), "TweetId", twitterId).iterator(); 
				// find all nodes with this property, even on such label
				if (twitters.hasNext()) {
					System.out.print("twitters = " + twitterId + "\n");
					return twitters.next();
				}

			}
			tx.success();
		}

		return null;
	}

	// delete the file of graph database
	private void deleteFileOrDirectory(File file) {
		if (file.exists()) {
			if (file.isDirectory()) {
				for (File child : file.listFiles()) {
					deleteFileOrDirectory(child);
				}
			}
			file.delete();
		}
	}

	public long getUserNodeNum() {
		return userNodeNum;
	}

	public long getTweetNodeNum() {
		return tweetNodeNum;
	}

	public long getEdges() {
		return edges;
	}

	public GraphDatabaseService getGdb() {
		return graphDb;
	}

	/**
	 * 
	 * @param user
	 * @param tweet
	 * @param time
	 * @throws NoSuchObjectException
	 */
	public void connectUserTweet(Node user, Node tweet, Date time) throws NoSuchObjectException {
		// get first node: reference node
		Node ref = graphDb.findNodes(DynamicLabel.label("REF")).next();
		// get run node
		Node run = ref.getRelationships().iterator().next().getEndNode();
		// check if user already connected to run node
		Iterator<Relationship> relationIt = run.getRelationships(Direction.OUTGOING, RelationshipTypes.RUN_USER)
				.iterator();
		boolean exist = false;
		while (relationIt.hasNext()) {
			if (user.equals(relationIt.next().getEndNode())) {
				exist = true;
				break;
			}
		}
		// create new relationship if new user appears
		if (!exist) {
			run.createRelationshipTo(user, RelationshipTypes.RUN_USER);
		}
		Node hour = getHourNode(time);
		hour.createRelationshipTo(tweet, RelationshipTypes.HOUR_TWEET);

		Iterator<Relationship> hourIt = user.getRelationships(Direction.OUTGOING, RelationshipTypes.USER_HOUR)
				.iterator();
		boolean userHourExist = false;
		while (hourIt.hasNext()) {
			if (hour.equals(hourIt.next().getEndNode())) {
				userHourExist = true;
				break;
			}
		}
		if (!userHourExist) {
			user.createRelationshipTo(hour, RelationshipTypes.USER_HOUR);
		}
	}

	/**
	 * 
	 * @param time
	 * @return
	 * @throws NoSuchObjectException
	 */
	public Node getHourNode(Date time) throws NoSuchObjectException {
		// search for the right hour node
		// get first node: reference node
		Label lb = DynamicLabel.label("REF");
//		System.out.println(lb.toString());
		
//		System.out.println(graphDb.isAvailable(2000));
		Node ref = graphDb.getNodeById(0);
//		Node ref = graphDb.findNodes(lb).next();
		// get run node
		Node run = ref.getRelationships().iterator().next().getEndNode();
//		System.out.println(run.getLabels().iterator().next());
		Node timeLine = run.getSingleRelationship(RelationshipTypes.HAS_TIMELINE, Direction.OUTGOING).getEndNode();
		Calendar cal = Calendar.getInstance();
		cal.setTime(time);
		int month = cal.get(Calendar.MONTH);
		int week = cal.get(Calendar.WEEK_OF_MONTH)- 1;
		int day = cal.get(Calendar.DAY_OF_WEEK) - 1;
		int hour = cal.get(Calendar.HOUR_OF_DAY);
		
//		System.out.println("month: "+ month);
//		System.out.println("week: "+ week);
//		System.out.println("day: "+ day);
//		System.out.println("hour: "+ hour);
		
		// find month
		Iterator<Relationship> monthIt = timeLine.getRelationships(Direction.OUTGOING).iterator();
		Node monthNode = null;
		while (monthIt.hasNext()) {
			Node tempNode = monthIt.next().getEndNode();
			if (month == (int) tempNode.getProperty("MonthOfYear")) {
				monthNode = tempNode;
				break;
			}
		}
		if (monthNode == null)
			throw new NoSuchObjectException("The month node does not exist");

		// find week
		Iterator<Relationship> weekIt = monthNode.getRelationships(Direction.OUTGOING).iterator();
		Node weekNode = null;
		while (weekIt.hasNext()) {
			Node tempNode = weekIt.next().getEndNode();
			if (tempNode.getPropertyKeys().iterator().next().equals("WeekOfMonth") && week == (int) tempNode.getProperty("WeekOfMonth")) {
				weekNode = tempNode;
				break;
			}
		}
		if (weekNode == null)
			throw new NoSuchObjectException("The week node does not exist");

		// find day_of_week
		Iterator<Relationship> dayIt = weekNode.getRelationships(Direction.OUTGOING).iterator();
		Node dayNode = null;
		while (dayIt.hasNext()) {
			Node tempNode = dayIt.next().getEndNode();
			if (tempNode.getPropertyKeys().iterator().next().equals("DayOfWeek") && day == (int) tempNode.getProperty("DayOfWeek")) {
				dayNode = tempNode;
				break;
			}
		}
		if (dayNode == null)
			throw new NoSuchObjectException("The day node does not exist");

		// find hour_of_day
		Iterator<Relationship> hourIt = dayNode.getRelationships(Direction.OUTGOING).iterator();
		Node hourNode = null;
//		Node alterHour
		System.out.println("Hour number: " + hour);
		while (hourIt.hasNext()) {
			Node tempNode = hourIt.next().getEndNode();
			
			if (tempNode.getPropertyKeys().iterator().next().equals("HourOfDay")&& hour == (int) tempNode.getProperty("HourOfDay")){
//					System.out.println(tempNode.getProperty("HourOfDay"));
					hourNode = tempNode;
					break;
				
			}
		}
		if (hourNode == null)
			throw new NoSuchObjectException("The hour node does not exist");

		return hourNode;

	}

	public void createTimeline() {
		// initialize REF and RUN node
		Node refNode = graphDb.createNode();
		Label refLabel = DynamicLabel.label("REF");
		refNode.addLabel(refLabel);
		Node runNode = graphDb.createNode();
		Label runLabel = DynamicLabel.label("RUN");
		runNode.addLabel(runLabel);

		refNode.createRelationshipTo(runNode, RelationshipTypes.HAS_RUN);
		
		
		

		// initialize year node
		Node yearNode = graphDb.createNode();
		Label yearLabel = DynamicLabel.label("TIMELINE");
		yearNode.addLabel(yearLabel);
		yearNode.setProperty("year", 2015);
		
		runNode.createRelationshipTo(yearNode, RelationshipTypes.HAS_TIMELINE);

		// initialize month node
		Node initialMonthNode = graphDb.createNode();
		Label initialMonthLabel = DynamicLabel.label("Month");
		initialMonthNode.addLabel(initialMonthLabel);
		initialMonthNode.setProperty("MonthOfYear", 0);
		yearNode.createRelationshipTo(initialMonthNode, RelationshipTypes.NEXT_LEVEL);
		Node preMonthNode = initialMonthNode;

		// initialize week node

		Node initialWeekNode = graphDb.createNode();
		Label initialWeekLabel = DynamicLabel.label("Week");
		initialWeekNode.addLabel(initialWeekLabel);
		initialWeekNode.setProperty("WeekOfMonth", 0);
		initialMonthNode.createRelationshipTo(initialWeekNode, RelationshipTypes.NEXT_LEVEL);
		Node preWeekNode = initialWeekNode;

		// initiallize day node

		Node initialDayNode = graphDb.createNode();
		Label initialDayLabel = DynamicLabel.label("Day");
		initialDayNode.addLabel(initialDayLabel);
		initialDayNode.setProperty("DayOfWeek", 0);
		initialWeekNode.createRelationshipTo(initialDayNode, RelationshipTypes.NEXT_LEVEL);
		Node preDayNode = initialDayNode;

		// initiallize hour node
		Node initialHourNode = graphDb.createNode();
		Label initialHourLabel = DynamicLabel.label("Hour");
		initialHourNode.addLabel(initialHourLabel);
		initialHourNode.setProperty("HourOfDay", 0);
		initialDayNode.createRelationshipTo(initialHourNode, RelationshipTypes.NEXT_LEVEL);
		Node preHourNode = initialHourNode;

		for (int m = 0; m < 12; m++) {

			if (m > 0) {
				Node monthNode = graphDb.createNode();
				Label monthLabel = DynamicLabel.label("Month");
				monthNode.addLabel(monthLabel);
				monthNode.setProperty("MonthOfYear", m);
				yearNode.createRelationshipTo(monthNode, RelationshipTypes.NEXT_LEVEL);
				preMonthNode.createRelationshipTo(monthNode, RelationshipTypes.NEXT_MONTH);
				preMonthNode = monthNode;
			}

			for (int w = 5 * m; w < 5 * m + 5; w++) {
				if (w / 5 < 1 && w > 0) {
					Node weekNode = graphDb.createNode();
					Label weekLabel = DynamicLabel.label("Week");
					weekNode.addLabel(weekLabel);
					weekNode.setProperty("WeekOfMonth", w);
					initialMonthNode.createRelationshipTo(weekNode, RelationshipTypes.NEXT_LEVEL);
					preWeekNode.createRelationshipTo(weekNode, RelationshipTypes.NEXT_WEEK);
					preWeekNode = weekNode;
				}

				if (w / 5 >= 1) {
					Node weekNode = graphDb.createNode();
					Label weekLabel = DynamicLabel.label("Week");
					weekNode.addLabel(weekLabel);
					weekNode.setProperty("WeekOfMonth", w % 5);
					preMonthNode.createRelationshipTo(weekNode, RelationshipTypes.NEXT_LEVEL);
					preWeekNode.createRelationshipTo(weekNode, RelationshipTypes.NEXT_WEEK);
					preWeekNode = weekNode;
				}

				for (int d = 7 * w; d < 7 * w + 7; d++) {
					if (d / 7 < 1 && d > 0) {
						Node dayNode = graphDb.createNode();
						Label dayLabel = DynamicLabel.label("Day");
						dayNode.addLabel(dayLabel);
						dayNode.setProperty("DayOfWeek", d);
						initialWeekNode.createRelationshipTo(dayNode, RelationshipTypes.NEXT_LEVEL);
						preDayNode.createRelationshipTo(dayNode, RelationshipTypes.NEXT_DAY);
						preDayNode = dayNode;
					}

					if (d / 7 >= 1) {
						Node dayNode = graphDb.createNode();
						Label dayLabel = DynamicLabel.label("Day");
						dayNode.addLabel(dayLabel);
						dayNode.setProperty("DayOfWeek", d % 7);
						preWeekNode.createRelationshipTo(dayNode, RelationshipTypes.NEXT_LEVEL);
						preDayNode.createRelationshipTo(dayNode, RelationshipTypes.NEXT_DAY);
						preDayNode = dayNode;
					}

					for (int h = 24 * d; h < 24 * d + 24; h++) {
						if (h / 24 < 1 && h > 0) {
							Node hourNode = graphDb.createNode();
							Label hourLabel = DynamicLabel.label("Hour");
							hourNode.addLabel(hourLabel);
							hourNode.setProperty("HourOfDay", h);
							initialDayNode.createRelationshipTo(hourNode, RelationshipTypes.NEXT_LEVEL);
							preHourNode.createRelationshipTo(hourNode, RelationshipTypes.NEXT_HOUR);
							preHourNode = hourNode;
						}

						if (h / 24 >= 1) {
							Node hourNode = graphDb.createNode();
							Label hourLabel = DynamicLabel.label("Hour");
							hourNode.addLabel(hourLabel);
							hourNode.setProperty("HourOfDay", h % 24);
							preDayNode.createRelationshipTo(hourNode, RelationshipTypes.NEXT_LEVEL);
							preHourNode.createRelationshipTo(hourNode, RelationshipTypes.NEXT_HOUR);
							preHourNode = hourNode;
						}
					}
				}

			}

		}

	}

}
