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

import project.GraphDatabase.RelationshipTypes;

/**
 * 
 * Graph database structures and methods for Model 2.
 * 
 * @author Yan
 *
 */
public class GraphDatabase {

	private String[] hashtagList = CreateDatabase.hashtagList;

	private String[] locations = CreateDatabase.locations;

	// Declare database path
	public static final String DB_PATH = "MovieDatabaseM2Nov30UntilDec5";
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
		retweetFrom, hasTweeted, RUN_USER, HOUR_TWEET, USER_HOUR, NEXT_LEVEL, NEXT_MONTH, NEXT_WEEK, NEXT_DAY, NEXT_HOUR, HAS_RUN, HAS_TIMELINE, RUN_MOVIE, MONTH_TWEET, WEEK_TWEET, DAY_TWEET, MOVIE_TWEET, MOVIE_MONTH, MOVIE_HOUR, MOVIE_WEEK, MOVIE_DAY, RUN_YEAR
	}

	// initialize graph database, create index
	public void initGraphDataBase() {
		// start database
		deleteFileOrDirectory(new File(DB_PATH));
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);

		// graphDb = new GraphDatabaseFactory()
		// .newEmbeddedDatabaseBuilder( DB_PATH )
		// .loadPropertiesFromFile( "MovieDatabase\neo4j.properties" )
		// .newGraphDatabase();

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
	 * The second graph model for graph database
	 * 
	 * @param tweet
	 * @return
	 * @throws NoSuchObjectException
	 */
	public int createTweeterNode(Tweet tweet) throws NoSuchObjectException {
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
				// connect tweeter node/movie/time nodes
				connectTweet(tweetNode, Hashtag.get(i), Time);
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
			tweetNode.setProperty("TweetNum", tweetNodeNum);
			tweetNode.setProperty("TweetId", TweetId);
			tweetNode.setProperty("Time", Time.getTime()); 
			tweetNode.setProperty("Text", Text);

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
						if (retweetfromid != 0) { 
							for (int j = 0; j < hashtagLabel.size(); j++) {
								ResourceIterator<Node> tweetsFrom = graphDb
										.findNodesByLabelAndProperty(hashtagLabel.get(j), "TweetId", retweetfromid)
										.iterator();
								// find all nodes with this property, even on such label
								if (tweetsFrom.hasNext()) {
									Node tweetNodeFrom = tweetsFrom.next();
									tweetNodeOri.createRelationshipTo(tweetNodeFrom, RelationshipTypes.retweetFrom);
									edges++;
									// System.out.print("find tweet id="+tweetNodeOri.getProperty("TweetId")+"\n");
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
	public void connectTweet(Node tweet, String hashtag, Date time) throws NoSuchObjectException {
		// search for the right time node
		// get first node: reference node
		Node ref = graphDb.findNodes(DynamicLabel.label("REF")).next();
		// get run node
		Node run = ref.getRelationships().iterator().next().getEndNode();
		Node timeLine = run.getSingleRelationship(RelationshipTypes.HAS_TIMELINE, Direction.OUTGOING).getEndNode();
		Calendar cal = Calendar.getInstance();
		cal.setTime(time);
		int month = cal.get(Calendar.MONTH);
		int week = cal.get(Calendar.WEEK_OF_MONTH) - 1;
		int day = cal.get(Calendar.DAY_OF_WEEK) - 1;
		int hour = cal.get(Calendar.HOUR_OF_DAY);

//		System.out.println("month: " + month);
//		System.out.println("week: " + week);
//		System.out.println("day: " + day);
//		System.out.println("hour: " + hour);

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
			if (tempNode.getPropertyKeys().iterator().next().equals("WeekOfMonth")
					&& week == (int) tempNode.getProperty("WeekOfMonth")) {
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
			if (tempNode.getPropertyKeys().iterator().next().equals("DayOfWeek")
					&& day == (int) tempNode.getProperty("DayOfWeek")) {
				dayNode = tempNode;
				break;
			}
		}
		if (dayNode == null)
			throw new NoSuchObjectException("The day node does not exist");

		// find hour_of_day
		Iterator<Relationship> hourIt = dayNode.getRelationships(Direction.OUTGOING).iterator();
		Node hourNode = null;
		// Node alterHour
//		System.out.println("Hour number: " + hour);
		while (hourIt.hasNext()) {
			Node tempNode = hourIt.next().getEndNode();

			if (tempNode.getPropertyKeys().iterator().next().equals("HourOfDay")
					&& hour == (int) tempNode.getProperty("HourOfDay")) {
				// System.out.println(tempNode.getProperty("HourOfDay"));
				hourNode = tempNode;
				break;

			}
		}
		if (hourNode == null)
			throw new NoSuchObjectException("The hour node does not exist");
		// connect time node to tweet
		monthNode.createRelationshipTo(tweet, RelationshipTypes.MONTH_TWEET);
		weekNode.createRelationshipTo(tweet, RelationshipTypes.WEEK_TWEET);
		dayNode.createRelationshipTo(tweet, RelationshipTypes.DAY_TWEET);
		hourNode.createRelationshipTo(tweet, RelationshipTypes.HOUR_TWEET);

		// find movie and connect it to tweet
		Iterator<Relationship> relationIt = run.getRelationships(Direction.OUTGOING, RelationshipTypes.RUN_MOVIE)
				.iterator();
		Node movieNode = null;
		while (relationIt.hasNext()) {
			movieNode = relationIt.next().getEndNode();
			if (movieNode.getPropertyKeys().iterator().next().equals("hashtag")
					&& hashtag.equals(movieNode.getProperty("hashtag"))) {
				movieNode.createRelationshipTo(tweet, RelationshipTypes.MOVIE_TWEET);
				break;// one tweet only connects to one movie
			}
		}
		if (movieNode == null)
			throw new NoSuchObjectException("The movie node does not exist");

		// create new relationship from time nodes to tweet
		boolean movieMonthExist = false;
		boolean movieWeekExist = false;
		boolean movieDayExist = false;
		boolean movieHourExist = false;
		Iterator<Relationship> movieIt = movieNode.getRelationships(Direction.OUTGOING).iterator();
		while (movieIt.hasNext()) {
			Relationship cur = movieIt.next();
			Node curNode = cur.getEndNode();
			if (monthNode.equals(curNode)) {
				// System.out.println("Month: NumberOfTweet has been updated");
				movieMonthExist = true;
				cur.setProperty("NumberOfTweet", (int) cur.getProperty("NumberOfTweet") + 1);
			}
			if (weekNode.equals(curNode)) {
				// System.out.println("Week: NumberOfTweet has been updated");
				movieWeekExist = true;
				cur.setProperty("NumberOfTweet", (int) cur.getProperty("NumberOfTweet") + 1);
			}
			if (dayNode.equals(curNode)) {
				// System.out.println("Day: NumberOfTweet has been updated");
				movieDayExist = true;
				cur.setProperty("NumberOfTweet", (int) cur.getProperty("NumberOfTweet") + 1);
			}
			if (hourNode.equals(curNode)) {
				// System.out.println("Hour: NumberOfTweet has been updated");
				movieHourExist = true;
				cur.setProperty("NumberOfTweet", (int) cur.getProperty("NumberOfTweet") + 1);
			}
		}
		if (!movieMonthExist) {
			// System.out.println("Month: NumberOfTweet has been created");
			Relationship rs = movieNode.createRelationshipTo(monthNode, RelationshipTypes.MOVIE_MONTH);
			rs.setProperty("NumberOfTweet", 1);
		}
		if (!movieWeekExist) {
			Relationship rs = movieNode.createRelationshipTo(weekNode, RelationshipTypes.MOVIE_WEEK);
			rs.setProperty("NumberOfTweet", 1);
		}
		if (!movieDayExist) {
			Relationship rs = movieNode.createRelationshipTo(dayNode, RelationshipTypes.MOVIE_DAY);
			rs.setProperty("NumberOfTweet", 1);
		}
		if (!movieHourExist) {
			Relationship rs = movieNode.createRelationshipTo(hourNode, RelationshipTypes.MOVIE_HOUR);
			rs.setProperty("NumberOfTweet", 1);
		}

	}



	public Node getHourNode(Date time) throws NoSuchObjectException {
		// search for the right hour node
		Calendar cal = Calendar.getInstance();
		cal.setTime(time);
		int hour = cal.get(Calendar.HOUR_OF_DAY);

		Node dayNode = getDayNode(time);
		if (dayNode == null)
			throw new NoSuchObjectException("The day node does not exist");

		// find hour_of_day
		Iterator<Relationship> hourIt = dayNode.getRelationships(Direction.OUTGOING).iterator();
		Node hourNode = null;
		// Node alterHour
		while (hourIt.hasNext()) {
			Node tempNode = hourIt.next().getEndNode();

			if (tempNode.getPropertyKeys().iterator().next().equals("HourOfDay")
					&& hour == (int) tempNode.getProperty("HourOfDay")) {
				// System.out.println(tempNode.getProperty("HourOfDay"));
				hourNode = tempNode;
				break;

			}
		}
		if (hourNode == null)
			throw new NoSuchObjectException("The hour node does not exist");

		return hourNode;

	}
	
	
	public Node getDayNode(Date time) throws NoSuchObjectException {
		// search for the right day node
		Calendar cal = Calendar.getInstance();
		cal.setTime(time);
		int day = cal.get(Calendar.DAY_OF_WEEK) - 1;
		
		Node weekNode = getWeekNode(time);
	
		if (weekNode == null)
			throw new NoSuchObjectException("The week node does not exist");

		// find day_of_week
		Iterator<Relationship> dayIt = weekNode.getRelationships(Direction.OUTGOING).iterator();
		Node dayNode = null;
		while (dayIt.hasNext()) {
			Node tempNode = dayIt.next().getEndNode();
			if (tempNode.getPropertyKeys().iterator().next().equals("DayOfWeek")
					&& day == (int) tempNode.getProperty("DayOfWeek")) {
				dayNode = tempNode;
				break;
			}
		}
		if (dayNode == null)
			throw new NoSuchObjectException("The day node does not exist");

		return dayNode;

	}
	
	public Node getWeekNode(Date time) throws NoSuchObjectException {
		// search for the right week node
		Calendar cal = Calendar.getInstance();
		cal.setTime(time);
		int week = cal.get(Calendar.WEEK_OF_MONTH) - 1;

		Node monthNode = getMonthNode(time);
		
		if (monthNode == null)
			throw new NoSuchObjectException("The month node does not exist");

		// find week
		Iterator<Relationship> weekIt = monthNode.getRelationships(Direction.OUTGOING).iterator();
		Node weekNode = null;
		while (weekIt.hasNext()) {
			Node tempNode = weekIt.next().getEndNode();
			if (tempNode.getPropertyKeys().iterator().next().equals("WeekOfMonth")
					&& week == (int) tempNode.getProperty("WeekOfMonth")) {
				weekNode = tempNode;
				break;
			}
		}
		if (weekNode == null)
			throw new NoSuchObjectException("The week node does not exist");

		return weekNode;

	}
	
	public Node getMonthNode(Date time) throws NoSuchObjectException {
		// search for the right hour node
		// get first node: reference node
//		System.out.println("Running the getMonthNode method. Number of REF nodes:" +graphDb.findNodes(DynamicLabel.label("REF")).hasNext());
		
		Node ref = graphDb.findNodes(DynamicLabel.label("REF")).next();
//		System.out.println("Got the REF node.");
		// get run node
		Node run = ref.getRelationships().iterator().next().getEndNode();
		Node timeLine = run.getSingleRelationship(RelationshipTypes.HAS_TIMELINE, Direction.OUTGOING).getEndNode();
		Calendar cal = Calendar.getInstance();
		cal.setTime(time);
		int month = cal.get(Calendar.MONTH);

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

		return monthNode;

	}

	/**
	 * @author YAN
	 * @param givenNode
	 * @return
	 * 
	 * return the Year Month Week Day Hour of a given time node
	 */
	public static String nodeTime(Node givenNode){
		Node node=givenNode;
		String nodeLabel=givenNode.getLabels().iterator().next().toString();
//		System.out.println("Node label: "+nodeLabel+"Date: "+node.getProperty("DayOfWeek"));
		String time=null;
		if(nodeLabel.equals("TIMELINE")){
//			System.out.println(node.getProperty("year"));
			time="Year: "+node.getProperty("year");
			
		}else if(nodeLabel.equals("Month")){
			Node yearNode = givenNode.getSingleRelationship(RelationshipTypes.NEXT_LEVEL, Direction.INCOMING).getOtherNode(givenNode);
//			System.out.println(yearNode.getProperty("year"));
			time=nodeTime(yearNode)+" Month: "+((int) (node.getProperty("MonthOfYear"))+1);
			
		}else if(nodeLabel.equals("Week")){
			Node monthNode=givenNode.getSingleRelationship(RelationshipTypes.NEXT_LEVEL, Direction.INCOMING).getOtherNode(givenNode);
//			System.out.println(monthNode.getProperty("MonthOfYear"));
			time=nodeTime(monthNode)+" WEEK: "+((int) (node.getProperty("WeekOfMonth"))+1);
		}else if(nodeLabel.equals("Day")){
			Node weekNode=givenNode.getSingleRelationship(RelationshipTypes.NEXT_LEVEL, Direction.INCOMING).getOtherNode(givenNode);
//			System.out.println(weekNode.getLabels().iterator().next());
//			System.out.println(weekNode.getProperty("WeekOfMonth"));
			time=nodeTime(weekNode)+" DAY: "+node.getProperty("DayOfWeek").toString();
		}else if(nodeLabel.equals("Hour")){
			Node hourNode=givenNode.getSingleRelationship(RelationshipTypes.NEXT_LEVEL, Direction.INCOMING).getOtherNode(givenNode);
//			System.out.println(hourNode.getProperty("HourOfDay"));
			time=nodeTime(hourNode)+" Hour: "+node.getProperty("HourOfDay").toString();
		}else{
			throw new IllegalArgumentException();
		}
//		System.out.println("time: "+time);
		return time;
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

		// create movieNodes and connect movieNodes with RUN node
		for (int i = 0; i < hashtagLabel.size(); i++) {
			Node movieNode = graphDb.createNode();
			// Label label = DynamicLabel.label(hashtagList[i]);
			movieNode.addLabel(DynamicLabel.label("MOVIE"));
			// movieNode.addLabel(label);
			movieNode.setProperty("hashtag", hashtagList[i]);
			runNode.createRelationshipTo(movieNode, RelationshipTypes.RUN_MOVIE);
			movieNode.createRelationshipTo(yearNode, RelationshipTypes.RUN_YEAR);
		}

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
