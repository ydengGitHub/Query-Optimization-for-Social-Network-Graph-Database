package project;

import java.rmi.NoSuchObjectException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import org.neo4j.consistency.checking.full.NodeToLabelScanRecordProcessor;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.tooling.GlobalGraphOperations;

import com.sun.xml.internal.messaging.saaj.packaging.mime.internet.ParseException;
import com.sun.xml.internal.ws.api.pipe.ThrowableContainerPropertySet;

import project.GraphDatabase.RelationshipTypes;
import project.QueryDatabase.MoviePopulity;

import project.GraphDatabase.RelationshipTypes;

/**
 * 
 * Query the graph database Model2.
 * 
 * @author Yan
 *
 */
public class QueryDatabase {

	private String[] hashtagString = CreateDatabase.hashtagList;

	private String[] locations = CreateDatabase.locations;

	// enum RelationshipTypes implements RelationshipType {
	// retweetFrom, hasTweeted
	// }

	ArrayList<String> hashtagList = new ArrayList<String>();
	ArrayList<String> locationList = new ArrayList<String>();

	private ArrayList<Label> hashtagLabel = new ArrayList<Label>();

	// connect graph database
	private final String DB_PATH = GraphDatabase.DB_PATH;
	public GraphDatabaseService graphDb;

	public GraphDatabase graphdb = new GraphDatabase();

	public static DateFormat formatter = new SimpleDateFormat("MM dd HH yyyy");

	public static String firstDateString = "01 01 00 2015";

	public static Date firstDate = stringToDate(firstDateString);

	public void initQuery() {

		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);

		System.out.println("Database Path: " + DB_PATH);

		for (int i = 0; i < hashtagString.length; i++) {
			hashtagList.add(hashtagString[i]);
		}

		for (int i = 0; i < locations.length; i++) {
			locationList.add(locations[i]);
		}

	}

	private Node isContainUser(String userId) {

		for (int i = 0; i < hashtagList.size(); i++) {
			Label label = DynamicLabel.label(hashtagList.get(i));
			hashtagLabel.add(label);
		}

		try (Transaction tx = graphDb.beginTx()) {
			for (int i = 0; i < hashtagLabel.size(); i++) { // find user by
															// userId
				ResourceIterator<Node> users = graphDb
						.findNodesByLabelAndProperty(hashtagLabel.get(i), "UserId", userId).iterator();
				if (users.hasNext()) {
					System.out.print("user = " + userId + "\n");
					return users.next();
				}

			}
			tx.success();
		}

		return null;
	}

	/**
	 * @author YAN
	 * @param startTime
	 * @param endTime
	 * @param k
	 * @return The best k movies' name, number of Tweets in given Time frame
	 */
	public List<MoviePopulity> query1(String startTime, String endTime, int k) throws NoSuchObjectException {
		List<MoviePopulity> res = new ArrayList<>();
		PriorityQueue<MoviePopulity> pq = new PriorityQueue<>(k, (MoviePopulity x, MoviePopulity y) -> x.num - y.num);
		for (int i = 0; i < hashtagString.length; i++) {
			MoviePopulity mv = new MoviePopulity(hashtagString[i], query3(startTime, endTime, hashtagString[i]));
//			System.out.println("query 3 result of " + mv.movie + " tweet No = " + mv.num);
			pq.add(mv);
			if (i >= k) {
				pq.poll();
			}
		}
		System.out.println();
		while (!pq.isEmpty()) {
			res.add(pq.poll());
		}
		return res;
	}

	/**
	 * @author YAN
	 * @param startTime
	 * @param endTime
	 * @param k
	 * @return The worst k movies' name, number of Tweets in given Time frame
	 * @throws NoSuchObjectException
	 */
	public List<MoviePopulity> query2(String startTime, String endTime, int k) throws NoSuchObjectException {
		List<MoviePopulity> res = new ArrayList<>();
		PriorityQueue<MoviePopulity> pq = new PriorityQueue<>(k, (MoviePopulity x, MoviePopulity y) -> y.num - x.num);
		for (int i = 0; i < hashtagString.length; i++) {
			MoviePopulity mv = new MoviePopulity(hashtagString[i], query3(startTime, endTime, hashtagString[i]));
			pq.add(mv);
			if (i >= k) {
				pq.poll();
			}
		}
		while (!pq.isEmpty()) {
			res.add(pq.poll());
		}
		return res;
	}

	/**
	 * @author YAN
	 * @param startTime
	 * @param endTime
	 * @param hashtag
	 * @return The number of Tweets of the given movie in given Time frame
	 * @throws NoSuchObjectException
	 */
	public int query3(String startTime, String endTime, String hashtag) throws NoSuchObjectException {
		int num = 0;
		Date start = stringToDate(startTime);
//		System.out.println(start);
		Date end = stringToDate(endTime);
//		System.out.println(end);
		num = tweetNumTimeframe(start, end, hashtag);
//		System.out.println(hashtag + " 's No. of Tweets between " + start + " and " + end + " : " + num);
		return num;
	}

	/**
	 * @author YAN
	 * @param dayTime
	 * @param hashag
	 * @return The best day of the given Movie
	 * @throws NoSuchObjectException
	 */
	public String query4(String hashtag) throws NoSuchObjectException {
		String bestDay = null;
		Node bestDayNode = null;
		int maxNum = 0;

		Node monthNode = getMonthNode(firstDate);
		while (tweetNumNode(monthNode, "MONTH", hashtag) == 0) {
			monthNode = monthNode.getSingleRelationship(GraphDatabase.RelationshipTypes.NEXT_MONTH, Direction.OUTGOING)
					.getEndNode();
		}
		Iterator<Relationship> weekIt = monthNode
				.getRelationships(GraphDatabase.RelationshipTypes.NEXT_LEVEL, Direction.OUTGOING).iterator();
		Node weekNode = weekIt.next().getEndNode();
		while (tweetNumNode(weekNode, "WEEK", hashtag) == 0) {
			weekNode = weekNode.getSingleRelationship(GraphDatabase.RelationshipTypes.NEXT_WEEK, Direction.OUTGOING)
					.getEndNode();
		}
		Iterator<Relationship> dateIt = weekNode
				.getRelationships(GraphDatabase.RelationshipTypes.NEXT_LEVEL, Direction.OUTGOING).iterator();
		Node dateNode = dateIt.next().getEndNode();
		Relationship nextRelation =null;

		bestDayNode = dateNode;
		maxNum = tweetNumNode(dateNode, "DAY", hashtag);

		// Should be NEXT_DAY
		dateNode = dateNode.getSingleRelationship(GraphDatabase.RelationshipTypes.NEXT_DAY, Direction.OUTGOING)
				.getEndNode();
		while (dateNode != null) {
			int tmpNum = tweetNumNode(dateNode, "DAY", hashtag);
			if (tmpNum > maxNum) {
				maxNum = tmpNum;
				bestDayNode = dateNode;
			}
			nextRelation = dateNode.getSingleRelationship(GraphDatabase.RelationshipTypes.NEXT_DAY,
					Direction.OUTGOING);
			if (nextRelation != null) {
				dateNode = nextRelation.getEndNode();
			} else {
				break;
			}
		}

		bestDay ="Best Day of "+hashtag+" is: "+ GraphDatabase.nodeTime(bestDayNode) + ". Number of Tweets: " + maxNum;
		return bestDay;
	}

	/**
	 * @author YAN
	 * @param dayTime
	 * @param hashag
	 * @return The best week of the given Movie
	 * @throws NoSuchObjectException
	 */
	public String query5(String hashtag) throws NoSuchObjectException {
		String bestWeek = null;
		Node bestWeekNode = null;
		int maxNum = 0;

		Node monthNode = getMonthNode(firstDate);
		while (tweetNumNode(monthNode, "MONTH", hashtag) == 0) {
			monthNode = monthNode.getSingleRelationship(GraphDatabase.RelationshipTypes.NEXT_MONTH, Direction.OUTGOING)
					.getEndNode();
		}
		Iterator<Relationship> weekIt = monthNode
				.getRelationships(GraphDatabase.RelationshipTypes.NEXT_LEVEL, Direction.OUTGOING).iterator();
		Node weekNode = weekIt.next().getEndNode();
		Relationship nextRelation =null;

		bestWeekNode = weekNode;
		maxNum = tweetNumNode(weekNode, "WEEK", hashtag);

		// wrong relationship, should be NEXT_DAY
		weekNode = weekNode.getSingleRelationship(GraphDatabase.RelationshipTypes.NEXT_WEEK, Direction.OUTGOING)
				.getEndNode();
		while (weekNode != null) {
			int tmpNum = tweetNumNode(weekNode, "WEEK", hashtag);
			if (tmpNum > maxNum) {
				maxNum = tmpNum;
				bestWeekNode = weekNode;
			}
			nextRelation = weekNode.getSingleRelationship(GraphDatabase.RelationshipTypes.NEXT_WEEK,
					Direction.OUTGOING);
			if (nextRelation != null) {
				weekNode = nextRelation.getEndNode();
			} else {
				break;
			}
		}

		bestWeek ="Best Week of "+hashtag+" is: "+ GraphDatabase.nodeTime(bestWeekNode) + ". Number of Tweets: " + maxNum;
		return bestWeek;
	}

	/**
	 * @author Yan
	 * @param time
	 * @return
	 * @throws NoSuchObjectException
	 */
	// public Node getHourNode(Date time) throws NoSuchObjectException {
	//
	// return graphdb.getHourNode(time);
	// }
	//
	// public Node getDayNode(Date time) throws NoSuchObjectException {
	//
	// return graphdb.getDayNode(time);
	// }
	//
	// public Node getWeekNode(Date time) throws NoSuchObjectException {
	//
	// return graphdb.getWeekNode(time);
	// }
	//
	// public Node getMonthNode(Date time) throws NoSuchObjectException {
	//
	// return graphdb.getMonthNode(time);
	// }

	/**
	 * @author YAN
	 * @param timeNode
	 * @param nodeType
	 * @param hashtag
	 * @return number of tweets for the given time Node with respect to the
	 *         given hashtag
	 */

	public int tweetNumNode(Node timeNode, String nodeType, String hashtag) {
		int num = 0;
		Iterator<Relationship> tweetIt = null;
		if (nodeType.equals("MONTH")) {
			tweetIt = timeNode.getRelationships(Direction.INCOMING, GraphDatabase.RelationshipTypes.MOVIE_MONTH)
					.iterator();
			// if (timeNode.hasProperty("MonthOfYear")) {
			// System.out.println("Month node: " +
			// timeNode.getProperty("MonthOfYear") + " Degree: "
			// + timeNode.getDegree(RelationshipTypes.MOVIE_MONTH));
			// }
		} else if (nodeType.equals("WEEK")) {
			tweetIt = timeNode.getRelationships(Direction.INCOMING, GraphDatabase.RelationshipTypes.MOVIE_WEEK)
					.iterator();
			// if (timeNode.hasProperty("WeekOfMonth")) {
			// System.out.println("Week node: " +
			// timeNode.getProperty("WeekOfMonth") + " Degree: "
			// + timeNode.getDegree(RelationshipTypes.MOVIE_WEEK));
			// }
		} else if (nodeType.equals("DAY")) {
			tweetIt = timeNode.getRelationships(Direction.INCOMING, GraphDatabase.RelationshipTypes.MOVIE_DAY)
					.iterator();
			// if (timeNode.hasProperty("DayOfWeek")) {
			// System.out.println("Day node: " +
			// timeNode.getProperty("DayOfWeek") + " Degree: "
			// + timeNode.getDegree(RelationshipTypes.MOVIE_DAY));
			// }
		} else if (nodeType.equals("HOUR")) {
			tweetIt = timeNode.getRelationships(Direction.INCOMING, GraphDatabase.RelationshipTypes.MOVIE_HOUR)
					.iterator();
			// if (timeNode.hasProperty("HourOfDay")) {
			// System.out.println("Hour node: " +
			// timeNode.getProperty("HourOfDay") + " Degree: "
			// +timeNode.getDegree(RelationshipTypes.MOVIE_HOUR));
			// }
		}
		Relationship rel = null;
		while (tweetIt.hasNext()) {
			rel = tweetIt.next();
			// if (rel.getOtherNode(timeNode).hasProperty("hashtag")) {
			// System.out.println(rel.getOtherNode(timeNode).getProperty("hashtag"));
			if (rel.getOtherNode(timeNode).getProperty("hashtag").equals(hashtag)) {
				// Not sure...
				num = (int) rel.getProperty("NumberOfTweet");
//				System.out.println("Num of Tweet of The " + nodeType + " node:" + num);
			}
		}
		// }
		return num;
	}

	/**
	 * @author YAN
	 * @param startTime
	 * @param endTime
	 * @param hashtag
	 * @return number of tweets with respect to the given hashtag during the
	 *         given time frame
	 * @throws NoSuchObjectException
	 */

	public int tweetNumTimeframe(Date startTime, Date endTime, String hashtag) throws NoSuchObjectException {
		int num = 0;
		if (startTime.after(endTime)) {
			throw new IllegalArgumentException("The start time is after the endTime.");
		}
		Calendar startCal = Calendar.getInstance();
		startCal.setTime(startTime);
		@SuppressWarnings("deprecation")
		// int startYear = startTime.getYear();
		int startYear = startCal.get(Calendar.YEAR);
		int startMonth = startTime.getMonth()+1;
		int startDate = startTime.getDate();
		int startWeek = startCal.get(Calendar.WEEK_OF_MONTH) - 1;
		int startDay = startCal.get(Calendar.DAY_OF_WEEK) - 1;
		int startHour = startCal.get(Calendar.HOUR_OF_DAY);

		// System.out.println("startTime: "+startTime);
		// System.out.println("startYear: "+startYear);
//		 System.out.println("startMonth: "+startMonth);
		// System.out.println("startDate: "+startDate);
		// System.out.println("startWeek: "+startWeek);
		// System.out.println("startDay: "+startDay);
		// System.out.println("startHour: "+startHour);

		Calendar endCal = Calendar.getInstance();
		endCal.setTime(endTime);
		@SuppressWarnings("deprecation")
		int endYear = endCal.get(Calendar.YEAR);
		int endMonth = endTime.getMonth()+1;
		int endDate = endTime.getDate();
		int endWeek = endCal.get(Calendar.WEEK_OF_MONTH) - 1;
		int endDay = endCal.get(Calendar.DAY_OF_WEEK) - 1;
		int endHour = endCal.get(Calendar.HOUR_OF_DAY);

		
//		System.out.println("endMonth: "+endMonth);
		// Input is a whole month
		if (startMonth == endMonth && startDate == 1 && startHour == 0
				&& endDate == endCal.getActualMaximum(Calendar.DAY_OF_MONTH) && endHour == 23) {
			Node monthNode = getMonthNode(startTime);
			int tmpNum = tweetNumNode(monthNode, "MONTH", hashtag);
			num += tmpNum;
//			System.out.println("Midle month of the date: " + startTime + "; Number of Tweets: " + tmpNum);
		}
		// Input is a whole week
		else if (startMonth == endMonth && startWeek == endWeek && startDay == 0 && startHour == 0 && endDay == 6
				&& endHour == 23) {
			Node weekNode = getWeekNode(startTime);
			int tmpNum = tweetNumNode(weekNode, "WEEK", hashtag);
			num += tmpNum;
//			System.out.println("Midle week of the date: " + startTime + "; Number of Tweets: " + tmpNum);
		}
		// Input is a whole day
		else if (startMonth == endMonth && startDate == endDate && startHour == 0 && endHour == 23) {
			Node dayNode = getDayNode(startTime);
			int tmpNum = tweetNumNode(dayNode, "DAY", hashtag);
			num += tmpNum;
//			System.out.println("Midle Day of the date: " + startTime + "; Number of Tweets: " + tmpNum);
		}

		else if (startMonth != endMonth) {
			int difference = endMonth - startMonth;
			// The end of the start month.
			int lastDay = startCal.getActualMaximum(Calendar.DAY_OF_MONTH);
			String dateInString1 = monthToString(startMonth) + " " + lastDay + " 23 " + Integer.toString(startYear);
			// The beginning of the end month.
			String dateInString2 = monthToString(endMonth) + " 01 00 " + Integer.toString(endYear);

			Date tmpDate1 = stringToDate(dateInString1);
			Date tmpDate2 = stringToDate(dateInString2);
			num += tweetNumTimeframe(startTime, tmpDate1, hashtag);
			num += tweetNumTimeframe(tmpDate2, endTime, hashtag);

			for (int i = 1; i < difference; i++) {
				String dateInString3 = monthToString(startMonth + i) + " 01 00 " + Integer.toString(endYear);
				int tmpNum = addNodeTweet(dateInString3, "MONTH", hashtag);
				num += tmpNum;
//				System.out.println("Midle month of the date: " + dateInString3 + "; Number of Tweets: " + tmpNum);
			}

		} else if (startWeek != endWeek) {
			int difference = endWeek - startWeek;
			// The end of the start week
			int startSaturday = startDate + (6 - startDay);
//			System.out
//					.println("startDate: " + startDate + " startDay: " + startDay + " startSaturday: " + startSaturday);

			// The start of the end week
			String dateInString1 = monthToString(startMonth) + " " + startSaturday + " 23 "
					+ Integer.toString(startYear);
			int endSunday = endDate - endDay;
//			System.out.println("endDate: " + endDate + " endDay: " + endDay + " endSunday: " + endSunday);

			// The beginning of the end week
			String dateInString2 = monthToString(endMonth) + " " + endSunday + " 00 " + Integer.toString(endYear);
			Date tmpDate1 = stringToDate(dateInString1);
//			System.out.println("tmpDate1: " + tmpDate1);
			Date tmpDate2 = stringToDate(dateInString2);
//			System.out.println("tmpDate2: " + tmpDate2);
			num += tweetNumTimeframe(startTime, tmpDate1, hashtag);
			num += tweetNumTimeframe(tmpDate2, endTime, hashtag);

			for (int i = 1; i < difference; i++) {
				String dateInString3 = monthToString(startMonth) + " " + (startDate + 7 * i) + " 00 "
						+ Integer.toString(startYear);
				int tmpNum = addNodeTweet(dateInString3, "WEEK", hashtag);
				num += tmpNum;
//				System.out.println("Midle week of the date: " + dateInString3 + "; Number of Tweets: " + tmpNum);
			}
		} else if (startDate != endDate) {
			int difference = endDate - startDate;
			// The end of the start Date.
			String dateInString1 = monthToString(startMonth) + " " + startDate + " 23 " + Integer.toString(startYear);
			// The beginning of the end Date
			String dateInString2 = monthToString(endMonth) + " " + endDate + " 00 " + Integer.toString(endYear);

			Date tmpDate1 = stringToDate(dateInString1);
			Date tmpDate2 = stringToDate(dateInString2);
			num += tweetNumTimeframe(startTime, tmpDate1, hashtag);
			num += tweetNumTimeframe(tmpDate2, endTime, hashtag);

			for (int i = 1; i < difference; i++) {
				String dateInString3 = monthToString(startMonth) + " " + (startDate + i) + " 00 "
						+ Integer.toString(startYear);
				int tmpNum = addNodeTweet(dateInString3, "DAY", hashtag);
				num += tmpNum;
//				System.out.println("Midle Day of the date: " + dateInString3 + "; Number of Tweets: " + tmpNum);
			}
		} else if (startHour != endHour) {
			int difference = endHour - startHour;

			Node startHourNode = getHourNode(startTime);
			num += tweetNumNode(startHourNode, "HOUR", hashtag);
			Node endHourNode = getHourNode(endTime);
			num += tweetNumNode(endHourNode, "HOUR", hashtag);

			for (int i = 1; i < difference; i++) {
				String dateInString3 = monthToString(startMonth) + " " + (startDate + i) + " 00 "
						+ Integer.toString(startYear);
				num += addNodeTweet(dateInString3, "HOUR", hashtag);
			}
		} else {
			Node startHourNode = getHourNode(startTime);
			num += tweetNumNode(startHourNode, "HOUR", hashtag);
		}

		return num;
	}

	/**
	 * @author YAN Transfer the int Month to String with leading 0
	 */
	private static String monthToString(int month) {
		if (month < 10) {
			return "0" + month;
		} else {
			return Integer.toString(month);
		}
	}

	/**
	 * @author YAN
	 * @param time
	 * @return The Date type of given time.
	 */
	public static Date stringToDate(String time) {
		Date tmpDate = null;
		try {
			tmpDate = formatter.parse(time);
		} catch (java.text.ParseException e) {
			e.printStackTrace();
		}
		return tmpDate;
	}

	/**
	 * @author YAN Transfer the String date to Date type and return the number
	 *         of Tweets for the given date and nodeType's node
	 */
	private int addNodeTweet(String dateInString3, String nodeType, String hashtag) throws NoSuchObjectException {
		int num = 0;
		try {
			Date tmpDate = formatter.parse(dateInString3);
//			System.out.println("Date: "+tmpDate);
			Node tmpNode = null;
			if (nodeType.equals("HOUR")) {
				tmpNode = getHourNode(tmpDate);
			} else if (nodeType.equals("DAY")) {
				tmpNode = getDayNode(tmpDate);
			} else if (nodeType.equals("WEEK")) {
				tmpNode = getWeekNode(tmpDate);
			} else if (nodeType.equals("MONTH")) {
				tmpNode = getMonthNode(tmpDate);
			} else if (nodeType.equals("YEAR")) {
				tmpNode = getYearNode(tmpDate);
			} else {
				throw new IllegalArgumentException("Invalid node type.");
			}
			num += tweetNumNode(tmpNode, nodeType, hashtag);

		} catch (java.text.ParseException e) {
			e.printStackTrace();
		}
		return num;
	}

	class MoviePopulity {
		String movie;
		int num;

		public MoviePopulity(String name, int num) {
			movie = name;
			this.num = num;
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
		// get first node: reference node
		Node timeLine = getYearNode(time);
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

	public Node getYearNode(Date time) throws NoSuchObjectException {
		Node ref = graphDb.findNodes(DynamicLabel.label("REF")).next();
		// System.out.println("Got the REF node.");
		// get run node
		Node run = ref.getRelationships().iterator().next().getEndNode();
		Node timeLine = run.getSingleRelationship(RelationshipTypes.HAS_TIMELINE, Direction.OUTGOING).getEndNode();
		return timeLine;
	}
}
