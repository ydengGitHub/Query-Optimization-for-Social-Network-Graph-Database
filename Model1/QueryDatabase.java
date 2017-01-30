package project;

import java.rmi.NoSuchObjectException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.tooling.GlobalGraphOperations;

import project.GraphDatabase.RelationshipTypes;


/**
 * 
 * query the graph database
 * @author Yan
 *
 */
public class QueryDatabase {


	private String[] hashtagString =CreateDatabase.hashtagList;
	
	private String[] locations = CreateDatabase.locations;
	
	public static SimpleDateFormat formatter = new SimpleDateFormat("MM dd HH yyyy");
	
//    enum RelationshipTypes implements RelationshipType
//	{
//	     retweetFrom, hasTweeted, NEXT_LEVEL, NEXT_HOUR
//	}
	
	ArrayList<String> hashtagList = new ArrayList<String>();
	ArrayList<String> locationList = new ArrayList<String>();
	
    private ArrayList<Label> hashtagLabel = new ArrayList<Label>();
	
	//connect graph database
	private final String DB_PATH = GraphDatabase.DB_PATH;
	public GraphDatabaseService graphDb;
	
	public void initQuery(){
		
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
//		System.out.println("in Graph database initQuery(): "+ graphDb.isAvailable(2000));
		for(int i = 0; i < hashtagString.length; i++){
			hashtagList.add(hashtagString[i]);
		}
		for(int i = 0; i < locations.length; i++){
			locationList.add(locations[i]);
		}

	}
	
	
	private Node isContainUser(String userId) {
    	
		for(int i = 0; i < hashtagList.size(); i++){
    		Label label = DynamicLabel.label( hashtagList.get(i) );
    		hashtagLabel.add(label);
    	}
		
		try (Transaction tx = graphDb.beginTx()) {
			for (int i = 0; i < hashtagLabel.size(); i++) { // find user by
															// userId
				ResourceIterator<Node> users = graphDb.findNodesByLabelAndProperty(hashtagLabel.get(i),
								"UserId", userId).iterator(); 
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
	 * @author Daolin
	 * @param time
	 * @return
	 * @throws NoSuchObjectException
	 */
	public Node getHourNode(Date time) throws NoSuchObjectException{
		// search for the right hour node
		// get first node: reference node
		Label lb = DynamicLabel.label("REF");
//		System.out.println(lb.toString());
		
//		System.out.println(graphDb.isAvailable(2000));
//		Node ref = graphDb.getNodeById(0);
		Node ref = graphDb.findNodes(lb).next();
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
//		System.out.println("Hour number: " + hour);
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
	/**
	 * @author Daolin
	 * @param hourNode
	 * @param hashtag
	 * @return
	 */
	//TODO debug 
	public int tweetNumHour(Node hourNode, String hashtag){
		int num = 0;
		Iterator<Relationship> tweetIt = hourNode.getRelationships(RelationshipTypes.HOUR_TWEET, Direction.OUTGOING).iterator();
//		System.out.println("tweetNumHour tweetIt exist? " + (tweetIt != null));
//		System.out.println("tweetNumHour tweetIt hasnext? " + (tweetIt.hasNext()));
		while(tweetIt.hasNext()){
			Iterator<Label> tags = tweetIt.next().getEndNode().getLabels().iterator();
			while(tags.hasNext()){
				String label = tags.next().name();
//				System.out.println("label get in tweetNumHour " + label);
				if(label.equals(hashtag)){
					num++;
				}
			}
		}
//		if (num > 0)
//		System.out.println("tweetNumHour(" + hashtag+") = " + num);

		return num;
	}
	public class tweetIdandDegreeNumforQuery{
		private int degreeNum = 0;		//degree
		private String tweetId;		//tweet id 
		public tweetIdandDegreeNumforQuery(){}
		
		public void setdegreeNum(int d){
			degreeNum = d;
		}
		
		public void settweetId(String s){
			tweetId = s;
		}
		
		public int getdegreeNum(){
			return degreeNum;
		}
		
		public String gettweetId(){
			return tweetId;
		}
		
	}
	
	/**
	 * @author Wei
	 * @param month
	 * @param week
	 * @param day
	 * @param hour
	 * @param hashtag
	 * @return
	 */
	public Long getTweetNumAtHour(int month, int week, int day, int hour, String hashtag)
	{
		
		String cypherQuery = "MATCH (m:Month{MonthOfYear:" +month+ "})-->(w:Week{WeekOfMonth:" +week+ "})-->(d:Day{DayOfWeek:" +day+ "})-->(h:Hour{HourOfDay:" +hour+ "})-->(n:" +hashtag+ ") return count(n)";
		Result result = graphDb.execute(cypherQuery);	
		Map<String,Object> row = result.next();
		Long n = (long) -1;
		for(Entry<String,Object> column : row.entrySet())
		{
			
			n=(Long) column.getValue(); 
			
		}
		return n;
	}

	

	/**
	 * @author Daolin
	 * @param start
	 * @param end
	 * @param k
	 * @return The best k movies' name, number of Tweets in given Time frame
	 * @throws NoSuchObjectException
	 */
	public List<MoviePopulity> query1(String start, String end, int k) throws NoSuchObjectException{
		List<MoviePopulity> res = new ArrayList<>();
		PriorityQueue<MoviePopulity> pq = new PriorityQueue<>(k, 
											(MoviePopulity x,MoviePopulity y) -> x.num - y.num);
		for (int i = 0; i < hashtagString.length; i++){
			MoviePopulity mv = new MoviePopulity(hashtagString[i],
									query3(start, end, hashtagString[i]));
			System.out.println("query 3 result of " + mv.movie + " tweet No = " + mv.num);
			pq.add(mv);
			if (i >= k){
				pq.poll();
			}
		}
		while(!pq.isEmpty()){
			res.add(pq.poll());
		}
		return res;		
	}
	/**
	 * @author Daolin
	 * @param start
	 * @param end
	 * @param k
	 * @return The worst k movies' name, number of Tweets in given Time frame
	 * @throws NoSuchObjectException
	 */
	public List<MoviePopulity> query2(String start, String end, int k) throws NoSuchObjectException{
		List<MoviePopulity> res = new ArrayList<>();
		PriorityQueue<MoviePopulity> pq = new PriorityQueue<>(k, 
											(MoviePopulity x,MoviePopulity y) -> y.num - x.num);
		for (int i = 0; i < hashtagString.length; i++){
			MoviePopulity mv = new MoviePopulity(hashtagString[i],
									query3(start, end, hashtagString[i]));
			pq.add(mv);
			if (i >= k){
				pq.poll();
			}
		}
		while(!pq.isEmpty()){
			res.add(pq.poll());
		}
		return res;		
	}
	class MoviePopulity {
		String movie;
		int num;
		public MoviePopulity(String name, int num){
			movie = name;
			this.num = num;
		}
	}
	
	/**
	 * @author Daolin
	 * @param start
	 * @param end
	 * @param hashtag
	 * @return The number of Tweets of the given movie in given Time frame
	 * @throws NoSuchObjectException 
	 * Query 3
	 */
	public int query3(String startTime, String endTime, String hashtag) throws NoSuchObjectException{
		
		Date start=stringToDate(startTime);
		Date end=stringToDate(endTime);
		Node startNode = getHourNode(start);
		Node endNode = getHourNode(end);
		int num = 0;
		Node cur = startNode;
		while(!cur.equals(endNode)){
			num += tweetNumHour(cur, hashtag);
			cur = cur.getSingleRelationship(RelationshipTypes.NEXT_HOUR, Direction.OUTGOING).getEndNode();
		}
		
		return num + tweetNumHour(cur, hashtag);
	}
	
	
	/**
	 * @author Wei
	 * @param hashtag
	 * @return The best day of the given Movie
	 * @throws NoSuchObjectException
	 */
	public String query4(String hashtag) throws NoSuchObjectException
	{
		Node hourNode = getHourNode(stringToDate("01 01 00 2015"));
		
		int stop=0;
		
		int	maxDayTweets=0;
		Node maxHourNode=hourNode;
		Node preHourNode=hourNode;
		
		
		for(int d=0; d<12*5*7; d++)
		{
			int count=0;
			for(int h=0; h<24; h++)
			{
				if(tweetNumHour(hourNode, hashtag)>0)
				{
					count = count + tweetNumHour(hourNode, hashtag);
					preHourNode=hourNode;
				}
				
				if(stop==9900)
				{
					break;
				}
				
				hourNode = hourNode.getSingleRelationship(RelationshipTypes.NEXT_HOUR, Direction.OUTGOING).getEndNode();
				stop++;
				
			}
			
			if(maxDayTweets<=count)
			{
				maxDayTweets = count;
				maxHourNode = preHourNode;
				
			}
			if(stop==9900)
			{
				break;
			}
		}
		
		Node maxDay=maxHourNode.getRelationships(RelationshipTypes.HOUR_TWEET,Direction.OUTGOING).iterator().next().getEndNode();
		
		Calendar c =Calendar.getInstance();
		c.setTimeInMillis((long) maxDay.getProperty("Time"));
		
		return "Year: 2015, "+"Month:" + c.get(Calendar.MONTH) + ", " +"Day:" + c.get(Calendar.DAY_OF_MONTH);
		//return maxDayTweets;
	}
	/**
	 * @author Wei
	 * @param hashtag
	 * @return The best week of the given Movie
	 * @throws NoSuchObjectException
	 */

	public String query5(String hashtag) throws NoSuchObjectException 
	{
		
		Node hourNode = getHourNode(stringToDate("01 01 00 2015"));
		
		int stop =0;
		
		
		int	maxWeekTweets=0;
		Node maxHourNode=hourNode;
		Node preHourNode=hourNode;
		
		
		for(int w=0; w<60; w++)
		{
			int count=0;
			
			for(int d=0; d<7; d++)
			{
				for(int h=0; h<24; h++)
				{
					if(tweetNumHour(hourNode, hashtag)>0)
					{
						count = count + tweetNumHour(hourNode, hashtag);
						preHourNode=hourNode;
					}
					
					if(stop==9900)
					{
						break;
					}
					
					
					hourNode = hourNode.getSingleRelationship(RelationshipTypes.NEXT_HOUR, Direction.OUTGOING).getEndNode();
					stop++;
				}
				if(stop==9900)
				{
					break;
				}
			}
			
			if(maxWeekTweets<=count)
			{
				maxWeekTweets = count;
				maxHourNode = preHourNode;
				
			}
			if(stop==9900)
			{
				break;
			}
		}
		
		Node maxWeek=maxHourNode.getRelationships(RelationshipTypes.HOUR_TWEET,Direction.OUTGOING).iterator().next().getEndNode();
		
		Calendar c =Calendar.getInstance();
		c.setTimeInMillis((long) maxWeek.getProperty("Time"));
		
		return "Year: 2015, "+"Month:" + c.get(Calendar.MONTH) + ", " +"Week:" + c.get(Calendar.WEEK_OF_MONTH);
		//return maxWeekTweets;
	}
	
	private static Date stringToDate(String time){
		Date tmpDate=null;
		try {
			tmpDate = formatter.parse(time);
		} catch (java.text.ParseException e) {
			e.printStackTrace();
		}
		return tmpDate;
	}
	
	
	
}
