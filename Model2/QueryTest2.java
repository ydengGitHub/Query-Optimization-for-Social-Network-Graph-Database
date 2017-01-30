package project;

import java.rmi.NoSuchObjectException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import project.GraphDatabase.RelationshipTypes;
import project.QueryDatabase.MoviePopulity;


/**
 * Test the queries and measure the time. 
 * @author YAN
 *
 */
public class QueryTest2 {
	
	private static QueryDatabase queryInstance = new QueryDatabase();
	

	public static void main(String[] args) throws NoSuchObjectException {
		queryInstance.initQuery();

		try (Transaction tx = queryInstance.graphDb.beginTx()) {
			Timer timer = new Timer();
			long startTime = timer.startTimer();
		
//			testQuery1();			
//			testQuery2();
			testQuery3();
//			testQuery4();
//			testQuery5();
			
			// general code
			long endTime = timer.endTimer();
			long usedTime = endTime - startTime;
			System.out.println();
			System.out.println("Used Time: " + usedTime);

			tx.success();
		}
	}
	
	private static void testQuery1() throws NoSuchObjectException{
		 
		 List<MoviePopulity> results = queryInstance.query1("01 01 00 2015", "12 31 23 2015", 5);
		 for (int i = results.size() -1 ; i >= 0 ; i--) {
				System.out.println(results.get(i).movie + " has " + results.get(i).num +  " of tweets.");
			}
	}
	
	private static void testQuery2() throws NoSuchObjectException{
		 
		 List<MoviePopulity> results = queryInstance.query2("01 01 00 2015", "12 31 23 2015", 5);
		 for (int i = results.size() -1 ; i >= 0 ; i--) {
				System.out.println(results.get(i).movie + " has " + results.get(i).num +  " of tweets.");
			}
	}

	private static void testQuery3() throws NoSuchObjectException{
		 queryInstance.query3("01 01 00 2015", "12 31 23 2015", "MockingjayPart2");
	}
	
	private static void testQuery4() throws NoSuchObjectException{
		System.out.println(queryInstance.query4("MockingjayPart2")); 

	}
	
	
	private static void testQuery5() throws NoSuchObjectException{
		System.out.println(queryInstance.query5("MockingjayPart2")); 

	}

	
	private static void getMonthNodes(){
		Node ref = queryInstance.graphDb.findNodes(DynamicLabel.label("REF")).next();
		System.out.println("REF node is null: " + (ref == null));
		// get run node
		System.out.println("RUN iterator has next: "+ ref.getRelationships().iterator().hasNext());
		Node run = ref.getRelationships().iterator().next().getEndNode();
		
		System.out.println("RUN node is null: " + (run == null));
		// get year node
		Node timeLine = run.getSingleRelationship(RelationshipTypes.HAS_TIMELINE, Direction.OUTGOING)
				.getEndNode();
		System.out.println("Year node is null: " + (timeLine == null));
		System.out.println("Year: "+timeLine.getProperty("year"));
		//get month nodes
		Iterator<Relationship> tweetIt = null;
//		tweetIt = timeLine.getRelationships(RelationshipTypes.NEXT_LEVEL, Direction.OUTGOING)
//				.iterator();
		tweetIt=timeLine.getRelationships(Direction.OUTGOING, RelationshipTypes.NEXT_LEVEL).iterator();
		while(tweetIt.hasNext()){
		Relationship rel=tweetIt.next();
		System.out.println( "Relationship Type: "+(rel.getType()));
		Node  firstMonth= rel.getEndNode();
		System.out.println( "Endnote Label: "+(firstMonth.getLabels()));
		System.out.println(firstMonth.getProperty("MonthOfYear"));
		}
		
//		System.out.println("Month Iterator is empty: " + tweetIt.hasNext());
	}
	
	private static Node getDayNode() throws NoSuchObjectException{
		Node tmp=null;
		Date date=QueryDatabase.stringToDate("11 26 01 2015");
		tmp=queryInstance.getDayNode(date);
		System.out.println(tmp.getLabels());
		return tmp;
	}
	
	private static Node getMonthNode() throws NoSuchObjectException{
		Node tmp=null;
		Date date=QueryDatabase.stringToDate("11 26 01 2015");
		System.out.println("Date: "+date);
		tmp=queryInstance.getMonthNode(date);
		System.out.println(tmp.getLabels());
		System.out.println("Month of the Year: "+tmp.getProperty("MonthOfYear"));
		return tmp;
	}
	
	private static int testTweetNumNode(Node givenNode){
		int num=queryInstance.tweetNumNode(givenNode, "MONTH", "Frozen");
		return num;
	}
	
	private static void getRelationships(Node givenNode){
		Iterator<Relationship> tweetIt = null;
		tweetIt = givenNode.getRelationships(Direction.INCOMING, RelationshipTypes.MOVIE_MONTH).iterator();
//		tweetIt = givenNode.getRelationships().iterator();
		while(tweetIt.hasNext()){
			Relationship rel=tweetIt.next();
			System.out.println( "Relationship Type: "+(rel.getType()));
			Node  movie= rel.getEndNode();
			System.out.println( "Endnote Label: "+(movie.getLabels()));
			}
	}
	
	
	private static int testTweetNumFrame() throws NoSuchObjectException{
		Date date1=QueryDatabase.stringToDate("01 01 00 2015");
		Date date2=QueryDatabase.stringToDate("12 31 23 2015");
		
		int num=queryInstance.tweetNumTimeframe(date1, date2, "Frozen");
		return num;
	}
}
