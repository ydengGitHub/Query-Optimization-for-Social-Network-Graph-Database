package project;

import java.rmi.NoSuchObjectException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class CreateDatabase {
	
	/**
	 * Main entry of this application.
	 * 
	 * @param args
	 */
	public int count = 1;

	private static String[] consumerKey = {"L1OalLzE0E5F0ypHMOg760zgD","gYkyLamr1jGUyxSV8yWNa6afn"};

	private static String[] consumerSecret = {"zgqQuwOPXm9YOqPtoAKIuBgIQcjG2uuSHYmz2FTy3vm8La7MO2","CQosNEPdCFPgrrpXm3yXqwDpVSzV8EiXQTEUmx6rkTfaJqFoXn"};

	private static String[] accessToken = {"171241381-ygBqDUyrAxls2oaKZszy4PbBZf06OfzbUKCWlman","2827231046-W3DJEaQpbm7Z39T819xKXTKIXDMH2BLdQsdTEIt"};

	private static String[] tokenSecret = {"RV2MjAUfeTpjQEtWHm8MFaQYWMmZnvLPRqY4FdjdAD9d1","eyPIRtTcZTkIYiE65iUwwxSQS9ItS4QpFvWYdY5wALH39"};
	

	
//	public static String[] hashtagList = { "#MockingjayPart2", "#TGITAwakens", "#SPECTRE", "#PeanutsMovie",
//			"#LoveTheCoopers", "#TheMartian", "#The33", "#Goosebumps", "#BridgeOfSpies", "#PRDP", "#HotelTransylvania2",
//			"#LastWitchHunter", "#TheNightBefore", "#TheSecretBookSeries","#Frozen2"," #TheForceAwakens"};
	public static String[] hashtagList = {"Mockingjay", "Frozen", "TGITAwakens", "SPECTRE", "PeanutsMovie",
			"LoveTheCoopers", "TheMartian", "The33", "Goosebumps", "BridgeOfSpies", "PRDP", "HotelTransylvania2",
			"LastWitchHunter", "TheNightBefore", "TheSecretBookSeries", "TheForceAwakens", "Krampus"};
		
	
	public static String[] locations={ "Unknown","North America","Europe","East Asia" };

	private static ArrayList<Location> locationList = new ArrayList<Location>();
	private static ArrayList<Tweet> tweetList = new ArrayList<Tweet>();
	private final static String DB_PATH = GraphDatabase.DB_PATH;
	
	//for query.until(endtime[i]) use format in "2015-12-01"
	private static String[] endtime = {"2015-11-30", "2015-12-01", "2015-12-02", "2015-12-03", 
								"2015-12-04", "2015-12-05", "2015-12-06",
								"2015-12-07", "2015-12-08"};

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws TwitterException,
		InterruptedException, NoSuchObjectException {
			initLocation(locationList);
			
			
			
		/*ConfigurationBuilder cb = new ConfigurationBuilder();

		cb.setDebugEnabled(true)
				.setOAuthConsumerKey("L1OalLzE0E5F0ypHMOg760zgD")
				.setOAuthConsumerSecret(
						"zgqQuwOPXm9YOqPtoAKIuBgIQcjG2uuSHYmz2FTy3vm8La7MO2")
				.setOAuthAccessToken(
						"171241381-ygBqDUyrAxls2oaKZszy4PbBZf06OfzbUKCWlman")
				.setOAuthAccessTokenSecret(
						"RV2MjAUfeTpjQEtWHm8MFaQYWMmZnvLPRqY4FdjdAD9d1");
		cb.setJSONStoreEnabled(true);

		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();*/
		
		
		GraphDatabase GraphDb = new GraphDatabase();
		
		//Check whether the database is empty or not
		GraphDatabaseService testDb;
		testDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
		boolean emp=false;
		try(Transaction ignored=testDb.beginTx();
				Result result=testDb.execute("Match (n:REF) Return n")){
				emp=result.hasNext();
				testDb.shutdown();
				if(!emp){					
					GraphDb.initGraphDataBase();
				}
				else
				{
					System.out.println("Database already exists...");
					System.out.println(result.next());
//					GraphDb.initGraphDataBase2();
				}
		}
		catch(Exception ex){
			System.out.println(ex);
		}
		
		
//		GraphDb.initGraphDataBase();
		
		//
		long i = 0;
		int m=0;

		String[] querryList=new String[hashtagList.length];
		for (int j = 0; j < querryList.length; j++) {
			querryList[j]=hashtagList[j]+" :)";
		}
		Twitter twitter = null;
//		for (int m = 0; m < locationList.size(); m++) {
			for (int k = 0; k < querryList.length; k++) {
				
				Query query = new Query(querryList[k]);
				query.setSince("2015-11-30");
				//query Nov 29 - 30
				query.setUntil(endtime[3]);
//				GeoLocation a = new GeoLocation(locationList.get(m)
//						.getLatitude(), locationList.get(m).getLongitude());
//				query.setGeoCode(a, locationList.get(m).getRadius(), "km");
//				query.setCount(100);
				if(k%2==0)
					twitter = auth(k/2);
				QueryResult result = twitter.search(query);
				do {
					List<Status> tweets = result.getTweets();
					for (Status tweet : tweets) {
						long j=1;
						long retweetID = 0;
						if (tweet.getRetweetedStatus() != null) {
							retweetID = tweet.getRetweetedStatus().getId();
						} else {
							retweetID = 0;
						}
						System.out.print("tweet = "+ i + "\n");
						long tweetID = tweet.getId();
						long userID = tweet.getUser().getId();
						Date tweetTime=tweet.getCreatedAt();
						System.out.print("Date = "+ tweetTime + "\n");
						String retweetIDStr = String.valueOf(retweetID);
						String tweetIDStr = String.valueOf(tweetID);
						String userIDStr = String.valueOf(userID);
						
						Tweet newTweet = new Tweet(tweet.getCreatedAt(),
								tweet.getText(), tweetIDStr, retweetIDStr,
								userIDStr, tweet.getUser()
										.getName(), locationList.get(m)
										.getName());
						GraphDb.createUserAndTweetNode(newTweet);
						Thread.sleep(2000);
//						Thread.sleep(1000);
						i = GraphDb.getTweetNodeNum();
						if(j%10000 ==0)
						{
							System.out.print("in" + "\n");
//							GraphDb.creatRelatioShipInTweets();
							break;
						}
						
					}
					query = result.nextQuery();
					if (query != null)
						result = twitter.search(query);
				} while (query != null);
			}
//		}
		
		GraphDb.creatRelatioShipInTweets();
		System.out.print("\n done \n");
	}
	
	private static Twitter auth(int kth){
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		int i = consumerKey.length;
		cb.setDebugEnabled(true)
				.setOAuthConsumerKey(consumerKey[kth%i])
				.setOAuthConsumerSecret(consumerSecret[kth%i])
				.setOAuthAccessToken(accessToken[kth%i])
				.setOAuthAccessTokenSecret(tokenSecret[kth%i]);
		cb.setJSONStoreEnabled(true);

		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		return twitter;
	}


	private static void initLocation(ArrayList<Location> location) {
		location.add(new Location(0, 0, 0, locations[0]));
		location.add(new Location(39.639537, -110.390625, 4589, locations[1]));
		location.add(new Location(53.014783, -4.921875, 2155, locations[2]));
		location.add(new Location(42.423456, 135.703131, 1395, locations[3]));
	}

}
