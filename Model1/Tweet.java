package project;

import java.util.ArrayList;
import java.util.Date;

/**
 * tweet information
 * @author Yan
 *
 */
public class Tweet {

	private String[] hashtagList = CreateDatabase.hashtagList;
	
	private Date time;
	private String text;
	private String tweetId;
	private String retweetFromID;
	private String userId;
	private String userName;
	private String location;
	private ArrayList<String> hashtag;
	private boolean isvalid;

	public Tweet(){} 
	public Tweet(Date time, String Text, String TweetId, String RetweetFromID,
			String UserId, String UserName, String State) {
		this.time = time;
		this.text = Text;
		this.tweetId = TweetId;
		this.retweetFromID = RetweetFromID;
		this.userId = UserId;
		this.userName = UserName;
		this.location = State;
		this.hashtag = new ArrayList<String>();
		for (int i = 0; i < hashtagList.length; i++) {
			if (isContain(Text, hashtagList[i])) {
				hashtag.add(hashtagList[i]);
			}
		}
		if (hashtag.isEmpty()) {
			isvalid = false;
		} else {
			isvalid = true;
		}

		
		System.out.print("state: " + State + "\n");
		System.out.print(" hashtag: ");
		for (int i = 0; i < hashtag.size(); i++) {
			System.out.print(hashtag.get(i));
		}
		System.out.println("");
	}

	public Date getTime() {
		return time;
	}

	public String getText() {
		return text;
	}

	public String getTweetId() {
		return tweetId;
	}

	public String getRetweetFromID() {
		return retweetFromID;
	}

	public String getUserID() {
		return userId;
	}

	public String getUserName() {
		return userName;
	}

	public String getState() {
		return location;
	}

	public ArrayList<String> getHashtag() {
		return hashtag;
	}

	public boolean getisValid() {
		return isvalid;
	}

	private boolean isContain(String target, String pharse) {
		target = target.toLowerCase();
		pharse = pharse.toLowerCase();
		boolean isContain = false;
		for (int i = 0; i < target.length() - pharse.length(); i++) {
			if (target.substring(i, i + pharse.length()).equals(pharse)) {
				isContain = true;
			}
		}
		return isContain;
	}

}
