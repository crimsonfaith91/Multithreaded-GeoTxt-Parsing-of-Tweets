import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalTime;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.*;

public class RunHttpURLConnector {
	/* CONSTANT */
	
	/* to detect tweets that yield empty result after being processed with GeoTxt */
	private final static int NULL_GEO_TXT_LENGTH = 42;
	
	/*user agent */
	private final static String USER_AGENT = "Mozilla/5.0";
	
	/* start tweet ID for each table */
	private final static String START_TWEET_ID = "8418333669";
	
	/* end tweet ID for each table */
	private final static String END_TWEET_ID = "450784605017501697";
	
	/* number of threads; affect efficiency a lot */
	private final static int THREAD_NUM = 100;
	
	/* amount of threads fetched from database for each fetch operation */
	private final static int FETCH_NUM = 10000;
	
	/* write thread's quota to write processed tweets into database */
	private final static int WRITE_NUM = 5000;
	
	/* fetch thread's quota to stop fetching tweets from database */
	/* the thread will sleep for a while when fetch queue has indicated number of tweets */
	private final static int FETCH_NUM_QUOTA = 50000;
	
	/* sleep duration for each thread; affect efficiency a lot */
	private final static int SLEEP_DURATION = 10;
	
	/* VARIABLES */
	
	/* threads */
	private static GeoTxtFetchThread geoTxtFetchThread = null;
	private static GeoTxtWriteThread geoTxtWriteThread = null;
	private static GeoTxtThread[] geoTxtThreads = null;
	
	/* current start tweet ID */
	private static String curStartTweetId = null;
	
	/* number of processed tweets so far */
	//private static int processed_tweet_num = 0;
	
	/* a synchronized queue to store the fetched tweet items */
	private static ConcurrentLinkedQueue<TweetItem> fetchedTweetItems = null;
	
	/* a synchronized queue to store the processed tweet items */
	private static ConcurrentLinkedQueue<TweetItem> processedTweetItems = null;
	
	/*
	 * encapsulate tweet ID and tweet text
	 */
	private static class TweetItem {
		String tweet_id, tweet_text;
		
		TweetItem(String tweet_id, String tweet_text) {
			this.tweet_id = tweet_id;
			this.tweet_text = tweet_text;
		}
	}
	
	/*
	 * fetch tweets from database
	 */
	private static class GeoTxtFetchThread implements Runnable {
		private Thread t;

		@Override
		public void run() {
			// TODO Auto-generated method stub
			while(true) {
				try {
					if(fetchedTweetItems.size() < FETCH_NUM_QUOTA) {
						try {
							getTweetItemsFromSQLServer();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					else {
						Thread.sleep(SLEEP_DURATION);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		public void start() {
			// TODO Auto-generated method stub
			if(t == null) {
				t = new Thread(this);
				t.start();
				//System.out.println("Fetch thread starts!");
			}
		}
		
		/* fetch tweets of defined range from database */
		private void getTweetItemsFromSQLServer() throws Exception {		
			try {				
				String username = "xxx";
				String password = "xxx";
				String url = "jdbc:sqlserver://xxx.xxx.purdue.edu";
	            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
	            Connection conn = DriverManager.getConnection(url, username, password);
	            Statement statement = conn.createStatement();
	            
	            String queryString = "SELECT TOP " + FETCH_NUM +
	            			  		 "[tweet_id]" +
	            			  		 ",[tweet_text]" +
	            			  		 "FROM [sns_viz].[dbo].[tweet_world_2014_1_3]" +
	            			  		 "WHERE [tweet_id] >= " + curStartTweetId + 
	            			  		 "ORDER BY [tweet_id]";
	            
	            ResultSet rs = statement.executeQuery(queryString);
	            
	            int cnt = 0;
	            
	            while (rs.next()) {	            	
	            	fetchedTweetItems.add(new TweetItem(rs.getString(1), rs.getString(2)));
	            	curStartTweetId = String.valueOf(Long.parseLong(rs.getString(1)) + 1);
	            	cnt++;
		        }
	            
	            System.out.println("Start Tweet ID: " + curStartTweetId + " Fetched " + cnt + " tweets");
	            
	            conn.close();
		    } catch (Exception e) {
		        e.printStackTrace();
		    }
		}
	}
	
	/*
	 * write tweets to database
	 */
	private static class GeoTxtWriteThread implements Runnable {
		private Thread t;
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			while(true) {
				try {
					if(processedTweetItems.size() > WRITE_NUM) {
						try {
							writeToSQLServer();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					else {
						Thread.sleep(SLEEP_DURATION);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		public void start() {
			if(t == null) {
				t = new Thread(this);
				t.start();
				//System.out.println("Write thread starts!");
			}
		}
		
		/* write processed tweets to database */
		private void writeToSQLServer() throws Exception {
			try {
				Vector<TweetItem> curTweetItemsToWrite = new Vector<TweetItem>();
				
				for(int i = 0; i < Math.min(WRITE_NUM, processedTweetItems.size()); i++) {
					curTweetItemsToWrite.add(processedTweetItems.poll());
				}	
				
				String url = "jdbc:sqlserver://xxx.xxx.purdue.edu"; 
	        		String username = "juntee";
	    			String password = "vaccine";
	            		Connection conn = DriverManager.getConnection(url, username, password); 
	            		Statement st = conn.createStatement(); 

	            		for(int i = 0; i < curTweetItemsToWrite.size(); i++) {
		            		try {
	            				if(curTweetItemsToWrite.get(i).tweet_text.length() > 0) {
	            					st.executeUpdate("INSERT INTO [sns_viz].[dbo].[tweet_geotxt] " + 
	            						"VALUES ('" + curTweetItemsToWrite.get(i).tweet_id + "', '" + curTweetItemsToWrite.get(i).tweet_text + "')");
	            				}
	            			}
	            			catch (Exception e) {
	            				continue;
	            			}
	            		}
	            
	            		conn.close();
	            
	            		System.out.println("Wrote " + curTweetItemsToWrite.size() + " tweets!");
	            
	            		//processed_tweet_num += curTweetItemsToWrite.size();          
	        	} catch (Exception e) {
	        		//do nothing
	            		//System.out.println(e.getMessage()); 
	        	} 
		}
	}
	
	/*
	 * process tweets using GeoTxt
	 */
	private static class GeoTxtThread implements Runnable {
		private Thread t;
		private int idx;
		
		GeoTxtThread(int idx) {
			this.idx = idx;
		}
		
		public void start() {
			if(t == null) {
				t = new Thread(this);
				t.start();
				//System.out.println("Geotxt thread starts!");
			}
		}
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			try {
				while(true) {
					if(fetchedTweetItems.size() > 0) {
						TweetItem curTweetItem = fetchedTweetItems.poll();
						
						String curGeoTxt = processTweetItemsWithGeoTxt(curTweetItem.tweet_text);
						
						if(curGeoTxt.length() > NULL_GEO_TXT_LENGTH) {
							curTweetItem.tweet_text = curGeoTxt;
						}
						else {
							curTweetItem.tweet_text = "";
						}
						
						processedTweetItems.add(curTweetItem);
					}
					else {
						Thread.sleep(SLEEP_DURATION);
					}
				}
			 } catch (Exception e) {
				 e.printStackTrace();
			 }
		}
		
		private String processTweetItemsWithGeoTxt(String tweet) throws Exception {
			try {
				tweet = URLEncoder.encode(tweet, "UTF-8");
				
				String url = "http://geotxt.org/api/1/geotxt.json?m=stanfords&q=" + tweet;		
				
				URL obj = new URL(url);
				HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		
				// geoTxt requires "GET"
				con.setRequestMethod("GET");
				con.setRequestProperty("User-Agent", USER_AGENT);
				
				int responseCode = con.getResponseCode();
				if(responseCode != 200) return ""; //if error, return empty string
					
				BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
				
				String inputLine;
				StringBuffer response = new StringBuffer();
		
				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				
				in.close();
				
				return response.toString();
			} catch (Exception e) { 
				//do nothing
				return "";
			}
		}
	}	
	
	/* print efficiency statistics */
	/*
	private static class printEfficiencyStatistics extends TimerTask {
		@Override
		public void run() {
			// TODO Auto-generated method stub
			System.out.println("------------------------------------------------");
			System.out.println("Fetch Queue Size: " + fetchedTweetItems.size());
			System.out.println("Write Queue Size: " + processedTweetItems.size());
			System.out.println("Processed Tweet Number: " + processed_tweet_num);
			System.out.println("------------------------------------------------");
		}
	}
	*/
	
	public static void main(String args[]) throws Exception {		
		/* initialize queue */
		fetchedTweetItems = new ConcurrentLinkedQueue<TweetItem>();
		processedTweetItems = new ConcurrentLinkedQueue<TweetItem>();
		
		/* initialize the threads */
		geoTxtFetchThread = new GeoTxtFetchThread();
		
		geoTxtWriteThread = new GeoTxtWriteThread();
		
		geoTxtThreads = new GeoTxtThread[THREAD_NUM];
		for(int i = 0; i < THREAD_NUM; i++) {
			geoTxtThreads[i] = new GeoTxtThread(i);
		}
		
		/* set current tweet ID range before beginning */
		curStartTweetId = START_TWEET_ID;
		
		/* start the threads */
		geoTxtFetchThread.start();
		geoTxtWriteThread.start();
		
		for(int i = 0; i < THREAD_NUM; i++) {
			geoTxtThreads[i].start();
		}
		
		//Timer timer = new Timer();
		//timer.schedule(new printEfficiencyStatistics(), 0, 60000);
		
		/* run the process */
		while(curStartTweetId.compareTo(END_TWEET_ID) <= 0) ;
	}   
}
