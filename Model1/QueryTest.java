package project;

import java.io.FileWriter;
import java.io.IOException;
import java.rmi.NoSuchObjectException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Transaction;

import project.QueryDatabase.MoviePopulity;

public class QueryTest {

	public static void main(String[] args) throws IOException {
		QueryDatabase queryInstance = new QueryDatabase();
		queryInstance.initQuery();

		FileWriter writer = new FileWriter("query_results.txt", true);
		java.util.Date date = new java.util.Date();
		writer.write("Com S 561 query test\n");
		writer.write("Database\t" + GraphDatabase.DB_PATH + "\n");
		writer.write("Time\t" + new Timestamp(date.getTime()).toString() + "\n");
		writer.write("Model_ID\tQuery_ID\tExperiment_ID\ttime\n");
		int model = 2;
		int qid = 1;
		int repeat=10;
		
		// Query 1
		for (int k = 0; k < repeat; k++) {
			try (Transaction tx = queryInstance.graphDb.beginTx()) {
				Timer timer = new Timer();
				long startTime = timer.startTimer();
				System.out.println("Top movies:");
				List<MoviePopulity> results = queryInstance.query1("01 01 00 2015", "12 31 23 2015", 5);
				for (int i = results.size() - 1; i >= 0; i--) {
					System.out.println("\t\t" + results.get(i).movie + " has " + results.get(i).num + " of tweets.");
				}
				long endTime = timer.endTimer();
				long usedTime = endTime - startTime;
				System.out.println(k + "th round of query 1 " + "Used Time: " + usedTime);
				writer.append(model + "\t" + qid + "\t" + k + "\t" + usedTime + "\n");
				tx.success();
			}
		}
		++qid;

		// Query 2
		for (int k = 0; k < repeat; k++) {
			try (Transaction tx = queryInstance.graphDb.beginTx()) {
				Timer timer = new Timer();
				long startTime = timer.startTimer();
				System.out.println("Bottom movies:");
				List<MoviePopulity> results = queryInstance.query2("01 01 00 2015", "12 31 23 2015", 5);
				for (int i = results.size() - 1; i >= 0; i--) {
					System.out.println("\t\t" + results.get(i).movie + " has " + results.get(i).num + " of tweets.");
				}
				long endTime = timer.endTimer();
				long usedTime = endTime - startTime;
				System.out.println(k + "th round of query 1 " + "Used Time: " + usedTime);
				writer.append(model + "\t" + qid + "\t" + k + "\t" + usedTime + "\n");
				tx.success();
			}
		}
		++qid;

		// Query 3
		for (int k = 0; k < repeat; k++) {
			try (Transaction tx = queryInstance.graphDb.beginTx()) {
				Timer timer = new Timer();
				long startTime = timer.startTimer();
				// System.out.println("Top movies:");
				int results = queryInstance.query3("01 01 00 2015", "12 31 23 2015", "Mockingjay");
				System.out.println("query 3 results = " + results);
				long endTime = timer.endTimer();
				long usedTime = endTime - startTime;
				System.out.println("Used Time: " + usedTime);
				writer.append(model + "\t" + qid + "\t" + k + "\t" + usedTime + "\n");
				tx.success();
			}
		}
		++qid;

		// Query 4
		for (int k = 0; k < repeat; k++) {
			try (Transaction tx = queryInstance.graphDb.beginTx()) {
				Timer timer = new Timer();
				long startTime = timer.startTimer();
				// System.out.println("Top movies:");
				String results = queryInstance.query4("SPECTRE");
				System.out.println("query 4 results = " + results);
				long endTime = timer.endTimer();
				long usedTime = endTime - startTime;
				System.out.println("Used Time: " + usedTime);
				writer.append(model + "\t" + qid + "\t" + k + "\t" + usedTime + "\n");
				tx.success();
			}
		}
		++qid;
		// Query 5
		for (int k = 0; k < repeat; k++) {
			try (Transaction tx = queryInstance.graphDb.beginTx()) {
				Timer timer = new Timer();
				long startTime = timer.startTimer();
				// System.out.println("Top movies:");
				String results = queryInstance.query5("SPECTRE");
				System.out.println("query 5 results = " + results);
				long endTime = timer.endTimer();
				long usedTime = endTime - startTime;
				System.out.println("Used Time: " + usedTime);
				writer.append(model + "\t" + qid + "\t" + k + "\t" + usedTime + "\n");
				tx.success();
			}
		}

		writer.close();

		// try (Transaction tx = queryInstance.graphDb.beginTx()) {
		// TimeTest timer = new TimeTest();
		// long startTime = timer.startTimer();
		// System.out.println("best day:"+queryInstance.query4("SPECTRE"));
		// long endTime = timer.endTimer();
		// long usedTime = endTime - startTime;
		// System.out.println("Used Time: " + usedTime);
		//
		// tx.success();
		// }
		//
		// try (Transaction tx = queryInstance.graphDb.beginTx()) {
		// TimeTest timer = new TimeTest();
		// long startTime = timer.startTimer();
		// System.out.println("best week:"+queryInstance.query5("SPECTRE"));
		// long endTime = timer.endTimer();
		// long usedTime = endTime - startTime;
		// System.out.println("Used Time: " + usedTime);
		//
		// tx.success();
		// }

	}

}
