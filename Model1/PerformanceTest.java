package project;

public class PerformanceTest {
	
/*	public static void main(String[] args){
		QueryDatabase QueryInstance = new QueryDatabase();
		QueryInstance.initQuery();
		Time_test[] tt = new Time_test[8];
		for(int j = 0; j < 8; j++)
			tt[j] = new Time_test();
		
//every time, you just can test one query, because of the accuracy problem
		for (int j = 0; j < 1; j++) {

			{
				long s = tt[0].startTimer();

				String abc = QueryInstance.returnKhsInState("New York", 5);
				System.out.print("return = " + abc);

				long e = tt[0].endTimer();
				long a = tt[0].setAverage(s, e, j);
				System.out.println("\ntime" + j + " = " + a);
			}

			{
				long s = tt[1].startTimer();

				String abc = QueryInstance.listNumofHsInState("#healthcare");
				System.out.print("return = " + abc);

				long e = tt[1].endTimer();
				long a = tt[1].setAverage(s, e, j);
				System.out.println("\ntime" + j + " = " + a);
			}

			{
				long s = tt[2].startTimer();

				long abc = QueryInstance.returnNumofTweetforHs("#healthcare");
				System.out.print("return = " + abc);

				long e = tt[2].endTimer();
				long a = tt[2].setAverage(s, e, j);
				System.out.println("\ntime" + j + " = " + a);
			}

			{
				long s = tt[3].startTimer();

				String abc = QueryInstance.returnKforHsInEachState(
						"#healthcare", "New York", 5);
				System.out.print("return = " + abc);

				long e = tt[3].endTimer();
				long a = tt[3].setAverage(s, e, j);
				System.out.println("\ntime" + j + " = " + a);
			}

			{
				long s = tt[4].startTimer();

				String abc = QueryInstance.showContentofTopRetweetforHsInState(
						"#Ebola", "New York");
				System.out.print("return = " + abc);

				long e = tt[4].endTimer();
				long a = tt[4].setAverage(s, e, j);
				System.out.println("\ntime" + j + " = " + a);
			}

			{
				long s = tt[5].startTimer();

				String abc = QueryInstance.findABCinEachState("#healthcare", "#healthcare", "#healthcare", "New York");
				 System.out.print("return = " + abc);

				long e = tt[5].endTimer();
				long a = tt[5].setAverage(s, e, j);
				System.out.println("\ntime" + j + " = " + a);
			}

			{
				long s = tt[6].startTimer();

				String abc = QueryInstance.listMostRetweetUserforHsInState("#healthcare", "New York");
				 System.out.print("return = " + abc);

				long e = tt[6].endTimer();
				long a = tt[6].setAverage(s, e, j);
				System.out.println("\ntime" + j + " = " + a);
			}
			
			{
				long s = tt[7].startTimer();

				String abc = QueryInstance.TweetsofAspecifiedUser("539766591433371650");
				System.out.print("return = " + abc);

				long e = tt[7].endTimer();
				long a = tt[7].setAverage(s, e, j);
				System.out.println("\ntime" + j + " = " + a);
			}

		}	
	}
	*/

}
