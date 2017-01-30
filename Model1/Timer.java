package project;

/**
 * 
 * used to calculate the time of each query
 * @author Yan
 *
 */

public class Timer {

	public long start = 0;
	public long end = 0;
	public long[] average_20 = new long[20];
	
	
	public long startTimer(){
		this.start = System.currentTimeMillis();
		return this.start;
	}
	
	public long endTimer(){
		this.end = System.currentTimeMillis();
		return this.end;
	} 
	
	public long setAverage(long start, long end, int i){
		this.average_20[i] = end - start;
		return this.average_20[i];
	}
	
	public long averageTime(){
		long s = 0;
		for(int i = 0; i < 20; i++){
			s += average_20[i];
		}
		
		s = s / 20;
		
		return s;
	}
}
