import java.io.IOException;
import java.util.Iterator;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class HighestAverageReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	@Override 
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int rate = 0,index = 0, userId=0,count = 0;
		//double counts=0;
		Iterator<IntWritable> valueIt = values.iterator();
		HashMap<Integer, Integer> ratings = new HashMap<Integer,Integer>(); //Rating Total 1
		HashMap<Integer, Integer> counterRate = new HashMap<Integer, Integer>(); //Number of Ratings for a movie 1
		HashMap<Integer, Double> avgRate = new HashMap<Integer, Double>();//average  rating score 1
		//HashMap<Integer, Double> userNumOfRate = new HashMap<Integer, Double>(); //number of user rating 2
		
		while(valueIt.hasNext()){ //reading TrainingRatings
			System.out.print(valueIt.next().get());
			System.out.print(valueIt.next().get());
			index = valueIt.next().get();
			userId = valueIt.next().get();
			rate = valueIt.next().get();
			if(ratings.containsKey(index)){
				rate += ratings.get(index);
				count = counterRate.get(index);
				ratings.put(index, rate);
				counterRate.put(index, ++count);
			}
			else {
				ratings.put(index, rate);
				counterRate.put(index, 1);
			}
			/*if(userNumOfRate.containsKey(userId)){
				counts = userNumOfRate.get(userId);
				userNumOfRate.put(userId, ++counts);
			}
			else {
				userNumOfRate.put(userId, 1.0);
			}*/
		}
		for(int indx : ratings.keySet()){ 
			int sum = ratings.get(indx);
			avgRate.put(indx, ((double) sum/(double) counterRate.get(indx)));
		}
		
		sort s1 = new sort(avgRate);
		//sort s2 = new sort(userNumOfRate);
		TreeMap<Integer, Double> sorted_Avg = new TreeMap<Integer, Double>(s1);
		//TreeMap<Integer, Double> sorted_Num = new TreeMap<Integer, Double>(s2);
		
		sorted_Avg.putAll(avgRate);
		//sorted_Num.putAll(userNumOfRate);
		
		context.write(key, new IntWritable());
	}
}

class sort implements Comparator<Integer> {
	Map<Integer,Double> base;
	public sort(Map<Integer, Double> base) {
        this.base = base;
    }
	
	public int compare(Integer o1, Integer o2) {
		// TODO Auto-generated method stub
		if (base.get(o1) >= base.get(o2)) {
            return -1;
        } else {
            return 1;
        }
	}	
}