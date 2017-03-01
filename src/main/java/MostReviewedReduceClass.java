import java.io.IOException;
import java.util.Iterator;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MostReviewedReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	@Override 
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int rate = 0,index = 0, userId=0,count = 0;
		double counts=0;
		Iterator<IntWritable> valueIt = values.iterator();
		HashMap<Integer, Double> userNumOfRate = new HashMap<Integer, Double>(); //number of user rating 2
		
		while(valueIt.hasNext()){ //reading TrainingRatings
			index = valueIt.next().get();
			userId = valueIt.next().get();
			rate = valueIt.next().get();
			if(userNumOfRate.containsKey(userId)){
				counts = userNumOfRate.get(userId);
				userNumOfRate.put(userId, ++counts);
			}
			else {
				userNumOfRate.put(userId, 1.0);
			}
		}
		sort s2 = new sort(userNumOfRate);
		TreeMap<Integer, Double> sorted_Num = new TreeMap<Integer, Double>(s2);
		
		sorted_Num.putAll(userNumOfRate);

		context.write(key, new IntWritable());
	}
}
