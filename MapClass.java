import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWriteable;
import org.apache.hadoop.io.LongWriteable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

publib class MapClass extends Mapper<LongWriteable, Text, Text, IntWriteable>{
	private final static IntWriteable one = new IntWriteable(1);
	private Text word = new Text();

	@Override 
	protected void map(LongWriteable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, " ");

		while(st.hasMoreTokens()){
			word.set(st.nextToken());
			context.write(word,one);
		}
	}
} 