import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MovieCount extends Configured implements Tool{

	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MovieCount(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		if(args.length != 2) {	
			System.err.printf("Usage: %s needs three arguments, input and output files\n", getClass().getSimpleName());
				return -1;
		}

		//first job -- top 10 movies that have the highest average ratings
		Job job1 = new Job();
		job1.setJarByClass(MovieCount.class);
		job1.setJobName("MovieCounter");

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		job1.setMapperClass(MapClass.class);
		job1.setReducerClass(HighestAverageReduceClass.class);

		int returnValue = job1.waitForCompletion(true) ? 0:1;

		if (job1.isSuccessful()) { 
			System.out.println("Job was successful");
		} else if(!job1.isSuccessful()) {
			System.out.println("Job was not successful");
		}
		
		//second job -- top 10  users that posted the most reviews 
		Job job2 = new Job();
		job2.setJarByClass(MovieCount.class);
		job2.setJobName("MovieCounter");

		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setMapperClass(MapClass.class);
		job2.setReducerClass(MostReviewedReduceClass.class);

		returnValue = job2.waitForCompletion(true) ? 0:1;

		if (job2.isSuccessful()) { 
			System.out.println("Job was successful");
		} else if(!job2.isSuccessful()) {
			System.out.println("Job was not successful");
		}
		

		return returnValue;
	}
}