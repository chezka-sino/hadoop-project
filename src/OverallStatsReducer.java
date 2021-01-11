import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Create a Reducer class and implement the reduce function and the cleanup function.
//Input Key,Value: LongWritable, IntWritable
//Output Key,Value: Text, DoubleWritable

//This Reducer should follow the example we learned in class to compute the count and average values.
//The output should look as clarified in the instructions document.


public class OverallStatsReducer extends Reducer<LongWritable, IntWritable, Text, DoubleWritable> {
	
	private static int count = 0;
	private static int sum = 0;
	
	@Override
	public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) 
		throws IOException, InterruptedException{
		
		int freq = 0;
		while (values.iterator().hasNext()) {
			freq+=values.iterator().next().get();
		}
		count++;
		sum+=freq;
		
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text("Count"), new DoubleWritable(count));
		context.write(new Text("Average"), new DoubleWritable(sum*1.0/count));		
	}
	
}