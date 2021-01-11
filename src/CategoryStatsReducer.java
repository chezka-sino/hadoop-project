import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Create a Reducer class and implement the reduce function.
//Input Key,Value: Text, IntWritable
//Output Key,Value: Text, Text

//This Reducer should compute the count and average body size for each category. 
//Write to the context object the category as the key and "AverageBodySize,DocCount" as the value (this is one string with the two values separated by a comma). 

public class CategoryStatsReducer extends Reducer<Text, IntWritable, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		int sum = 0;
		int total = 0;
		
		for (IntWritable value:values) {
			sum+=value.get();
			total++;
		}
		
		context.write(key, new Text(String.valueOf(sum*1.0/total) + "," + 
				String.valueOf(total)));
		
	}
	
}