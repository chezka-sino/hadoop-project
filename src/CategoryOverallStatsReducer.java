import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Create a Reducer class and implement the reduce function and the cleanup function.
//Input Key,Value: Text, Text 
//Output Key,Value: Text, Text 

//This Reducer should follow the example we learned in class to compute the stats as described in the instructions document for this part.

public class CategoryOverallStatsReducer extends Reducer<Text, Text, Text, Text> {
	
	public static Double min_count = Double.MAX_VALUE;
	public static Double max_count = Double.MIN_VALUE;
	public static int sum = 0;
	public static int count = 0;
	public static String min_cat;
	public static String max_cat;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) {
		Double ave = 0.0;
		
		while (values.iterator().hasNext()) {
			Text vals = values.iterator().next();
			String[] nums = vals.toString().split(",");
			ave = Double.valueOf(nums[0]);
			
			count++;
			sum+=Integer.valueOf(nums[1]);
			
			if (ave > max_count) {
				max_count = ave;
				max_cat = key.toString();

			}
			if (ave < min_count) {
				min_count = ave;
				min_cat = key.toString();

			}

		}
		
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		String average = String.valueOf(sum*1.0/count);
		context.write(new Text("Average Document Per Category"), new Text(average));
		context.write(new Text("Category With Max Avg Body Word Count"), new Text(max_cat));
		context.write(new Text("Category With Min Avg Body Word Count"), new Text(min_cat));
	}
	
}