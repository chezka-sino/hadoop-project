import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Create a Mapper class and implement the map function.
//Input Key,Value: LongWritable, Text
//Output Key,Value: Text, IntWritable

//In the map function, you need to split the content of each line and get the category and body column.
//Write to the context object the category as the key and the length (character count) of the text of the body column as the value.

public class CategoryStatsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] content = value.toString().split("\t");
		String category = content[1];
		String body = content[4];
		String[] words = body.split(" ");
		
		context.write(new Text(category), new IntWritable(words.length));
		
	}
	
}