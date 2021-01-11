import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Create a Reducer class and implement the reduce function.
// Input Key,Value: Text, Article
// Output Key,Value: Text, Article

// This Reducer does nothing but passing on the key values through the context object. 
// Simply iterate through the values and write the key,value pairs to the context object.

public class DocumentParsingReducer extends Reducer<Text, Article, Text, Article> {

	  public void reduce(Text key, Iterable<Article> values, Context context)
	      throws IOException, InterruptedException {
		  
		  while (values.iterator().hasNext()) {
			  context.write(key, values.iterator().next());
		  }
		  
	  }
	}