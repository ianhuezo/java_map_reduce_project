import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//Create a Reducer class and implement the reduce function.
//Input Key,Value: Text, IntWritable
//Output Key,Value: Text, Text

//This Reducer should compute the count and average body size for each category. 
//Write to the context object the category as the key and "AverageBodySize,DocCount" as the value (this is one string with the two values separated by a comma). 

public class CategoryStatsReducer extends Reducer<Text, IntWritable, Text, Text>{
	private static Map<String, Double> totalCount = new HashMap<>();
	private static Map<String, Double> bodySize = new HashMap<>();
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		double sum = 0;
		double size = 0;
		for (IntWritable value: values){
			sum += value.get();
			size += 1;
		}
		
		if(totalCount.get(key) == null){
			totalCount.put(key.toString(), sum);
		}
		else{
			totalCount.put(key.toString(), totalCount.get(key) + sum);
		}
		
		if(bodySize.get(key) == null){
			bodySize.put(key.toString(), size);
		}
		else{
			bodySize.put(key.toString(), bodySize.get(key) + size);
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		for(String category: bodySize.keySet()){
			double avg = totalCount.get(category)/bodySize.get(category);
			String output = "," + totalCount.get(category);
			String next = String.valueOf(avg).concat(output);
			context.write(new Text(category), new Text(next));
		}
	}
}