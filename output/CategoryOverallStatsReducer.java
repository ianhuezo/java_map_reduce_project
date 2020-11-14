import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//Create a Reducer class and implement the reduce function and the cleanup function.
//Input Key,Value: Text, Text 
//Output Key,Value: Text, Text 

//This Reducer should follow the example we learned in class to compute the stats as described in the instructions document for this part.

public class CategoryOverallStatsReducer extends Reducer<Text, Text, Text, Text>{
	private static String maxCategory = "";
	private static double maxWordCount = 0.0;
	private static String minCategory = "";
	private static double minWordCount = Double.MAX_VALUE;
	private static Map <String, Integer> categoryCount = new HashMap<>();
	private static int catCounter = 0;
	private static int docCounter = 0;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		for(Text value: values){
			String[] data = value.toString().split(",");
			String category = key.toString();
			Double wordCount = Double.parseDouble(data[0]);
			Double avgDocCount = Double.parseDouble(data[1]);
			docCounter += avgDocCount;
			if(categoryCount.get(category) == null){
				catCounter += 1;
				categoryCount.put(category, 1);
			}
			docCounter += 1;
			if(wordCount > maxWordCount){
				maxCategory = category;
				maxWordCount = wordCount;
			}
			if(wordCount < minWordCount){
				minCategory = category;
				minWordCount = wordCount;
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		double avgDocCat = ((double)docCounter)/catCounter;
		context.write(new Text("Average Document Words Per Category"), new Text(String.valueOf(avgDocCat)));
		context.write(new Text("Category with max avg body word count"), new Text(maxCategory));
		context.write(new Text("Category with min avg body word count"), new Text(minCategory));
	}
}