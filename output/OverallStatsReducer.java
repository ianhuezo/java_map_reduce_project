import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//Create a Reducer class and implement the reduce function and the cleanup function.
//Input Key,Value: LongWritable, IntWritable
//Output Key,Value: Text, DoubleWritable

//This Reducer should follow the example we learned in class to compute the count and average values.
//The output should look as clarified in the instructions document.

public class OverallStatsReducer extends Reducer<LongWritable, IntWritable, Text, DoubleWritable>{
	private static double docCounter = 0;
	private static double wordCount = 0;
	@Override
	public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

		for(IntWritable value: values){
			wordCount = wordCount + value.get();
		}
		docCounter += 1;
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		context.write(new Text("Count"), new DoubleWritable(docCounter));
		context.write(new Text("Average"), new DoubleWritable(wordCount/docCounter));
	}
}