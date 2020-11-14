import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//Create a Mapper class and implement the map function.
//Input Key,Value: LongWritable, Text
//Output Key,Value: LongWritable, IntWritable

//In the map function, you need to split the content of each line and get the body column.
//Write to the context object the smae input key and the length (character count) of the text of the body column as the value.
public class OverallStatsMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable>{
	
	@Override
	public void map(LongWritable lineNumber, Text lineContent, Context context) throws IOException, InterruptedException{
		String body = lineContent.toString().split("\t")[3];//column 3 is the body column
		IntWritable bodyWordCount = new IntWritable(body.split(" ").length);//assume every word is separated by space
		context.write(lineNumber, bodyWordCount);
	}
}