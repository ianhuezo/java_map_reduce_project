import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//Create a Mapper class and implement the map function.
//Input Key,Value: LongWritable, Text
//Output Key,Value: Text, IntWritable

//In the map function, you need to split the content of each line and get the category and body column.
//Write to the context object the category as the key and the length (character count) of the text of the body column as the value.

public class CategoryStatsMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	public void map(LongWritable lineNumber, Text lineContent, Context context) throws IOException, InterruptedException{
		String body = lineContent.toString().split("\t")[4];//column 4 is the body column
		String category = lineContent.toString().split("\t")[1];
		IntWritable bodyWordCount = new IntWritable(body.split(" ").length);//assume every word is separated by space
		context.write(new Text(category), bodyWordCount);
	}
}