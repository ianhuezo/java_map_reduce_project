import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Create a Mapper class and implement the map function.
//Input Key,Value: Text, Text
//Output Key,Value: Text, Text

//The map function does nothing but passing on the <key, values> pairs to the reducer. 

public class CategoryOverallStatsMapper extends Mapper<Text, Text, Text, Text> {
  @Override
  public void map(Text key, Text value, Context context)
       throws IOException, InterruptedException {
	  context.write(key, value);
  }
}