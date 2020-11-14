import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AssignmentDriver {
  public static void main(String[] args) throws Exception {
	  if(args.length != 3){
		  throw new Exception("Input arg length of 3 is not satisfied");
	  }
	  String inputFolder, outputFolderJob2, outputFolderJob3;
	  inputFolder = args[0];
	  outputFolderJob2 = args[1];
	  outputFolderJob3 = args[2];
	 // ------------------------
	 //  Job 1
	 // ------------------------
	 // Create job1 Object
	 // Set JAR class: AssignmentDriver
	Job job1 = new Job();
	job1.setJobName("parser");
	job1.setJarByClass(AssignmentDriver.class);
	 
	 // Set Mapper class for Job1: DocumentParsingMapper
	 // Set Reducer class for Job1: DocumentParsingReducer
	job1.setMapperClass(DocumentParsingMapper.class);
	job1.setReducerClass(DocumentParsingReducer.class);
	
	  
	 // Set Output Key type: Text
	 // Set Output Value type: Article
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(Article.class);
	  
	 // Set Inputformat class: WholeFileInputFormat
	job1.setInputFormatClass(WholeFileInputFormat.class);
	 // Since the dataset contains multiple folders, make sure to read in recursive mode:  WholeFileInputFormat.setInputDirRecursive(job1, true);
	WholeFileInputFormat.setInputDirRecursive(job1, true);
	FileInputFormat.setInputPaths(job1, new Path(inputFolder));
	 FileOutputFormat.setOutputPath(job1, new Path("jobOutput1"));
	 // Set Input Path "20-newsgroups" for local testing or args[0] when you export the jar
	 // Set Output path
	  
	  // Don't submit the job!
	  boolean success1 = job1.waitForCompletion(true);
	  if(!success1) System.exit(1);
     
     // ------------------------
	 //  Job 2
	 // ------------------------
	 // Create job2 Object
	 // Set JAR class: AssignmentDriver
		Job job2 = new Job();
		job2.setJobName("Overall Statistics");
		job2.setJarByClass(AssignmentDriver.class);	 
	 // Set Mapper class for Job1: OverallStatsMapper
	 // Set Reducer class for Job1: OverallStatsReducer
		job2.setMapperClass(OverallStatsMapper.class);
		
		job2.setReducerClass(OverallStatsReducer.class);
		
	 // Set Output Key type: Text
	 // Set Output Value type: DoubleWritable
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
     
     // Set Mapper Output Key type: LongWritable  (this is needed here because the key and value types of Mapper are different from reducer). Use the Job2.setMapKeyClass(...)
     // Set Mapper Output Key type: IntWritable  (this is needed here because the key and value types of Mapper are different from reducer).  Use the Job2.setMapValueClass(...)
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(IntWritable.class);
		
		
	 // Set Inputformat class: TextInputFormat
		job2.setInputFormatClass(TextInputFormat.class);
	 
	 // Set Input Path: the output path of Job 1
		TextInputFormat.setInputPaths(job2, "jobOutput1");
	 // Set Output path
		FileOutputFormat.setOutputPath(job2, new Path(outputFolderJob2));
     // Don't submit the job!
		  boolean success2 = job2.waitForCompletion(true);
		  if(!success2) System.exit(1);
     
     // ------------------------
  	 //  Job 3
  	 // ------------------------
  	 // Create job3 Object
  	 // Set JAR class: AssignmentDriver
		Job job3 = new Job();
		job3.setJobName("Category Statistics");
		job3.setJarByClass(AssignmentDriver.class);	 
  	 // Set Mapper class for Job1: CategoryStatsMapper
  	 // Set Reducer class for Job1: CategoryStatsReducer
		job3.setMapperClass(CategoryStatsMapper.class);
		job3.setReducerClass(CategoryStatsReducer.class);
  	  
  	 // Set Output Key type: Text
  	 // Set Output Value type: Text
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
       
     // Set Mapper Output Key type: Text  (this is needed here because the key and value types of Mapper are different from reducer). Use the Job2.setMapKeyClass(...)
     // Set Mapper Output Key type: IntWritable  (this is needed here because the key and value types of Mapper are different from reducer).  Use the Job2.setMapValueClass(...)
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(IntWritable.class);
  	  	
  	 // Set Inputformat class: TextInputFormat
  	  	job3.setInputFormatClass(TextInputFormat.class);
  	 // Set Input Path: the output path of Job 1
  	  TextInputFormat.setInputPaths(job3, new Path("jobOutput1"));
  	 // Set Output path
  	  FileOutputFormat.setOutputPath(job3, new Path(outputFolderJob3));
       
     // Don't submit the job!
	  boolean success3 = job3.waitForCompletion(true);
	  if(!success3) System.exit(1);
     
     // ------------------------
  	 //  Job 4
  	 // ------------------------
  	 // Create job4 Object
  	 // Set JAR class: AssignmentDriver
	  	Job job4 = new Job();
		job4.setJobName("Category Overall Statistics");
		job4.setJarByClass(AssignmentDriver.class);	 
  	 // Set Mapper class for Job1: CategoryOverallStatsMapper
  	 // Set Reducer class for Job1: CategoryOverallStatsReducer
		job4.setMapperClass(CategoryOverallStatsMapper.class);
		job4.setReducerClass(CategoryOverallStatsReducer.class);
  	  
  	 // Set Output Key type: Text
  	 // Set Output Value type: Text
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
      
  	 // Set Inputformat class: KeyValueTextInputFormat
		job4.setInputFormatClass(KeyValueTextInputFormat.class);
  	 
  	 // Set Input Path: the output path of Job 3
		KeyValueTextInputFormat.setInputPaths(job4, new Path(outputFolderJob3));
  	 // Set Output path
		FileOutputFormat.setOutputPath(job4, new Path("jobOutput4"));
       
     // Don't submit the job!
		  boolean success4 = job4.waitForCompletion(true);
		  if(!success4) System.exit(1);
     
     // ------------------------
  	 //  Create Controlled Jobs
  	 // ------------------------
     
     // Create Controlled Job for Job1.
     // Configuration ControlJobConf1 = new Configuration();
     // ControlledJob controlledJob1 = new ControlledJob(ControlJobConf1);
     // controlledJob1.setJob(job1);
		Configuration ControlJobConf1 = new Configuration();
		ControlledJob controlledJob1 = new ControlledJob(ControlJobConf1);
		controlledJob1.setJob(job1);
     
    
     // Create Controlled Job for Job2.
		Configuration ControlJobConf2 = new Configuration();
		ControlledJob controlledJob2 = new ControlledJob(ControlJobConf2);
		controlledJob2.setJob(job2);
    
     
     // Create Controlled Job for Job3.
		Configuration ControlJobConf3 = new Configuration();
		ControlledJob controlledJob3 = new ControlledJob(ControlJobConf3);
		controlledJob3.setJob(job3);
     
     
     // Create Controlled Job for Job4.
		Configuration ControlJobConf4 = new Configuration();
		ControlledJob controlledJob4 = new ControlledJob(ControlJobConf4);
		controlledJob4.setJob(job4);    
     
     // ------------------------
  	 //  Create Job Dependencies
  	 // ------------------------
     
     // add job1 as a dependency for job2
		controlledJob2.addDependingJob(controlledJob1);
     
     // add job1 as a dependency for job3
		controlledJob3.addDependingJob(controlledJob1);
     
     // add job3 as a dependency for job4
		controlledJob4.addDependingJob(controlledJob3);
     
     
     // ------------------------
  	 // The Job Controller
  	 // ------------------------
     
     // create a job controller object
     
     // add ControlledJob1 to the controller
	 // add ControlledJob2 to the controller
	 // add ControlledJob3 to the controller
	 // add ControlledJob4 to the controller
     JobControl jc = new JobControl("test");
     jc.addJob(controlledJob1);
     jc.addJob(controlledJob2);
     jc.addJob(controlledJob3);
     jc.addJob(controlledJob4);
     jc.run();
     // Run the controller
  }
}
