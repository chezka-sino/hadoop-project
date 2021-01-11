import org.apache.hadoop.conf.Configuration;
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

public class AssignmentDriver {
  public static void main(String[] args) throws Exception {
  	  
		 // ------------------------
		 //  Job 1
		 // ------------------------
		 // Create job1 Object
		 Job job1 = new Job();
		 job1.setJobName("documentparser");
		 // Set JAR class: AssignmentDriver
		 job1.setJarByClass(AssignmentDriver.class);
		 
		 // Set Mapper class for Job1: DocumentParsingMapper
		 job1.setMapperClass(DocumentParsingMapper.class);	 
		 // Set Reducer class for Job1: DocumentParsingReducer
		 job1.setReducerClass(DocumentParsingReducer.class);
		  
		 // Set Output Key type: Text
		 job1.setOutputKeyClass(Text.class);
		 // Set Output Value type: Article
		 job1.setOutputValueClass(Article.class);
		  
		 // Set Inputformat class: WholeFileInputFormat
		 job1.setInputFormatClass(WholeFileInputFormat.class);
		 // Since the dataset contains multiple folders, make sure to read in recursive mode:  WholeFileInputFormat.setInputDirRecursive(job1, true);
		 WholeFileInputFormat.setInputDirRecursive(job1, true);
		 
		 // Set Input Path "20-newsgroups" for local testing or args[0] when you export the jar
		 FileInputFormat.setInputPaths(job1, new Path(args[0]));
		 // Set Output path
		 FileOutputFormat.setOutputPath(job1, new Path(args[1]));
	  
	  // Don't submit the job!
	  
     
     // ------------------------
	 //  Job 2
	 // ------------------------
	 // Create job2 Object
		 Job job2 = new Job();
		 job2.setJobName("datasetstatistics");
	 // Set JAR class: AssignmentDriver
		 job2.setJarByClass(AssignmentDriver.class);
		 	 
	 // Set Mapper class for Job1: OverallStatsMapper
		 job2.setMapperClass(OverallStatsMapper.class);
	 // Set Reducer class for Job1: OverallStatsReducer
		 job2.setReducerClass(OverallStatsReducer.class);
	  
	 // Set Output Key type: Text
		 job2.setOutputKeyClass(Text.class);
	 // Set Output Value type: DoubleWritable
		 job2.setOutputValueClass(DoubleWritable.class);
     
     // Set Mapper Output Key type: LongWritable  (this is needed here because the key and value types of Mapper are different from reducer). Use the 
		 job2.setMapOutputKeyClass(LongWritable.class);
     // Set Mapper Output Key type: IntWritable  (this is needed here because the key and value types of Mapper are different from reducer).  Use the Job2.setMapValueClass(...)
		 job2.setMapOutputValueClass(IntWritable.class);
	 // Set Inputformat class: TextInputFormat
		 job2.setInputFormatClass(TextInputFormat.class);
	 // Set Input Path: the output path of Job 1
		 FileInputFormat.setInputPaths(job2, new Path(args[1]));
	 // Set Output path
		 FileOutputFormat.setOutputPath(job2, new Path(args[2]));
     // Don't submit the job!
		 
     
     // ------------------------
  	 //  Job 3
  	 // ------------------------
  	 // Create job3 Object
		 Job job3 = new Job();
		 job3.setJobName("categorystats");
  	 // Set JAR class: AssignmentDriver
		 job3.setJarByClass(AssignmentDriver.class);
  	 
  	 // Set Mapper class for Job1: CategoryStatsMapper
		 job3.setMapperClass(CategoryStatsMapper.class);
  	 // Set Reducer class for Job1: CategoryStatsReducer
		 job3.setReducerClass(CategoryStatsReducer.class);
  	  
  	 // Set Output Key type: Text
		 job3.setOutputKeyClass(Text.class);
  	 // Set Output Value type: Text
		 job3.setOutputValueClass(Text.class);
       
     // Set Mapper Output Key type: Text  (this is needed here because the key and value types of Mapper are different from reducer). Use the Job2.setMapKeyClass(...)
		 job3.setMapOutputKeyClass(Text.class);
     // Set Mapper Output Key type: IntWritable  (this is needed here because the key and value types of Mapper are different from reducer).  Use the Job2.setMapValueClass(...)
		 job3.setMapOutputValueClass(IntWritable.class);
  	  
  	 // Set Inputformat class: TextInputFormat
		 job3.setInputFormatClass(TextInputFormat.class);
  	 
  	 // Set Input Path: the output path of Job 1
		 FileInputFormat.setInputPaths(job3, new Path(args[1]));
  	 // Set Output path
		 FileOutputFormat.setOutputPath(job3, new Path(args[3]));
       
     // Don't submit the job!
 
     
     // ------------------------
  	 //  Job 4
  	 // ------------------------
  	 // Create job4 Object
		 Job job4 = new Job();
		 job4.setJobName("categoryoverall");
  	 // Set JAR class: AssignmentDriver
		 job4.setJarByClass(AssignmentDriver.class);
  	 
  	 // Set Mapper class for Job1: CategoryOverallStatsMapper
		 job4.setMapperClass(CategoryOverallStatsMapper.class);
  	 // Set Reducer class for Job1: CategoryOverallStatsReducer
		 job4.setReducerClass(CategoryOverallStatsReducer.class);
  	  
  	 // Set Output Key type: Text
		 job4.setOutputKeyClass(Text.class);
  	 // Set Output Value type: Text
		 job4.setOutputValueClass(Text.class);
      
  	 // Set Inputformat class: KeyValueTextInputFormat
		 job4.setInputFormatClass(KeyValueTextInputFormat.class);
  	 
  	 // Set Input Path: the output path of Job 3
		 FileInputFormat.setInputPaths(job4, new Path(args[3]));
  	 // Set Output path
		 FileOutputFormat.setOutputPath(job4, new Path(args[4]));
       
     // Don't submit the job!

     
     // ------------------------
  	 //  Create Controlled Jobs
  	 // ------------------------
     
     // Create Controlled Job for Job1.
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
      JobControl jc = new JobControl(null);
     
     // add ControlledJob1 to the controller
      jc.addJob(controlledJob1);
	 // add ControlledJob2 to the controller
      jc.addJob(controlledJob2);
	 // add ControlledJob3 to the controller
      jc.addJob(controlledJob3);
	 // add ControlledJob4 to the controller
      jc.addJob(controlledJob4);
     
     // Run the controller
      
      Thread thread = new Thread(jc);
      thread.start();

      while(!jc.allFinished()) {
    	  System.out.println("Jobs running...");
		  Thread.sleep(5000);
      }
      
      System.exit(0);
  }
}
