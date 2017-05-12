/**
 * @author Anamika Sharaf
 * @date April 7, 2017
 * 
 */


package Lab1; 

import org.apache.hadoop.conf.Configured;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class MRdriver extends Configured implements Tool {

   public int run(String[] args) throws Exception {

      // TODO: configure first MR job 
 
         Job job = new Job(getConf(), "Lab1"); 
         job.setJarByClass(MRdriver.class);
	 job.setMapperClass(MRmapper1.class);
	 job.setReducerClass(MRreducer1.class);
	// job.setNumReduceTasks(0);
	
      // TODO: setup input and output paths for first MR job

	 job.setInputFormatClass(TextInputFormat.class);
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(IntWritable.class);
	 job.setMapOutputKeyClass(Text.class);
	 job.setMapOutputValueClass(IntWritable.class);
     
	 	

      // TODO: run first MR job syncronously with verbose output set to true

	 FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 job.waitForCompletion(true);
       	// return job.waitForCompletion(true) ? 0 : 1; 

      // TODO: configure the second MR job 

	 Job job1 = new Job(getConf(), "Lab1");
	 job1.setJarByClass(MRdriver.class);
	 job1.setMapperClass(MRmapper2.class);
	 job1.setReducerClass(MRreducer2.class);
	 //job1.setNumReduceTasks(0);


		
      // TODO: setup input and output paths for second MR job

	 job1.setInputFormatClass(TextInputFormat.class);
	 job1.setOutputKeyClass(Text.class);
	 job1.setOutputValueClass(Text.class);
	 job1.setMapOutputKeyClass(Text.class);
	 job1.setMapOutputValueClass(Text.class);


      // TODO: run second MR job syncronously with verbose output set to true

	 FileInputFormat.addInputPath(job1, new Path(args[1]));
	 FileOutputFormat.setOutputPath(job1, new Path(args[2]));
	 job1.waitForCompletion(true);
	// return job1.waitForCompletion(true) ? 0 : 1;      



      // TODO: detect anomaly based on sigma_threshold provided by user
	try {	
		Path outputfile_path = new Path("/user/user01/LAB1/OUT2/part-r-00000"); 
		FileSystem file_sys = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(file_sys.open(outputfile_path)));
	

		String currentline = "";
		String sigma = args[3];
		double sigma_thershold = Double.parseDouble(sigma);
		int count = 0; 

		while((currentline = br.readLine())!= null)
		{
			String[] tempArray = currentline.split("\\s+");
			String string_sigma = tempArray[1];
			double sigmacheck = Double.parseDouble(string_sigma);
			count++;
			
			 // TODO: for each user with score higher than threshold, print to screen:
			if( sigmacheck > sigma_thershold && count >2 )
			{
				// detected anomaly for user: <username>  with score: <numSigmas>
				String message = ("detected anomaly for user:" + tempArray[0] + "with score:" + sigmacheck); 
				System.out.println(message);
			}
		
		}
	
	}catch(IOException e) {
		e.printStackTrace();
	}

return 0;   
}

   public static void main(String[] args) throws Exception { 
	   if(args.length != 4) {
		   System.err.println("usage: MRdriver <input-path> <output1-path> <output2-path> <sigma_int_threshold>");
		   System.exit(1);
	   }
	   // check sigma_int_threshold is an int
	   try {
		  Integer.parseInt(args[3]);
	   }
	   catch (NumberFormatException e) {
		  System.err.println(e.getMessage());
		  System.exit(1);
	   }
           Configuration conf = new Configuration();
           System.exit(ToolRunner.run(conf, new MRdriver(), args));
   } 
}
