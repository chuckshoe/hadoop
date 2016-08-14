import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Name : Akshay Kalia
// Description : Calculate the PR of a webpage
public class PageRank {
	

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: PageRank <input path> <output path>");
			System.exit(-1);
		}
		int iterationCount = 0;
		String input, output;
		
		while( iterationCount < 3){
			Job job = new Job();
			job.setJarByClass(PageRank.class);
			job.setJobName("PageRank"+iterationCount);
			// the output of the previous iteration will be the input
	        	if (iterationCount == 0) 
	        		// for the first iteration the input will be the first input argument
	            		input = args[0];
	        	else
	            		// for the remaining iterations, the input will be the output of the previous iteration
	            		input = args[1]+ "-" + iterationCount;
	
	        	output = args[1] + "-"+ (iterationCount + 1); // setting the output file
	
	        	FileInputFormat.setInputPaths(job, new Path(input)); // setting the input files for the job
	        	FileOutputFormat.setOutputPath(job, new Path(output)); // setting the output file
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.waitForCompletion(true);
	        	iterationCount++;
   
		}
		System.exit(0);
	}
}
