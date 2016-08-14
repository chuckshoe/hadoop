import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: PageRank <input path> <output path>");
      System.exit(-1);
    }
    
    Job job1 = new Job();
    Job job2 = new Job();
    Job job3 = new Job();
    
    job1.setJarByClass(PageRank.class);
    job1.setJobName("Page Rank");
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    job1.setMapperClass(PageRankMapper.class);
    job1.setReducerClass(PageRankReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    
    if (job1.waitForCompletion(true)) {

	job2.setJarByClass(PageRank.class);
	job2.setJobName("Page Rank");
	FileInputFormat.addInputPath(job2, new Path(args[1]));
	FileOutputFormat.setOutputPath(job2,new Path( "pr_output2"));
	job2.setMapperClass(PageRankMapper.class);
	job2.setReducerClass(PageRankReducer.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
    }
    
    if (job2.waitForCompletion(true)) {

	job3.setJarByClass(PageRank.class);
	job3.setJobName("Page Rank");
	FileInputFormat.addInputPath(job3, new Path("pr_output2"));
	FileOutputFormat.setOutputPath(job3,new Path("pr_output3"));
	job3.setMapperClass(PageRankMapper.class);
	job3.setReducerClass(PageRankReducer.class);
	job3.setOutputKeyClass(Text.class);
	job3.setOutputValueClass(Text.class);
    }
 
    System.exit(job3.waitForCompletion(true) ? 0 : 1);
  }
}
