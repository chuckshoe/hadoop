import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString().toLowerCase();

    Pattern p ;
    Matcher m ;
    int sum = 0;
    
    p = Pattern.compile("hackathon");
    m = p.matcher( line );
    while (m.find()) {
      sum++ ;
    }
    context.write(new Text("hackathon"), new IntWritable(sum));

    p = Pattern.compile("dec");
    m = p.matcher( line );
    sum = 0;
    while (m.find()) {
      sum++ ;
    }
    context.write(new Text("Dec"), new IntWritable(sum));
    
    p = Pattern.compile("chicago");
    m = p.matcher( line );
    sum = 0;
    while (m.find()) {
      sum++ ;
    }
    context.write(new Text("Chicago"), new IntWritable(sum));
    
    p = Pattern.compile("java");
    m = p.matcher( line );
    sum = 0;
    while (m.find()) {
      sum++ ;
    }
    context.write(new Text("Java"), new IntWritable(sum));

  } // end map
} // end class
