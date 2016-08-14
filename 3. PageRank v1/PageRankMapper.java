import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper
  extends Mapper<LongWritable, Text, Text, Text> {
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
    if(!line.isEmpty()){	
        String tokens[] = line.split(" ");
        int numOutLinks = tokens.length - 2;
        double pageRank = Double.parseDouble(tokens[tokens.length-1]);

        if (numOutLinks != 0 ) {
            String links = "";
            for(int i = 1; i <= numOutLinks; i++) {
                links = links + " " + tokens[i];
                context.write(new Text(tokens[i]),new Text(tokens[0] +"," + (pageRank/numOutLinks)));
            }
            context.write(new Text(tokens[0]),new Text(links.trim()));
        }
    }
  } // end map
} // end class
