import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer
  extends Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    double totalPR = 0;
    String links = "";
    String finalkey = "";
	
    for (Text value : values) {
        finalkey = value.toString();
        if (finalkey.contains(","))
        {
            String items[] = finalkey.split(",");
            double pageRank = Double.parseDouble(items[items.length - 1]);
            totalPR += pageRank;
        }
        else
        {
            links = value.toString();
        }
    }
	
    context.write(key, new Text(links.trim() + " " + totalPR));       

  } // end reduce
} // end class
