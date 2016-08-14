import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer
	extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		double totalPR = 0;
        	String outboundLinks = "";
        	String keyValue = "";
		for (Text value : values) {
			keyValue = value.toString();
			if (keyValue.contains(","))
            		{
				String items[] = keyValue.split(",");
                		double pageRank = Double.parseDouble(items[items.length - 1]);
                		totalPR += pageRank;
            		}
            		else
            		{
                		outboundLinks = value.toString();
            		}
		}
		context.write(key, new Text(outboundLinks + " " + totalPR));
	}
}
