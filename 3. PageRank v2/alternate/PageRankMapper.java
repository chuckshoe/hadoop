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
			line = line.replaceAll("\t"," ");
			String lineItems[] = line.split(" ");
			// Number of outbound Links equal to array length, minus page itself and page Rank
			int noOfOutboundLinks = lineItems.length - 2;
			double pageRank = Double.parseDouble(lineItems[lineItems.length-1]);
			if (noOfOutboundLinks != 0 ) {
				String outboundLinks = "";
				for(int counter = 1; counter <= noOfOutboundLinks; counter++)
				{
					outboundLinks = outboundLinks + " " + lineItems[counter];
					context.write(new Text(lineItems[counter]),new Text(lineItems[0] +"," + (pageRank/noOfOutboundLinks)));
				}
				context.write(new Text(lineItems[0]),new Text(outboundLinks.trim()));
			}
		}
	}	
}
