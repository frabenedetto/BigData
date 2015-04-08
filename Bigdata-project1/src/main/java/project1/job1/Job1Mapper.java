package project1.job1;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class Job1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

	private Text product = new Text();
	private IntWritable one = new IntWritable(1);

	/*uso TextInputFormat(default inputFormat): An InputFormat for plain text files.
	 *  Files are broken into lines. Either linefeed or carriage-return are used to signal end of line.
	 *  Keys are the position in the file, and values are the line of text.. (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException {
			
			String line = value.toString();
			line = line.replaceAll("\\d+(?:[.,]\\d+)*\\s*", "");
			StringTokenizer tokenizer = new StringTokenizer(line, ",", false);
			while(tokenizer.hasMoreTokens()){
				product.set(tokenizer.nextToken());
				arg2.collect(product, one);
			}
			
	}

	
	
}
