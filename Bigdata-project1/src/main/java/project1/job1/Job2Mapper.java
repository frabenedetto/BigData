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

public class Job2Mapper extends MapReduceBase 
					implements Mapper<LongWritable, Text, IntWritable, Text>{

	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> arg2, Reporter arg3)
			throws IOException {
		
		Text product = new Text();
		IntWritable quantity = new IntWritable();
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while(tokenizer.hasMoreTokens()){
			product.set(tokenizer.nextToken());
			quantity.set(Integer.parseInt(tokenizer.nextToken()));
			arg2.collect(quantity, product);
		}
	}

	
}
