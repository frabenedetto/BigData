package project1.ex3;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Job1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, ProductPair, IntWritable>{

	private IntWritable one = new IntWritable(1);
	
	public void map(LongWritable arg0, Text value,
			OutputCollector<ProductPair, IntWritable> arg2, Reporter arg3) throws IOException {
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, ",", false);
		//date not needed
		tokenizer.nextToken();
		//first token wich will go on the left side
		String insToken = tokenizer.nextToken();
		while(tokenizer.hasMoreTokens()){
			ProductPair pp = new ProductPair();
			pp.setProductLeft(new Text(insToken));
			insToken = tokenizer.nextToken();
			pp.setProductRight(new Text(insToken));
			arg2.collect(pp, one);
		}
	}

}
