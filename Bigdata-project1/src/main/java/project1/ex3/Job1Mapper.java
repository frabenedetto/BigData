package project1.ex3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Job1Mapper extends Mapper<LongWritable, Text, ProductPair, IntWritable>{

	private IntWritable one = new IntWritable(1);
	
	public void map(LongWritable arg0, Text value,
			Context ctx) throws IOException, InterruptedException {
		
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
			ctx.write(pp, one);
		}
	}

}
