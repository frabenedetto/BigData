package project1.facB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Job1Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

	private IntWritable one = new IntWritable(1);
	
	public void map(LongWritable arg0, Text value,
			Context ctx) throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, ",", false);
		//date not needed
		tokenizer.nextToken();
		
		//conteggio prodotti negli scontrini
		int i=0;
		while(tokenizer.hasMoreTokens()){
			i++;
			}
		
		if(i<5)
			ctx.write(new IntWritable(0), one);
		
		ctx.write
		
	}

}
