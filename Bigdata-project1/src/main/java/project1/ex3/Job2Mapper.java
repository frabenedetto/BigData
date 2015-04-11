package project1.ex3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper extends Mapper<LongWritable, Text, ProductPair, IntWritable >{

	     
	     public void map(LongWritable arg0, Text arg1,
			Context arg2)
			throws IOException, InterruptedException {
		
	    	 String line = arg1.toString();
			 StringTokenizer tokenizer = new StringTokenizer(line);
			 
			 ProductPair pp = new ProductPair();
			 IntWritable quantity = new IntWritable();
			 
			 while(tokenizer.hasMoreTokens()) {
				 pp.setProductLeft(new Text(tokenizer.nextToken()));
				 pp.setProductRight(new Text(tokenizer.nextToken()));
				 quantity.set(Integer.parseInt(tokenizer.nextToken()));
				 
				 arg2.write(pp, quantity);
				 
				 
				 
			 }

		 
	    	
	    	 

		
	}
}
