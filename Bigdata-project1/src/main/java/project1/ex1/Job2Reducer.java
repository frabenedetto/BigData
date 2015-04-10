package project1.ex1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Job2Reducer extends MapReduceBase 
		implements Reducer<IntWritable, Text, Text, IntWritable>{

	public void reduce(IntWritable arg0, Iterator<Text> arg1,
			OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException {
			
		while(arg1.hasNext()) {
			arg2.collect(arg1.next(), arg0);
		}
		
	}
	
}
