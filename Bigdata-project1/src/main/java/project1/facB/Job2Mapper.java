package project1.facB;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper extends Mapper<LongWritable, Text, IntWritable, Text>{
	
	public void map(LongWritable arg0, Text value,
			Context ctx) throws IOException, InterruptedException {
		
		StringTokenizer t = new StringTokenizer(value.toString(), ")", false);
		int n = 0; String chiave = "";
		if(t.hasMoreTokens()) chiave = t.nextToken();
		n = new StringTokenizer(chiave,",",false).countTokens();
		
		ctx.write(new IntWritable(n), new Text(chiave));
	}

}
