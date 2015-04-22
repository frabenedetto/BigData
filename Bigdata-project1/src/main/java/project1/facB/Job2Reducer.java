package project1.facB;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Job2Reducer extends Reducer<IntWritable, Text, Text, IntWritable>{
	
	public void reduce(IntWritable arg0, Iterable<Text> arg1,
			Context ctx)
			throws IOException, InterruptedException {
		String tipo = null;
		if(arg0.get() == 2)
			tipo = "coppie";
		else if(arg0.get() == 3)
			tipo = "triple";
		else if(arg0.get() == 4)
			tipo = "quadruple";
		int sum = 0;
		
		
		for(Text t : arg1){
			sum++;
		}
		
		
		ctx.write(new Text(tipo), new IntWritable(sum));
		
		
	}

}
