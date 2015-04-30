package project1.facB;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Job2Reducer extends Reducer<IntWritable, Text, Text, IntWritable>{
	
	public void reduce(IntWritable arg0, Iterable<Text> arg1,
			Context ctx)
			throws IOException, InterruptedException {
		String tipo = new String(Integer.toString(arg0.get()));
		
		int sum =0;
		for(@SuppressWarnings("unused") Text t : arg1){
			sum++;
		}
		
		
		ctx.write(new Text(tipo), new IntWritable(sum));
		
		
	}

}
