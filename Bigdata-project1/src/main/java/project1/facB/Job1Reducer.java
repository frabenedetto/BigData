package project1.facB;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	
	
	public void reduce(Text arg0, Iterable<IntWritable> arg1,
			Context ctx)
			throws IOException, InterruptedException {

		int sum = 0;
		for(IntWritable value: arg1) {
			sum+=value.get();
		}
		
		ctx.write(arg0, new IntWritable(sum));
		
	}
	


}
