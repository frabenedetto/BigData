package project1.facA;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer extends Reducer<ProductPair, IntWritable, Text, IntWritable>{

	
	
	public void reduce(ProductPair arg0, Iterable<IntWritable> arg1,
			Context ctx)
			throws IOException, InterruptedException {

		int sum = 0;
		for(IntWritable value: arg1) {
			sum+=value.get();
		}
		
		ctx.write(new Text(arg0.toString()), new IntWritable(sum));
		
	}

}
