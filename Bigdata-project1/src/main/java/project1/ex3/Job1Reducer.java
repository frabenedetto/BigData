package project1.ex3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Job1Reducer extends MapReduceBase implements Reducer<ProductPair, IntWritable, Text, IntWritable>{

    private final static IntWritable SUM = new IntWritable();

	
	public void reduce(ProductPair arg0, Iterator<IntWritable> arg1,
			OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException {

		int sum = 0;
		while(arg1.hasNext()){
			sum+=arg1.next().get();
		}
		SUM.set(sum);
		arg2.collect(new Text(arg0.toString()), SUM);
		
	}

}
